mod mysql;
mod protocal;
mod binlog;
mod executor;
mod config;
mod message_queue;

use std::{
    io::{Read, Write},
    net::TcpStream,
};
use bytes::BytesMut;
use nom::AsBytes;
use crate::binlog::{DeleteRowEvent, EventHeader, EventRaw, QueryEvent, TableMap, TableMapEvent, UpdateRowEvent, WriteRowEvent};
use crate::executor::{DmlData, Workers};
use crate::mysql::{Decoder, MySQLConnection, native_password_auth, Packet};
use crate::protocal::{AuthSwitchReq, AuthSwitchResp, Capabilities, ComBinLogDump, ComQuery, HandshakeResponse41, HandshakeV10, OkPacket};
use clap::{Arg, App};
use crate::config::{Config, get_abs_path};
use crate::message_queue::{MessageQueues, QueueMessage};

fn main() {
    let matches = App::new("Ru-CDC")
        .arg(Arg::with_name("config")
            .short('c')
            .long("config")
            .help("配置文件地址")
            .required(true)
            .takes_value(true))
        .arg(Arg::with_name("serve")
            .short('s')
            .long("serve")
            .help("启动服务"))
        .arg(Arg::with_name("gen")
            .short('g')
            .long("gen")
            .help("启动服务"))
        .get_matches();
    let config_path = matches.get_one::<String>("config").expect("配置文件地址");
    if matches.is_present("gen") {
        cli_gen_default(config_path);
    }
    if matches.is_present("serve") {
        serve(config_path);
    }
}

fn cli_gen_default(config_path: &String) {
    println!("写入默认配置到目标地址:{config_path}");
    let mut cfg = Config::gen_default();
    let cfg_str = cfg.to_json();
    println!("{cfg_str}");
    cfg.save_to(get_abs_path(config_path.to_string()));
    println!("Dump complete!")
}


fn serve(cfg_path: &String) {
    let config = Config::load_from(cfg_path.to_string());
    let mut mq = MessageQueues::new();
    mq.start_message_queue_from_config(config.clone().mqs);

    let mut conn = MySQLConnection::get_connection(config.clone().db_ip.as_str(), config.clone().db_port as u32, config.clone().max_packages as u32, config.clone().user_name, config.clone().passwd);

    let query = ComQuery{query: "set @master_binlog_checksum= @@global.binlog_checksum".to_string()};
    conn.write_package(0, &query).unwrap();
    let (i, resp) = conn.read_package::<OkPacket>().unwrap();
    //println!("ok resp:{:?}", resp);

    let query: ComQuery = "show master status".into();
    conn.write_package(0, &query).unwrap();

    let (_, text_resp) = conn.read_text_result_set().unwrap();
    //println!("text result is :{:?}", text_resp);

    let file = String::from_utf8(text_resp.rows[0].columns[0].clone()).unwrap();
    let pos: u32 = String::from_utf8(text_resp.rows[0].columns[1].clone())
        .unwrap()
        .parse()
        .unwrap();
    println!("{file} {pos}");

    let dump = ComBinLogDump {
        pos,
        flags: 0u16,
        server_id: 100,
        filename: file,
    };
    conn.write_package(0, &dump).unwrap();
    let mut table_map = TableMap::new();
    let mut current_data:Option<DmlData> = None;

    let mut worker = Workers::new();
    worker.start(2usize, mq.clone(), config.clone().instances, config.clone());
    let mut seq_idx:u64 = 0;

    loop {
        let (_, buf) = conn.read_package::<Vec<u8>>().unwrap();
        //println!("raw ev:{:?}", &buf);
        let event_result = EventRaw::decode(buf.payload.as_bytes());

        if let Ok((_, ev)) = event_result {
            println!("Get Event: {:?}", ev);
            if ev.header.event_type == 19 {
                let (i, tablemap) = TableMapEvent::decode(ev.payload.as_bytes()).expect("table map error");
                //println!("table map:{:?}", tablemap);
                table_map.decode_columns(tablemap.header.table_id, tablemap.column_types, tablemap.column_metas.as_bytes());
                //println!("meta map:{:?}", table_map.metas);
                current_data = Some(DmlData::new_data(tablemap.header.table_id as u32, tablemap.schema_name.clone(), tablemap.table_name.clone()));
            }
            if ev.header.event_type == 30 {
                let (i, event) = WriteRowEvent::decode(ev.payload.as_bytes()).unwrap();
                let (i, rows) = WriteRowEvent::decode_column_multirow_vals(&table_map, i, event.header.table_id, event.col_map_len).expect("解码数据错误");
                if let Some(ref mut data) = current_data {
                    for row in rows{
                        data.append_data(seq_idx, "Insert".to_string(), row, Vec::new());
                        &worker.push(data);
                        seq_idx += 1;
                        println!("=====> Data In Queue");
                    }
                } else {
                    println!("=====> no DML instance");
                }
            }
            if ev.header.event_type == 31{
                let (i, event) = UpdateRowEvent::decode(ev.payload.as_bytes()).unwrap();
                let (i, rows) = UpdateRowEvent::fetch_rows(i, table_map.clone(), event.header.table_id, event.col_map_len).expect("解码Update Val错误");
                if let Some(ref mut data) = current_data {
                    for (old_vals, new_vals) in rows{
                        data.append_data(seq_idx, "Update".to_string(), new_vals, old_vals);
                        &worker.push(data);
                        seq_idx += 1;
                        println!("=====> Data In Queue");
                    }
                }else{
                    println!("=====> no DML instance");
                }
            }
            if ev.header.event_type == 32{
                let (i, event) = DeleteRowEvent::decode(ev.payload.as_bytes()).unwrap();
                let (i, old_values) = DeleteRowEvent::fetch_rows(i, table_map.clone(), event.header.table_id, event.col_map_len).expect("解码 Delete Val错误");
                println!("======>Old val:{old_values:?} \n Rest update bytes: {i:?}");
                if let Some(ref mut data) = current_data {
                    for old_val in old_values {
                        data.append_data(seq_idx, "Delete".to_string(), Vec::new(), old_val);
                        &worker.push(data);
                        seq_idx += 1;
                        println!("=====> Data In Queue");
                    }
                }else{
                    println!("=====> no DML instance");
                }
            }
            if ev.header.event_type == 2 {
                let (i, query) = QueryEvent::decode(ev.payload.as_bytes()).unwrap();
                println!("statement event:{:?}", query);
            }

        }

    }

    println!("Done!");
}

