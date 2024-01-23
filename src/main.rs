mod mysql;
mod protocal;
mod binlog;
mod executor;
mod config;
mod message_queue;
mod statistics;
mod position_manager;

use std::{
    io::{Read, Write},
    net::TcpStream,
};
use bytes::BytesMut;
use nom::AsBytes;
use crate::binlog::{DeleteRowEvent, EventHeader, EventRaw, QueryEvent, RotateEvent, TableMap, TableMapEvent, UpdateRowEvent, WriteRowEvent};
use crate::executor::{DmlData, Workers};
use crate::mysql::{Decoder, MySQLConnection, native_password_auth, Packet};
use crate::protocal::{AuthSwitchReq, AuthSwitchResp, Capabilities, ComBinLogDump, ComQuery, HandshakeResponse41, HandshakeV10, OkPacket};
use clap::{Arg, App};
use crate::config::{Config, get_abs_path};
use crate::message_queue::{MessageQueues, QueueMessage};
use crate::position_manager::{check_valid_pos, load_from_file, PositionMng, update_name_pos, update_pos};
use crate::statistics::Statistics;

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
    let posMng = PositionMng::thread_safe_new();
    mq.start_message_queue_from_config(config.clone().mqs, posMng.clone());
    let if_pos_loaded = load_from_file(posMng.clone());
    let mut conn = MySQLConnection::get_connection(config.clone().db_ip.as_str(), config.clone().db_port as u32, config.clone().max_packages as u32, config.clone().user_name, config.clone().passwd);
    let query = ComQuery{query: "set @master_binlog_checksum= @@global.binlog_checksum".to_string()};
    conn.write_package(0, &query).unwrap();
    let (i, resp) = conn.read_package::<OkPacket>().unwrap();
    //println!("ok resp:{:?}", resp);
    let query: ComQuery = "show master status".into();
    conn.write_package(0, &query).unwrap();
    let text_resp = conn.read_text_result_set().unwrap();
    //println!("text result is :{:?}", text_resp);
    let (file, pos) = check_valid_pos(posMng.clone(), text_resp, config.from_start.is_some_and(|b|b));
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
    worker.start(7usize, mq.clone(), config.clone().instances, config.clone());
    let mut seq_idx:u64 = 0;
    let mut statistics= Statistics::new();


    loop {
        let (_, buf) = conn.read_package::<Vec<u8>>().unwrap();
        statistics.feed_bytes(buf.payload.len());
        let event_result = EventRaw::decode(buf.payload.as_bytes());

        if let Ok((_, ev)) = event_result {
            if ev.header.event_type == 19 {
                let (i, tablemap) = TableMapEvent::decode(ev.payload.as_bytes()).expect("table map error");
                table_map.decode_columns(tablemap.header.table_id, tablemap.column_types, tablemap.column_metas.as_bytes());
                current_data = Some(DmlData::new_data(tablemap.header.table_id as u32, tablemap.schema_name.clone(), tablemap.table_name.clone()));
            }
            if ev.header.event_type == 30 {
                let (i, event) = WriteRowEvent::decode(ev.payload.as_bytes()).unwrap();
                let (i, rows) = WriteRowEvent::decode_column_multirow_vals(&table_map, i, event.header.table_id, event.col_map_len).expect("解码数据错误");
                if let Some(ref mut data) = current_data {
                    data.append_data(seq_idx, "INSERT".to_string(), rows, Vec::new(), ev.header.log_pos);
                    &worker.push(data);
                    seq_idx += 1;
                } else {
                    println!("=====> no DML instance");
                }
            }
            if ev.header.event_type == 31{
                let (i, event) = UpdateRowEvent::decode(ev.payload.as_bytes()).unwrap();
                let (rest_i, (old_val, new_val)) = match UpdateRowEvent::fetch_rows(i, table_map.clone(), event.header.table_id, event.col_map_len){
                    Ok((i, (old_values, new_values)))=> (i, (old_values, new_values)),
                    Err(err)=>{
                        if let Some(ref mut data) = current_data {
                            println!("exec table:{} fail with: {err:?}", data.table);
                        }
                        panic!("{err:?}")
                    }
                };
                if let Some(ref mut data) = current_data {
                    data.append_data(seq_idx, "UPDATE".to_string(), new_val, old_val, ev.header.log_pos);
                    &worker.push(data);
                    seq_idx += 1;
                }else{
                    println!("=====> no DML instance");
                }
            }
            if ev.header.event_type == 32{
                let (i, event) = DeleteRowEvent::decode(ev.payload.as_bytes()).unwrap();
                let (i, old_values) = DeleteRowEvent::fetch_rows(i, table_map.clone(), event.header.table_id, event.col_map_len).expect("解码 Delete Val错误");
                //println!("======>Old val:{old_values:?} \n Rest update bytes: {i:?}");
                if let Some(ref mut data) = current_data {
                    data.append_data(seq_idx, "DELETE".to_string(), Vec::new(), old_values, ev.header.log_pos);
                    &worker.push(data);
                    seq_idx += 1;
                }else{
                    println!("=====> no DML instance");
                }
            }
            if ev.header.event_type == 16 {
            }
            if ev.header.event_type == 4 {
                let (i, rotate) = RotateEvent::decode(ev.payload.as_bytes()).unwrap();
                update_name_pos(posMng.clone(), &rotate.binlog_name, rotate.position as u32);
            }

        }

    }
}

