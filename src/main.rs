mod mysql;
mod protocal;
mod binlog;

use std::{
    io::{Read, Write},
    net::TcpStream,
};
use bytes::BytesMut;
use nom::AsBytes;
use crate::binlog::{EventHeader, EventRaw, QueryEvent, TableMap, TableMapEvent, UpdateRowEvent, WriteRowEvent};
use crate::mysql::{Decoder, MySQLConnection, native_password_auth, Packet};
use crate::protocal::{AuthSwitchReq, AuthSwitchResp, Capabilities, ComBinLogDump, ComQuery, HandshakeResponse41, HandshakeV10, OkPacket};

fn main() {
    let stream = TcpStream::connect("192.168.1.222:3399").unwrap();
    let mut conn = MySQLConnection::from_tcp(stream);
    let (i, p) = conn.read_package::<HandshakeV10>().expect("read error");
    println!("read packet: {:?}", p);
    let mut auth_resp = BytesMut::new();
    //auth_resp.extend_from_slice(&auth_data);
    let resp = HandshakeResponse41 {
        caps: Capabilities::CLIENT_LONG_PASSWORD
            | Capabilities::CLIENT_PROTOCOL_41
            | Capabilities::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
            | Capabilities::CLIENT_RESERVED
            | Capabilities::CLIENT_RESERVED2
            | Capabilities::CLIENT_DEPRECATE_EOF
            | Capabilities::CLIENT_PLUGIN_AUTH,
        max_packet_size: 4294967295,
        charset: 255,
        user_name: "canal".into(),
        auth_resp,
        database: None,
        plugin_name: Some("canal".into()),
        connect_attrs: Default::default(),
        zstd_level: 0,
    };
    conn.write_package(1, &resp).expect("Write Error");
    let (i, switch_req) = conn.read_package::<AuthSwitchReq>().expect("auth error");
    println!("read switch: {:?}", switch_req);
    if switch_req.payload.plugin_name != "mysql_native_password" {
        panic!("")
    }
    let auth_data = native_password_auth("canal".as_bytes(), &p.payload.auth_plugin_data);
    println!("auth data: {:?}", auth_data);
    let resp = AuthSwitchResp {
        data: BytesMut::from_iter(auth_data),
    };
    conn.write_package(3, &resp).expect("sent auth error");


    let (i, resp) = conn.read_package::<OkPacket>().unwrap();
    println!("ok resp:{:?}", resp.payload);

    let query = ComQuery{query: "set @master_binlog_checksum= @@global.binlog_checksum".to_string()};
    conn.write_package(0, &query).unwrap();
    let (i, resp) = conn.read_package::<OkPacket>().unwrap();
    println!("ok resp:{:?}", resp);

    let query: ComQuery = "show master status".into();
    conn.write_package(0, &query).unwrap();

    let (_, text_resp) = conn.read_text_result_set().unwrap();
    println!("text result is :{:?}", text_resp);

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

    loop {
        let (_, buf) = conn.read_package::<Vec<u8>>().unwrap();
        //println!("raw ev:{:?}", &buf);
        let event_result = EventRaw::decode(buf.payload.as_bytes());
        if let Ok((_, ev)) = event_result {
            println!("Get Event: {:?}", ev);
            if ev.header.event_type == 19 {
                let (i, tablemap) = TableMapEvent::decode(ev.payload.as_bytes()).expect("table map error");
                println!("table map:{:?}", tablemap);
                table_map.decode_columns(tablemap.header.table_id, tablemap.column_types, tablemap.column_metas.as_bytes());
                println!("meta map:{:?}", table_map.metas);
            }
            if ev.header.event_type == 30 {
                let (i, event) = WriteRowEvent::decode(ev.payload.as_bytes()).unwrap();
                table_map.decode_column_vals(i, event.header.table_id).expect("解码数据错误");
                println!("insert event:{:?}", event);
            }
            if ev.header.event_type == 31{
                let (i, event) = UpdateRowEvent::decode(ev.payload.as_bytes()).unwrap();
                println!("update event:{:?}", event);
            }
            if ev.header.event_type == 2 {
                let (i, query) = QueryEvent::decode(ev.payload.as_bytes()).unwrap();
                println!("statement event:{:?}", query);
            }

        }

    }

    println!("Done!");
}

