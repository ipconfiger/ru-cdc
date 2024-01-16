use std::collections::HashMap;
use bitflags::Flags;
use nom::error::ErrorKind;
use nom::IResult;
use crate::binlog::ColumnType::SET;
use crate::mysql::{Decoder, ParseError, take_bytes, take_eof_string, take_fix_string, take_int1, take_int2, take_int4, take_int6, take_utf8_end_of_null};
use crate::protocal::{err_maker, VLenInt};

bitflags::bitflags! {
    /// https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__binglog__event__header__flags.html
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    #[cfg_attr(feature="serde", serde::Serialize, serde::DeSerialize)]
    pub struct EventHeaderFlag: u16 {
        /// If the query depends on the thread (for example: TEMPORARY TABLE)
        const LOG_EVENT_THREAD_SPECIFIC_F=   0x4;
        /// Suppress the generation of 'USE' statements before the actual statement
        const LOG_EVENT_SUPPRESS_USE_F   =0x8;
        /// Artificial events are created arbitrarily and not written to binary log
        const LOG_EVENT_ARTIFICIAL_F =    0x20;
        /// Events with this flag set are created by slave IO thread and written to relay log
        const LOG_EVENT_RELAY_LOG_F =    0x40;
        /// For an event, 'e', carrying a type code, that a slave, 's', does not recognize, 's' will check 'e' for LOG_EVENT_IGNORABLE_F, and if the flag is set, then 'e' is ignored
        const LOG_EVENT_IGNORABLE_F =    0x80;
        /// Events with this flag are not filtered
        const LOG_EVENT_NO_FILTER_F =    0x100;
        /// MTS: group of events can be marked to force its execution in isolation from any other Workers
        const LOG_EVENT_MTS_ISOLATE_F =    0x200;
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ColumnType {
    DECIMAL = 0,
    TINY = 1,
    SHORT = 2,
    LONG = 3,
    FLOAT = 4,
    DOUBLE = 5,
    NULL = 6,
    TIMESTAMP = 7,
    LONGLONG = 8,
    INT24 = 9,
    DATE = 10,
    TIME = 11,
    DATETIME = 12,
    YEAR = 13,
    NEWDATE = 14,
    VARCHAR = 15,
    BIT = 16,
    TIMESTAMP2 = 17,
    DATETIME2 = 18,
    TIME2 = 19,
    JSON = 245,
    NEWDECIMAL = 246,
    ENUM = 247,
    SET = 248,
    TINY_BLOB = 249,
    MEDIUM_BLOB = 250,
    LONG_BLOB = 251,
    BLOB = 252,
    VAR_STRING = 253,
    STRING = 254,
    GEOMETRY = 255,
    NOT_MATCH =-1
}

impl From<u8> for ColumnType {
    fn from(value: u8) -> Self {
        match value {
            0=>Self::DECIMAL,
            1=>Self::TINY,
            2=>Self::SHORT,
            3=>Self::LONG,
            4=>Self::FLOAT,
            5=>Self::DOUBLE,
            6=>Self::NULL,
            7=>Self::TIMESTAMP,
            8=>Self::LONGLONG,
            9=>Self::INT24,
            10=>Self::DATE,
            11=>Self::TIME,
            12=>Self::DATETIME,
            13=>Self::YEAR,
            14=>Self::NEWDATE,
            15=>Self::VARCHAR,
            16=>Self::BIT,
            17=>Self::TIMESTAMP2,
            18=>Self::DATETIME2,
            19=>Self::TIME2,
            245=>Self::JSON,
            246=>Self::NEWDECIMAL,
            247=>Self::ENUM,
            248=>Self::SET,
            249=>Self::TINY_BLOB,
            250=>Self::MEDIUM_BLOB,
            251=>Self::LONG_BLOB,
            252=>Self::BLOB,
            253=>Self::VAR_STRING,
            254=>Self::STRING,
            255=>Self::GEOMETRY,
            _ => Self::NOT_MATCH
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableMap {
    pub mapping: HashMap<u64, Vec<ColumnType>>
}

impl TableMap {
    pub fn new() -> Self {
        Self{
            mapping: HashMap::new()
        }
    }

    pub fn decode_columns(&mut self, tb: u64, cols_type: Vec<u8>) {
        let mut types: Vec<ColumnType> = Vec::new();
        for type_flag in cols_type {
            types.push(ColumnType::from(type_flag));
        }
        println!("mapped columns:{types:?}");
        if let Some(val) = self.mapping.insert(tb, types){
            println!("更新列映射");
        }else{
            println!("新增列映射");
        }
    }

    //pub fn decode_column_vals(&mut self, input: &[u8], table_id: u64) -> IResult<&[u8], >

}


impl Decoder for EventHeaderFlag {
    fn decode(input: &[u8]) -> IResult<&[u8], Self, ParseError> where Self: Sized {
        let (i, flags) = take_int2(input)?;
        if let Some(ins) = Self::from_bits(flags as u16){
            Ok((i, ins))
        }else{
            Err(err_maker(i, ErrorKind::Verify))
        }
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", serde::Serialize, serde::DeSerialize)]
pub struct EventHeader {
    pub timestamp: u32,
    pub event_type: u8,
    pub server_id: u32,
    pub event_size: u32,
    pub log_pos: u32,
    pub flags: EventHeaderFlag,
}

impl Decoder for EventHeader {
    fn decode(input: &[u8]) -> IResult<&[u8], Self, ParseError> where Self: Sized {
        let (i, _) = take_int1(input)?;
        let (i, timestemp) = take_int4(i)?;
        println!("ev type:{}", i[0]);
        let (i, event_type) = take_int1(i)?;
        let (i, server_id) = take_int4(i)?;
        let (i, event_size) = take_int4(i)?;
        let (i, log_pos) = take_int4(i)?;
        let (i, flags) = EventHeaderFlag::decode(i)?;
        Ok((i, Self {
            timestamp:timestemp,
            event_type: event_type as u8,
            server_id,
            event_size,
            log_pos,
            flags,
        }))
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", serde::Serialize, serde::DeSerialize)]
pub struct EventRaw {
    pub header: EventHeader,
    pub payload: Vec<u8>,
}

impl Decoder for EventRaw {
    fn decode(input: &[u8]) -> IResult<&[u8], Self, ParseError> where Self: Sized {
        let (i, header) = EventHeader::decode(input)?;
        Ok((&[], Self{
            header,
            payload: Vec::from(i)
        }))
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", serde::Serialize, serde::DeSerialize)]
pub struct Event<P> {
    pub header: EventHeader,
    pub payload: P,
}

#[derive(Debug, Clone)]
pub struct RowEventHeader {
    pub table_id: u64,
    pub flag: u16,
}

impl RowEventHeader {
    pub const EventType: u8 = 19;
}

impl Decoder for RowEventHeader {
    fn decode(input: &[u8]) -> IResult<&[u8], Self, ParseError> where Self: Sized {
        let (i, table_id) = take_int6(input)?;
        let (i, flag) = take_int2(i)?;
        Ok((i, Self{table_id, flag:flag as u16}))
    }
}


#[derive(Debug, Clone)]
pub struct TableMapEvent {
    pub header: RowEventHeader,
    pub schema_name: String,
    pub table_name: String,
    pub column_count: u8,
    pub column_types: Vec<u8>
}

impl Decoder for TableMapEvent {
    fn decode(input: &[u8]) -> IResult<&[u8], Self, ParseError> where Self: Sized {
        let (i, header) = RowEventHeader::decode(input)?;
        let (i, schema_len) = take_int1(i)?;
        let (i, schema_name) = take_utf8_end_of_null(i)?;
        let (i, table_name_len) = take_int1(i)?;
        let (i, table_name) = take_utf8_end_of_null(i)?;
        let (i, column_count) = VLenInt::decode(i)?;
        let (i, column_map) = take_bytes(i, column_count.int() as usize)?;
        println!("cloumn_map: {column_map:?}");
        let (i, meta_count) = VLenInt::decode(i)?;
        let (i, meta_block) = take_bytes(i, meta_count.int() as usize)?;
        println!("meta_block: {meta_block:?}");
        let (i, null_map) = take_bytes(i, meta_count.int() as usize)?;
        println!("null_map: {null_map:?}");

        println!("Rest:{i:?}");
        Ok((i, Self{
            header,
            schema_name,
            table_name,
            column_count: column_count.int() as u8,
            column_types: Vec::from(column_map)
        }))
    }
}

#[derive(Debug, Clone)]
pub struct WriteRowEvent {
    pub header: RowEventHeader,

}

impl Decoder for WriteRowEvent{
    fn decode(input: &[u8]) -> IResult<&[u8], Self, ParseError> where Self: Sized {
        let (i, header) = RowEventHeader::decode(input)?;
        let (i, extra_len) = take_int2(i)?;
        println!("extra len:{extra_len}");
        if extra_len > 2 {
            //处理大于2的extra数据
        }
        println!("rest:{:?}", i);
        let (i, column_count) = VLenInt::decode(i)?;
        println!("column:{}", column_count.int());


        Ok((i, Self{
            header,
        }))

    }
}

#[derive(Debug, Clone)]
pub struct UpdateRowEvent {
    pub header: RowEventHeader,
}

impl Decoder for UpdateRowEvent{
    fn decode(input: &[u8]) -> IResult<&[u8], Self, ParseError> where Self: Sized {
        let (i, header) = RowEventHeader::decode(input)?;
        println!("UPdate Rest: {i:?}");
        Ok((i, Self{
            header,
        }))
    }
}



#[derive(Debug, Clone)]
pub struct QueryEventHeader {
    pub thread_id: u32,
    pub timestamp: u32,
    pub db_name_len: usize,
    pub error_code: u16,
    pub status_len: u16
}

impl Decoder for QueryEventHeader{
    fn decode(input: &[u8]) -> IResult<&[u8], Self, ParseError> where Self: Sized {
        let (i, thread_id) = take_int4(input)?;
        let (i, timestamp) = take_int4(i)?;
        let (i, db_len) = take_int1(i)?;
        let (i, err_code) = take_int2(i)?;
        let (i, status_len) = take_int2(i)?;
        Ok((i, Self{
            thread_id,
            timestamp,
            db_name_len: db_len as usize,
            error_code: err_code as u16,
            status_len: status_len as u16,
        }))
    }
}

#[derive(Debug, Clone)]
pub struct QueryEvent {
    pub header: QueryEventHeader,
    pub database: String,
    pub statement: String
}

impl Decoder for QueryEvent {
    fn decode(input: &[u8]) -> IResult<&[u8], Self, ParseError> where Self: Sized {
        let (i, header) = QueryEventHeader::decode(input)?;
        println!("Query Rest:{i:?}");
        let (i, bs) = take_bytes(i, header.status_len as usize)?;
        println!("status rest:{bs:?}");
        let (i, database) = take_utf8_end_of_null(i)?;
        let (i, statement) = take_eof_string(&i[0..i.len()-4])?;
        Ok((i, Self{ header, database, statement }))
    }
}

