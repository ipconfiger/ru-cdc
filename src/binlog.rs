use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use bitflags::Flags;
use bytes::BytesMut;
use nom::error::{Error, ErrorKind, VerboseError, VerboseErrorKind};
use nom::{AsBytes, AsChar, IResult, Err as NomErr};
use serde_json::{Value};
use crate::mysql::{Decoder, read_fps, take_bytes, take_eof_string, take_fix_string, take_i_int3, take_i_int4, take_i_int8, take_int1, take_int2, take_int3, take_int4, take_int6, take_int8, take_int_n, take_utf8_end_of_null};
use crate::protocal::{ VLenInt};
use std::io::{Cursor};
use byteorder::{BigEndian, ReadBytesExt};


const DIG_PER_DEC: usize = 9;
const COMPRESSED_BYTES: [usize; 10] = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4];

struct DecimalVal {
    val: String
}

impl DecimalVal {
    fn decode(input: &[u8], precision: u8, scale: u8) -> IResult<&[u8], Self> {
        let integral = precision - scale;
        let uncomp_intg = integral / DIG_PER_DEC as u8;
        let uncomp_frac = scale / DIG_PER_DEC as u8;
        let comp_intg = integral - (uncomp_intg * DIG_PER_DEC as u8);
        let comp_frac = scale - (uncomp_frac * DIG_PER_DEC as u8);

        let comp_frac_bytes = COMPRESSED_BYTES[comp_frac as usize];
        let comp_intg_bytes = COMPRESSED_BYTES[comp_intg as usize];

        let total_bytes = 4 * uncomp_intg + 4 * uncomp_frac + comp_frac_bytes as u8 + comp_intg_bytes as u8;

        let (rest, decimal_bs) = take_bytes(input, total_bytes as usize)?;
        let is_negative = (decimal_bs[0] & 0x80) == 0;
        let mut bs = BytesMut::new();
        bs.resize(total_bytes as usize, 0);
        bs.extend_from_slice(decimal_bs);

        bs[0] ^= 0x80;
        if is_negative {
            for i in 0..total_bytes {
                bs[i as usize] ^= 0xFF;
            }
        }
        let mut intg_str = String::new();
        if is_negative {
            intg_str = "-".to_string();
        }
        let mut decimal_cursor = Cursor::new(bs.as_bytes());
        let mut is_intg_empty = true;
        // compressed integral
        if comp_intg_bytes > 0 {
            let value = decimal_cursor.read_uint::<BigEndian>(comp_intg_bytes).unwrap();
            if value > 0 {
                intg_str += value.to_string().as_str();
                is_intg_empty = false;
            }
        }

        // uncompressed integral
        for _ in 0..uncomp_intg {
            let value = decimal_cursor.read_u32::<BigEndian>().unwrap();
            if is_intg_empty {
                if value > 0 {
                    intg_str += value.to_string().as_str();
                    is_intg_empty = false;
                }
            } else {
                intg_str += format!("{value:0size$}", value = value, size = DIG_PER_DEC).as_str();
            }
        }

        if is_intg_empty {
            intg_str += "0";
        }

        let mut frac_str = String::new();
        // uncompressed fractional
        for _ in 0..uncomp_frac {
            let value = decimal_cursor.read_u32::<BigEndian>().unwrap();
            frac_str += format!("{value:0size$}", value = value, size = DIG_PER_DEC).as_str();
        }

        // compressed fractional
        if comp_frac_bytes > 0 {
            let value = decimal_cursor.read_uint::<BigEndian>(comp_frac_bytes).unwrap();
            frac_str += format!("{value:0size$}", value = value, size = comp_frac as usize).as_str();
        }

        if frac_str.is_empty() {
            Ok((rest, Self{ val: intg_str }))
        } else {
            Ok((rest, Self{ val: intg_str + "." + frac_str.as_str() }))
        }


    }
}


fn parse_bcd(input: &[u8], digits: usize) -> String {
    input.iter().flat_map(|&byte| {
        vec![
            byte >> 4, // 高四位
            byte & 0x0F, // 低四位
        ]
    })
        .take(digits) // 只取需要的位数
        .map(|n| char::from_digit(n as u32, 10).unwrap())
        .collect()
}

fn read_bits(input: &[u8], start: usize, count: usize) -> u32 {
    if count > 32 {
        panic!("Cannot read more than 32 bits into a u32.");
    }

    let mut result = 0u32;
    for i in 0..count {
        let bit_pos = start + i;
        let byte_pos = bit_pos / 8;
        let bit_in_byte = bit_pos % 8;
        let bit = (input[byte_pos] >> (7 - bit_in_byte)) & 1;
        result |= (bit as u32) << (count - 1 - i);
    }
    result
}

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
    TINYINT = 1,               // 1
    SMALLINT = 2,              // 2
    INT = 3,                   // 4
    FLOAT = 4,                 // 4
    DOUBLE = 5,                // 8
    BIGINT = 8,                // 8
    MEDIUMINT = 9,             // 3
    DECIMAL = 246,             // 8,8
    // above are number type
    DATE = 10,                 // 3
    TIME = 19,                 // 3
    DATETIME = 18,             // 8
    YEAR = 13,                 // 1
    TIMESTAMP = 17,            // 4
    // above are date & time type
    CHAR = 254,                // String
    VARCHAR = 15,
    TEXT = 252,
    NOT_MATCH =-1
}

impl From<u8> for ColumnType {
    fn from(value: u8) -> Self {
        match value {
            1=>Self::TINYINT,
            2=>Self::SMALLINT,
            3=>Self::INT,
            4=>Self::FLOAT,
            5=>Self::DOUBLE,
            8=>Self::BIGINT,
            9=>Self::MEDIUMINT,
            246=>Self::DECIMAL,
            10=>Self::DATE,
            19=>Self::TIME,
            18=>Self::DATETIME,
            13=>Self::YEAR,
            17=>Self::TIMESTAMP,
            254=>Self::CHAR,
            15=>Self::VARCHAR,
            252=>Self::TEXT,
            _=>Self::NOT_MATCH,
        }
    }
}

impl ColumnType {
    fn decode_val<'a>(tp: ColumnType, input: &'a [u8], meta: &ColMeta) -> IResult<&'a [u8], Value> {
        match tp {
            Self::TINYINT=>{
                let (i, val) = take_int1(input)?;
                Ok((i, Value::from(val as u8)))
            },
            Self::SMALLINT=>{
                let (i, val) = take_int2(input)?;
                Ok((i, Value::from(val as u16)))
            },
            Self::MEDIUMINT=>{
                let (i, val) = take_i_int3(input)?;
                Ok((i, Value::from(val)))
            },
            Self::INT=>{
                let (i, val) = take_i_int4(input)?;
                Ok((i, Value::from(val)))
            },
            Self::BIGINT=>{
                let (i, val) = take_i_int8(input)?;
                Ok((i, Value::from(val)))
            },
            Self::FLOAT=>{
                let (i, bs) = take_bytes(input, 4usize)?;
                let mut val = [0u8; 4];
                val.copy_from_slice(bs);
                let val = f32::from_le_bytes(val);
                Ok((i, Value::from(val)))
            },
            Self::DOUBLE=>{
                let (i, bs) = take_bytes(input, 8usize)?;
                let mut val = [0u8; 8];
                val.copy_from_slice(bs);
                let val = f64::from_le_bytes(val);
                Ok((i, Value::from(val)))
            },
            Self::DECIMAL=>{
                let precision = meta.precision.unwrap_or(0u8) as usize;
                let decimals = meta.decimals.unwrap_or(0u8) as usize;
                let (i, dec) = DecimalVal::decode(input, precision as u8, decimals as u8)?;
                Ok((i, Value::from(dec.val)))
            },
            Self::DATE=>{
                let (i, time) = take_int3(input)?;
                if time == 0{
                    Ok((i, Value::Null))
                }else{
                    let year = (time & ((1 << 15) - 1) << 9) >> 9;
                    let month = (time & ((1 << 4) - 1) << 5) >> 5;
                    let day = time & ((1 << 5) - 1);
                    Ok((i, Value::from(format!("{:02}-{:02}-{:02}", year, month, day))))
                }
            },
            Self::TIME=>{
                let (i, time) = take_bytes(input, 3usize)?;
                let sign = read_bits(time, 0, 1);
                let mut buffer = [0u8; 4];
                let data = if sign < 1 {
                    let n = u32::from_le_bytes([time[0], time[1], time[2], 0]);
                    let n = !n + 1;
                    let bs = u32::to_le_bytes(n);
                    buffer.copy_from_slice(&bs);
                    &buffer[..3]
                }else{ time };
                let hours= read_bits(data, 2, 10);
                let minutes = read_bits(data, 12, 6);
                let seconds = read_bits(data,18, 6);
                let (i, microsecond) = read_fps(i, meta.fsp.unwrap_or(0u8))?;
                let val = format!("{hours:02}:{minutes:02}:{seconds:02}.{microsecond}");
                Ok((i, Value::from(val)))
            },
            Self::DATETIME=>{
                let (i, dt) = take_bytes(input, 5usize)?;
                let val = u64::from_be_bytes([0u8, 0u8, 0u8, dt[0], dt[1], dt[2], dt[3], dt[4]]) - 0x8000000000;
                let d_val = val >> 17;
                let t_val = val % (1 << 17);
                let year = ((d_val >> 5) / 13) as u32;
                let month = ((d_val >> 5) % 13) as u32;
                let day = (d_val % (1 << 5)) as u32;
                let hour = ((val >> 12) % (1 << 5)) as u32;
                let minute = ((t_val >> 6) % (1 << 6)) as u32;
                let second = (t_val % (1 << 6)) as u32;
                let (i, microsecond) = read_fps(i, meta.fsp.unwrap_or(0u8))?;
                if meta.fsp.unwrap_or(0u8) > 0u8 {
                    let datetime_str = format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{microsecond:03}");
                    Ok((i, Value::from(datetime_str)))
                }else{
                    let datetime_str = format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}");
                    Ok((i, Value::from(datetime_str)))
                }
            },
            Self::YEAR=>{
                let (i, dt) = take_int1(input)?;
                Ok((i, Value::from(1900u16 + dt as u16)))
            }
            Self::TIMESTAMP=>{
                let (i, bs) = take_bytes(input, 4usize)?;
                let mut val = [0u8; 4];
                val.copy_from_slice(bs);
                let val = i32::from_be_bytes(val);
                let (i, microsecond) = read_fps(i, meta.fsp.unwrap_or(0u8))?;
                let ts = format!("{val}.{microsecond}");
                Ok((i, Value::from(ts)))
            },
            Self::CHAR | Self::VARCHAR=>{
                if meta.max_length.unwrap_or(0u16) > 255u16 {
                    let (i, slen) = take_int2(input)?;
                    let (i, val_str) = take_fix_string(i, slen as usize)?;
                    Ok((i, Value::from(val_str)))
                }else{
                    let (i, slen) = take_int1(input)?;
                    let (i, val_str) = take_fix_string(i, slen as usize)?;
                    Ok((i, Value::from(val_str)))
                }
            },
            Self::TEXT=>{
                let length_size = meta.length_size.unwrap_or(1u8);
                let (i, str_len) = take_int_n(input, length_size as usize)?;
                //println!("BLOB str len:{str_len}");
                let (i, bs) = take_bytes(i, str_len as usize)?;
                let v = Value::from(bs);
                Ok((i, v))
            },
            _=>Err(NomErr::Error(Error::new(input, ErrorKind::Eof)))
        }
    }

}

#[derive(Clone)]
pub struct ColMeta {
    pub max_length: Option<u16>,
    pub size: Option<u8>,
    pub fsp: Option<u8>,
    pub length_size: Option<u8>,
    pub precision: Option<u8>,
    pub decimals: Option<u8>,
    pub real_type: Option<u8>
}

impl ColMeta {
    fn new() -> Self {
        ColMeta{
            max_length: None,
            size: None,
            fsp: None,
            length_size: None,
            precision: None,
            decimals: None,
            real_type: None,
        }
    }
}

impl Debug for ColMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut fs = f.debug_struct("Meta");
        if self.max_length.is_some() {
            fs.field("max_length", &self.max_length.unwrap());
        }
        if self.size.is_some(){
            fs.field("size", &self.size.unwrap());
        }
        if self.fsp.is_some() {
            fs.field("fsp", &self.fsp.unwrap());
        }
        if self.length_size.is_some() {
            fs.field("length_size", &self.length_size.unwrap());
        }
        if self.precision.is_some() {
            fs.field("precision", &self.precision.unwrap());
        }
        if self.decimals.is_some() {
            fs.field("decimals", &self.decimals.unwrap());
        }
        if self.real_type.is_some() {
            fs.field("real_type", &self.real_type.unwrap());
        }
        fs.finish()
    }
}


#[derive(Debug, Clone)]
pub struct TableMap {
    pub mapping: HashMap<u64, Vec<ColumnType>>,
    pub metas: HashMap<u64, Vec<ColMeta>>
}

impl TableMap {
    pub fn new() -> Self {
        Self{
            mapping: HashMap::new(),
            metas: HashMap::new()
        }
    }

    pub fn decode_columns(&mut self, tb: u64, cols_type: Vec<u8>, meta_bits: &[u8]) {
        let mut types: Vec<ColumnType> = Vec::new();
        let mut metas: Vec<ColMeta> = Vec::new();
        let mut i = meta_bits;
        for type_flag in cols_type {
            let tp = ColumnType::from(type_flag);
            let mut m = ColMeta::new();
            types.push(tp);
            let meta = match tp {
                ColumnType::VARCHAR=>{
                    if let Ok((ni, maxlength)) = take_int2(i){
                        m.max_length = Some(maxlength);
                        i = ni;
                    }
                    m
                },
                ColumnType::CHAR => {
                    if let Ok((ni, bs)) = take_bytes(i, 2usize){
                        let metadata = (bs[0] as u16) << 8 | bs[1] as u16;
                        let rs = (((metadata >> 4) & 0x300) ^ 0x300) + (metadata & 0x00FF);
                        m.max_length = Some(rs);
                        i = ni;
                    }
                    m
                }
                ColumnType::DOUBLE=>{
                    if let Ok((ni, size)) = take_int1(i) {
                        m.size = Some(size as u8);
                        i = ni;
                    }
                    m
                },
                ColumnType::FLOAT=>{
                    if let Ok((ni, size)) = take_int1(i) {
                        m.size = Some(size as u8);
                        i = ni
                    }
                    m
                },
                ColumnType::TIMESTAMP | ColumnType::DATETIME | ColumnType::TIME =>{
                    if let Ok((ni, size)) = take_int1(i) {
                        m.fsp = Some(size as u8);
                        i = ni;
                    }
                    m
                },
                ColumnType::TEXT=>{
                    if let Ok((ni, size)) = take_int1(i) {
                        m.length_size = Some(size as u8);
                        i = ni;
                    }
                    m
                },
                ColumnType::DECIMAL=>{
                    if let Ok((ni, size)) = take_int1(i) {
                        m.precision = Some(size as u8);
                        i = ni;
                    }
                    if let Ok((ni, size)) = take_int1(i) {
                        m.decimals = Some(size as u8);
                        i = ni;
                    }
                    m
                },
                _=>ColMeta::new()
            };
            metas.push(meta.clone());
            //println!("{tp:?} with meta:{meta:?} rest:{i:?}");
        }
        //println!("mapped columns:{types:?}");
        if let Some(val) = self.mapping.insert(tb, types){
            //println!("更新列映射");
        }else{
            //println!("新增列映射");
        }
        if let Some(m) = self.metas.insert(tb, metas) {
            //println!("更新列元数据")
        }else{
            //println!("新增列元数据")
        }
    }

    pub fn decode_column_vals<'a>(&'a mut self, input: &'a [u8], table_id: u64, col_map_len: usize) -> IResult<&[u8], Vec<Value>> {
        let (ip, null_map1) = take_bytes(input, col_map_len)?;
        let mut col_types = self.mapping.get_mut(&table_id).unwrap();
        let mut values:Vec<Value> = Vec::new();
        let mut metas = self.metas.get_mut(&table_id).unwrap();
        let mut i = ip;
        for (idx, mut col_type) in col_types.iter_mut().enumerate() {
            let meta = metas[idx].clone();
            //println!("{col_type:?} use meta: {meta:?} idx: {idx}");
            let (new_i, val) = ColumnType::decode_val(col_type.clone(), i, &meta)?;
            i = new_i;
            values.push(val);
        }
        Ok((i, values.clone()))
    }
}


impl Decoder for EventHeaderFlag {
    fn decode(input: &[u8]) -> IResult<&[u8], Self> {
        let (i, flags) = take_int2(input)?;
        if let Some(ins) = Self::from_bits(flags as u16){
            Ok((i, ins))
        }else{
            Err(NomErr::Error(Error::new(input, ErrorKind::Eof)))
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
    fn decode(input: &[u8]) -> IResult<&[u8], Self> {
        let (i, _) = take_int1(input)?;
        let (i, timestemp) = take_int4(i)?;
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
    fn decode(input: &[u8]) -> IResult<&[u8], Self> {
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
    fn decode(input: &[u8]) -> IResult<&[u8], Self> {
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
    pub column_types: Vec<u8>,
    pub column_metas: Vec<u8>
}

impl Decoder for TableMapEvent {
    fn decode(input: &[u8]) -> IResult<&[u8], Self> {
        let (i, header) = RowEventHeader::decode(input)?;
        let (i, schema_len) = take_int1(i)?;
        let (i, schema_name) = take_utf8_end_of_null(i)?;
        let (i, table_name_len) = take_int1(i)?;
        let (i, table_name) = take_utf8_end_of_null(i)?;
        let (i, column_count) = VLenInt::decode(i)?;
        let (i, column_map) = take_bytes(i, column_count.int() as usize)?;
        //println!("cloumn_map: {column_map:?}");
        let (i, meta_count) = VLenInt::decode(i)?;
        let (i, meta_block) = take_bytes(i, meta_count.int() as usize)?;
        //println!("meta_block: {meta_block:?}");
        //println!("Rest:{i:?}");
        Ok((i, Self{
            header,
            schema_name,
            table_name,
            column_count: column_count.int() as u8,
            column_types: Vec::from(column_map),
            column_metas: Vec::from(meta_block)
        }))
    }
}

#[derive(Debug, Clone)]
pub struct WriteRowEvent {
    pub header: RowEventHeader,
    pub col_count: u32,
    pub col_map_len: usize
}
impl WriteRowEvent {
    pub fn decode_column_multirow_vals<'a>(table_map: &TableMap, input: &'a [u8], table_id: u64, col_map_len: usize) -> IResult<&'a [u8], Vec<Vec<Value>>> {
        let mut rest_input = input;
        let mut rows:Vec<Vec<Value>> = Vec::new();
        loop{
            let (i, vals) = decode_column_vals(table_map.clone(), rest_input, table_id, col_map_len)?;
            rest_input = i;
            rows.push(vals);
            if rest_input.len() <= 4 {
                break;
            }
        }
        Ok((rest_input, rows))
    }
}

impl Decoder for WriteRowEvent{
    fn decode(input: &[u8]) -> IResult<&[u8], Self> {
        let (i, header) = RowEventHeader::decode(input)?;
        let (i, extra_len) = take_int2(i)?;
        //println!("extra len:{extra_len}");
        let (i, _) = if extra_len > 2 {
            //处理大于2的extra数据
            let (i, extra_bs) = take_bytes(i, extra_len as usize)?;
            (i, extra_bs)
        }else{(i, i)};
        let (i, column_count) = VLenInt::decode(i)?;
        let col_map_len = (column_count.int() as u32 + 7u32)/8u32;
        let (i, col_map) = take_bytes(i, col_map_len as usize)?;
        //println!("rest:{:?}", i);
        Ok((i, Self{
            header,
            col_count:column_count.int() as u32,
            col_map_len: col_map_len as usize
        }))

    }
}

#[derive(Debug, Clone)]
pub struct UpdateRowEvent {
    pub header: RowEventHeader,
    pub col_map_len: usize,
}


impl Decoder for UpdateRowEvent{
    fn decode(input: &[u8]) -> IResult<&[u8], Self> {
        let (i, header) = RowEventHeader::decode(input)?;
        let (i, extra_len) = take_int2(i)?;
        //println!("extra len:{extra_len}");
        let (i, _) = if extra_len > 2 {
            //处理大于2的extra数据
            let (i, extra_bs) = take_bytes(i, extra_len as usize)?;
            (i, extra_bs)
        }else{(i, i)};
        let (i, column_count) = VLenInt::decode(i)?;
        //println!("共{}列", column_count.int());

        let col_map_len = (column_count.int() as u32 + 7u32)/8u32;
        //println!("列映射长度:{col_map_len}");
        let (i, col_map1) = take_bytes(i, col_map_len as usize)?;
        let (i, col_map2) = take_bytes(i, col_map_len as usize)?;
        //println!("列映射1:{col_map1:?} 列映射2:{col_map2:?}");
        Ok((i, Self{
            header,
            col_map_len: col_map_len as usize
        }))
    }
}

impl UpdateRowEvent {
    pub fn fetch_rows<'a>(input: &'a [u8], mut table_map: TableMap, table_id: u64, col_map_len: usize) -> IResult<&'a [u8], (Vec<Vec<Value>>, Vec<Vec<Value>>)> {
        let mut rest_input = input;
        let mut old_result:Vec<Vec<Value>> = Vec::new();
        let mut new_result:Vec<Vec<Value>> = Vec::new();
        loop {
            let (i, old_vals) = decode_column_vals(table_map.clone(), rest_input, table_id, col_map_len)?;
            //println!("old values:{old_vals:?}");
            let (i, new_vals) = decode_column_vals(table_map.clone(), i, table_id, col_map_len)?;
            //println!("new values:{new_vals:?}");
            rest_input = i;
            old_result.push(old_vals);
            new_result.push(new_vals);
            if rest_input.len() <= 4{
                break;
            }
        }
        Ok((rest_input, (old_result, new_result)))
    }
}

pub struct DeleteRowEvent {
    pub header: RowEventHeader,
    pub col_map_len: usize
}

impl DeleteRowEvent {
    pub fn fetch_rows<'a>(input: &'a [u8], mut table_map: TableMap, table_id: u64, col_map_len: usize) -> IResult<&'a [u8], Vec<Vec<Value>>> {
        let mut rest_input = input;
        let mut result:Vec<Vec<Value>> = Vec::new();
        loop {
            let (i, old_vals) = decode_column_vals(table_map.clone(), rest_input, table_id, col_map_len)?;
            rest_input = i;
            result.push(old_vals);
            if rest_input.len() <= 4 {
                break;
            }
        }
        Ok((rest_input, result))
    }
}

impl Decoder for DeleteRowEvent {
    fn decode(input: &[u8]) -> IResult<&[u8], Self> {
        //new_values
        let (i, header) = RowEventHeader::decode(input)?;
        let (i, extra_len) = take_int2(i)?;
        //println!("extra len:{extra_len}");
        let (i, _) = if extra_len > 2 {
            //处理大于2的extra数据
            let (i, extra_bs) = take_bytes(i, extra_len as usize)?;
            (i, extra_bs)
        }else{(i, i)};
        let (i, column_count) = VLenInt::decode(i)?;
        let col_map_len = (column_count.int() as u32 + 7u32)/8u32;
        let (i, col_map1) = take_bytes(i, col_map_len as usize)?;
        //println!("col map:{col_map1:?}");
        //println!("rest:{:?}", i);
        Ok((i, Self{
            header,
            col_map_len: col_map_len as usize
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
    fn decode(input: &[u8]) -> IResult<&[u8], Self> {
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
    fn decode(input: &[u8]) -> IResult<&[u8], Self>{
        let (i, header) = QueryEventHeader::decode(input)?;
        //println!("Query Rest:{i:?}");
        let (i, bs) = take_bytes(i, header.status_len as usize)?;
        //println!("status rest:{bs:?}");
        let (i, database) = take_utf8_end_of_null(i)?;
        let (i, statement) = take_eof_string(&i[0..i.len()-4])?;
        Ok((i, Self{ header, database, statement }))
    }
}


pub struct RotateEvent {
    pub position: u8,
    pub binlog_name: String
}

impl Decoder for RotateEvent{
    fn decode(input: &[u8]) -> IResult<&[u8], Self> {
        let (i, position) = take_bytes(input, 1usize)?;
        let (i, binlog_name) = take_eof_string(i)?;
        Ok((i, Self{
            position: 0,
            binlog_name,
        }))
    }
}


fn check_bit(arr: &[u8], index: u16) -> u8 {
    let byte_index = (index / 8) as usize;
    let bit_index = (index % 8) as u8;
    (arr[byte_index] >> bit_index) & 1
}

fn compute_null_map(arr: &[u8], col_map_len: usize) -> Vec<bool> {
    let mut map:Vec<bool> = Vec::new();
    for idx in 0..col_map_len {
        map.push(check_bit(arr, idx as u16) > 0);
    }
    map
}

pub fn decode_column_vals<'a>(mut table_map: TableMap, input: &'a [u8], table_id: u64, col_map_len: usize) -> IResult<&[u8], Vec<Value>> {
    let (ip, null_map1) = take_bytes(input, col_map_len)?;
    let mut values:Vec<Value> = Vec::new();
    let mut metas = &mut table_map.metas.get_mut(&table_id).unwrap();
    let null_map = compute_null_map(null_map1, metas.len());
    let mut i = ip;
    for (idx, mut col_type) in &mut table_map.mapping.get_mut(&table_id).unwrap().iter_mut().enumerate() {
        let meta = metas[idx].clone();
        if null_map[idx] {
            values.push(Value::from(None::<String>));
            continue;
        }
        let (new_i, val) = ColumnType::decode_val(col_type.clone(), i, &meta)?;
        i = new_i;
        values.push(val.clone());
    }
    Ok((i, values))
}