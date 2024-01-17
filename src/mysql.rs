use std::fmt::Debug;
use std::io::{Read, Write};
use std::net::TcpStream;
use nom::{IResult, error, Err, bytes::{complete}, AsBytes};
use bytes::{Buf, BufMut, BytesMut};
use nom::error::ErrorKind;
use crate::protocal::{ColDef, err_maker, OkPacket, TextResult, TextResultSet, VLenInt};

pub(crate) type ParseError<'a> = error::Error<&'a [u8]>;


pub struct MySQLConnection {
    conn: TcpStream
}

impl MySQLConnection {
    pub(crate) fn from_tcp(tcp: TcpStream) -> Self {
        Self{conn: tcp}
    }

    pub fn read_package<P:Decoder+Debug>(&mut self) -> Result<(&[u8], Packet<P>), Err<ParseError<'_>>> {
        let mut buff = BytesMut::new();
        buff.resize(4, 0);
        self.conn.read_exact(&mut buff).expect("read error");
        let (_, header) = Header::decode(buff.chunk()).expect("");
        //println!("header:{:?}", header);
        let mut buff = BytesMut::with_capacity(header.len as usize);
        buff.resize(header.len as usize, 0);
        self.conn.read_exact(&mut buff).expect("read body error");
        //println!("body pack:{:?}", buff.to_vec());
        let (_, payload) =  P::decode(buff.chunk()).expect("");
        Ok((&[], Packet { header, payload}))
    }

    pub fn read_text_result_set(&mut self) -> Result<(&[u8], TextResultSet), Err<ParseError<'_>>> {
        let column_count = self.read_package::<VLenInt>().expect("column count error").1.payload;
        let mut col_defs = vec![];
        for _ in 0.. column_count.0 as usize {
            let col = self.read_package::<ColDef>().expect("read col def error").1.payload;
            //println!("col def:{:?}", &col);
            col_defs.push(col)
        }
        let mut rows = vec![];
        loop {
            let packet = self.read_package::<Vec<u8>>();
            let (i, buf)  = packet.expect("read line error");
            let mut buf = BytesMut::from_iter(buf.payload);
            //println!("row data:{:?}", &buf);
            if buf.first() == Some(&0xfe) && buf.len() < 9 {
                let okrs = OkPacket::decode(&mut buf);
                if okrs.is_err(){
                    if let Err(err) = okrs {
                        println!("ok err:{:?}", err);
                        panic!("test");
                    }
                }
                break;
            }
            let (i, row) = TextResult::decode(&mut buf).unwrap();
            //println!("row: {:?}", &row);
            rows.push(row);
        }
        Ok((&[], TextResultSet {
            column_count,
            col_defs,
            rows,
        }))
    }

    pub fn write_package<P:Encoder+Debug>(&mut self,
                                    seq_id: u8,
                                    payload: &P)-> Result<(), std::io::Error> {
        let mut buff = BytesMut::new();
        encode_package::<P>(&mut buff, seq_id, payload);
        println!("write command {:?}: {:?}", payload, &buff.as_bytes());
        self.conn.write_all(&buff)
    }



}


pub fn take_int1(i: &[u8])->IResult<&[u8], u8, ParseError> {
    let (i, n_bytes) = complete::take::<usize, &[u8], ParseError>(1)(i)?;
    Ok((i, n_bytes[0]))
}

pub fn take_int2(i: &[u8])->IResult<&[u8], u16, ParseError> {
    let (i, n_bytes) = complete::take::<usize, &[u8], ParseError>(2)(i)?;
    let mut bs = [0u8; 2];
    bs.copy_from_slice(n_bytes);
    Ok((i, u16::from_le_bytes(bs)))
}

pub fn take_int3(i: &[u8])->IResult<&[u8], u32, ParseError> {
    let (i, n_bytes) = complete::take::<usize, &[u8], ParseError>(3)(i)?;
    Ok((i, u32::from_le_bytes([n_bytes[0], n_bytes[1], n_bytes[2], 0])))
}

pub fn take_int4(i: &[u8])->IResult<&[u8], u32, ParseError> {
    let (i, n_bytes) = complete::take::<usize, &[u8], ParseError>(4)(i)?;
    let mut bs = [0u8; 4];
    bs.copy_from_slice(n_bytes);
    Ok((i, u32::from_le_bytes(bs)))
}

pub fn take_int5(i: &[u8])->IResult<&[u8], u64, ParseError> {
    let (i, n_bytes) = complete::take::<usize, &[u8], ParseError>(5)(i)?;
    Ok((i, u64::from_le_bytes([n_bytes[0], n_bytes[1], n_bytes[2], n_bytes[3], n_bytes[4], 0, 0, 0])))
}

pub fn take_int6(i: &[u8])->IResult<&[u8], u64, ParseError> {
    let (i, n_bytes) = complete::take::<usize, &[u8], ParseError>(6)(i)?;
    Ok((i, u64::from_le_bytes([n_bytes[0], n_bytes[1], n_bytes[2], n_bytes[3], n_bytes[4], n_bytes[5], 0, 0])))
}

pub fn take_int7(i: &[u8])->IResult<&[u8], u64, ParseError> {
    let (i, n_bytes) = complete::take::<usize, &[u8], ParseError>(7)(i)?;
    Ok((i, u64::from_le_bytes([n_bytes[0], n_bytes[1], n_bytes[2], n_bytes[3], n_bytes[4], n_bytes[5], n_bytes[6], 0])))
}

pub fn take_int8(i: &[u8])->IResult<&[u8], u64, ParseError> {
    let (i, n_bytes) = complete::take::<usize, &[u8], ParseError>(8)(i)?;
    let mut bs = [0u8; 8];
    bs.copy_from_slice(n_bytes);
    Ok((i, u64::from_le_bytes(bs)))
}


pub fn take_int_n(i: &[u8], n: usize) -> IResult<&[u8], u64, ParseError> {
    let (i, u) = match n {
        1usize=>{
            let (i, v) = take_int1(i)?;
            (i, v as u64)
        },
        2usize=>{
            let (i, v) = take_int2(i)?;
            (i, v as u64)
        },
        3usize=>{
            let (i, v) = take_int3(i)?;
            (i, v as u64)
        },
        4usize=>{
            let (i, v) = take_int4(i)?;
            (i, v as u64)
        },
        5usize=>take_int5(i)?,
        6usize=>take_int6(i)?,
        7usize=>take_int7(i)?,
        8usize=>take_int8(i)?,
        _=>(i, 0)
    };
    Ok((i, u as u64))
}

pub fn take_be_int(i: &[u8], n: usize) -> IResult<&[u8], i64, ParseError>{
    let (i, bs) = take_bytes(i, n)?;
    println!("======> ms bs:{bs:?} of n:{n}");
    if n == 2 {
        let test = u16::from_be_bytes([bs[0], bs[1]]);
        println!("======> test ms:  {test}");
    }
    Ok((i,
    match n {
        1usize=>u8::from_be_bytes([bs[0]]) as i64,
        2usize=>u16::from_be_bytes([bs[0], bs[1]])  as i64,
        3usize=>i32::from_be_bytes([0, bs[0], bs[1], bs[2]])  as i64,
        4usize=>i32::from_be_bytes([bs[0], bs[1], bs[2], bs[3]])  as i64,
        5usize=>i64::from_be_bytes([0, 0, 0, bs[0], bs[1], bs[2], bs[3], bs[4]]),
        6usize=>i64::from_be_bytes([0, 0, bs[0], bs[1], bs[2], bs[3], bs[4], bs[5]]),
        7usize=>i64::from_be_bytes([0, bs[0], bs[1], bs[2], bs[3], bs[4], bs[5], bs[6]]),
        8usize=>i64::from_be_bytes([bs[0], bs[1], bs[2], bs[3], bs[4], bs[5], bs[6], bs[7]]),
        _=>0 as i64
    }))
}

pub fn take_utf8_end_of_null(i: &[u8])->IResult<&[u8], String, ParseError> {
    let (i, str_bytes) = complete::take_while(|b|{ b != b'\0' })(i)?;
    let (i, _) = complete::take::<usize, &[u8], ParseError>(1)(i)?;
    Ok((i, String::from_utf8(Vec::from(str_bytes)).unwrap_or_else(|e| "".to_string())))
}


pub fn take_bytes(i: &[u8], size:usize) -> IResult<&[u8], &[u8], ParseError> {
    let (i, bs) = complete::take::<usize, &[u8], ParseError>(size)(i)?;
    Ok((i, bs))
}

pub fn take_var_bytes(i: &[u8]) ->IResult<&[u8], &[u8], ParseError> {
    let (i, len) = VLenInt::decode(i)?;
    let (i, bs) = take_bytes(i, len.0 as usize)?;
    Ok((i, bs))
}

pub fn take_var_string(i: &[u8]) -> IResult<&[u8], String, ParseError> {
    let (i, bs) = take_var_bytes(i)?;
    let rs = String::from_utf8(Vec::from(bs));
    if let Err(ex) = rs {
        Err(err_maker(i, ErrorKind::Char))
    }else {
        Ok((i, rs.expect("no error")))
    }
}

pub fn take_eof_string(i: &[u8]) -> IResult<&[u8], String, ParseError> {
    // let rs = String::from_utf8(Vec::from(i));
    // if let Err(ex) = rs {
    //     println!("utf8 format error: {ex:?}");
    //     Err(err_maker(i, ErrorKind::Char))
    // }else {
    //     Ok((&[], rs.expect("no error")))
    // }
    Ok((&[], String::from_utf8_lossy(i).to_string()))
}

pub fn take_fix_string(i: &[u8], len:usize) -> IResult<&[u8], String, ParseError> {
    let (i, str_bytes) = take_bytes(i, len)?;
    Ok((i, String::from_utf8(Vec::from(str_bytes)).unwrap_or_else(|e| "".to_string())))
}

pub fn read_fps(i: &[u8], fps: u8) -> IResult<&[u8], u32, ParseError> {
    let read = match fps {
        1|2=>1,
        3|4=>2,
        5|6=>3,
        _=>0
    };
    if read>0 {
        println!(" =======> read: {read}  raw=>{i:?}");
        let (i, microsecond) = take_be_int(i, read)?;
        println!("read ms:{microsecond}");
        let microsecond = if microsecond > 0 {
            let microsecond = if fps % 2 > 0 {
                microsecond / 10i64
            } else { microsecond };
            microsecond * (10i64.pow((6u8 - fps) as u32))
        } else { 0i64 };
        Ok((i, microsecond as u32))
    }else{
        Ok((i, 0))
    }
}

pub fn write_var_bytes(buf:&mut BytesMut, input: impl AsRef<[u8]>) {
    let len = input.as_ref().len() as u64;
    let len = VLenInt::new(len);
    len.encode(buf);
    buf.extend_from_slice(input.as_ref());
}

pub fn write_null_term_str(buf:&mut BytesMut, s: &str) {
    write_null_term_bytes(buf, s.as_bytes());
}

pub fn write_null_term_bytes(buf:&mut BytesMut, bs: &[u8]) {
    buf.extend_from_slice(bs);
    buf.put_u8(b'\0');
}

pub fn write_var_str(buf:&mut BytesMut, s: &str) {
    write_var_bytes(buf, s);
}



#[derive(Debug, Clone)]
pub struct Header {
    pub len: u32,
    pub serial_id: u32
}

impl Decoder for Header {
    fn decode(input: &[u8]) -> IResult<&[u8], Self, ParseError> where Self: Sized{
        let (input, len) = take_int3(input)?;
        let (ip, serial_id) = take_int1(input)?;
        Ok((&[], Header{len, serial_id:serial_id as u32}))
    }
}

#[derive(Debug, Clone)]
pub struct Packet<P> {
    pub header: Header,
    pub payload: P,
}


pub fn encode_package<P: Encoder>( buf: &mut BytesMut, serial_id: u8, payload: &P) {
    buf.extend_from_slice(&[0, 0, 0]);
    buf.put_u8(serial_id);
    payload.encode(buf);
    let end = buf.len();
    let len = end - 4;
    println!("body len:{:?} {:?}", len, u32::to_le_bytes(len as u32));
    buf[0..3].copy_from_slice(&u32::to_le_bytes(len as u32)[0..3])
}


pub trait Decoder {
    fn decode(input: &[u8]) -> IResult<&[u8], Self, ParseError> where Self: Sized;
}

impl Decoder for Vec<u8> {
    fn decode(input: &[u8]) -> IResult<&[u8], Self, ParseError> where Self: Sized {
        Ok((&[], Vec::from(input)))
    }
}


pub trait Encoder {
    fn encode(&self, buffer: &mut BytesMut);
}

macro_rules! sha1 {
    ($($d:expr),*) => {{
        let mut hasher = Sha1::new();
        $(hasher.update($d);)*
        let i: [u8; 20] = hasher.finalize().into();
        i
    }};
}

pub fn native_password_auth(password: &[u8], auth_data: &[u8]) -> [u8; 20] {
    use sha1::{Digest, Sha1};
    let mut h1 = sha1!(password);
    //println!("h1: {:?}", h1);
    let h2 = sha1!(&h1);
    //println!("h2: {:?}", h2);
    let multi = sha1!(&auth_data[0..20], h2);
    //println!("multi: {:?}", multi);
    for i in 0..20 {
        h1[i] ^= multi[i];
    }
    //println!("auth salt: {:?}", auth_data);
    //println!("pass: {:?}", h1);
    h1
}