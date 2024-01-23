use std::collections::HashMap;
use bytes::{BufMut, BytesMut};
use nom::{IResult, AsBytes};
use nom::error::{Error, ErrorKind, VerboseError, VerboseErrorKind};
use nom::Err as NomErr;
use crate::mysql::{Decoder, take_int1, take_int2, take_int4, take_bytes, take_utf8_end_of_null, Encoder, take_int3, write_var_bytes, write_null_term_str, write_var_str, take_var_string, take_var_bytes};


#[derive(Debug, Clone)]
pub struct HandshakeV10 {
    pub protocol_version: u32,     //int1
    pub server_version: String,
    pub thread_id: u32,            //int4
    pub caps: Capabilities,
    pub charset: u32,             //int1
    pub status: u32,              //int2
    pub auth_plugin_name: String,
    pub auth_plugin_data: BytesMut,
}

impl Decoder for HandshakeV10 {
    fn decode(input: &[u8]) -> IResult<&[u8], Self> where Self: Sized {
        let (i, protocol_version) = take_int1(input)?;
        if protocol_version != 10 {
            return Err(NomErr::Error(Error::new("".as_ref(), ErrorKind::Not)));
        }
        //println!("server_version: {:?}\n", i.to_vec());
        let (i, server_version) = take_utf8_end_of_null(i)?;
        //println!("thread_id: {:?}\n", i.to_vec());
        let (i, thread_id) = take_int4(i)?;
        //println!("auth_plugin_data_bytes: {:?}\n", i.to_vec());
        let (i, auth_plugin_data_bytes) = take_bytes(i, 8)?;
        let (i, _) = take_bytes(i, 1)?;
        let mut auth_plugin_data = BytesMut::from_iter(auth_plugin_data_bytes);

        let (i, l_cap) = take_bytes(i, 2usize)?;
        let (i, charset) = take_int1(i)?;
        let (i, status) = take_int2(i)?;
        let (i, h_cap) = take_bytes(i, 2usize)?;
        //println!("auth_data_len: {:?}\n", i.to_vec());
        let (i, auth_data_len) = take_int1(i)?;
        //println!("take_bytes 10: {:?}\n", i.to_vec());
        let (i, _) = take_bytes(i,10usize)?;
        let i = if auth_data_len > 0{
            let len = 13.max(auth_data_len - 8) as usize;
            //println!("auth_data_len大于0: {}  {}", auth_data_len, len);
            let (ip, bs) = take_bytes(i, len)?;
            auth_plugin_data.extend_from_slice(bs);
            ip
        }else{
            i
        };
        let mut caps_num = [0u8; 4];
        caps_num[..2].copy_from_slice(l_cap);
        caps_num[2..].copy_from_slice(h_cap);
        let caps = Capabilities::from_bits(u32::from_le_bytes(caps_num)).unwrap();
        let auth_plugin_name = if caps.contains(Capabilities::CLIENT_PLUGIN_AUTH) {
            let (i, name) = take_utf8_end_of_null(i)?;
            name
        } else {
            Default::default()
        };


        Ok((i, Self {
            protocol_version:protocol_version as u32,
            server_version,
            thread_id,
            caps,
            charset:charset as u32,
            status:status as u32,
            auth_plugin_name,
            auth_plugin_data,
        }))
    }
}


#[derive(Debug, Clone)]
pub struct HandshakeResponse41 {
    pub caps: Capabilities,
    pub max_packet_size: u32, //int4
    pub charset: u8,         //int1
    pub user_name: String,
    pub auth_resp: BytesMut,
    pub database: Option<String>,
    pub plugin_name: Option<String>,
    pub connect_attrs: HashMap<String, String>,
    pub zstd_level: u32,       //int1
}

impl Encoder for HandshakeResponse41 {
    fn encode(&self, buffer: &mut BytesMut) {
        buffer.extend_from_slice(&self.caps.bits().to_le_bytes());
        buffer.extend_from_slice(&self.max_packet_size.to_le_bytes());
        buffer.extend_from_slice(&[self.charset]);
        buffer.extend_from_slice(&vec![0].repeat(23));
        buffer.extend_from_slice(self.user_name.as_bytes());
        buffer.put_u8(b'\0');
        if self
            .caps
            .contains(Capabilities::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA)
        {
            write_var_bytes(buffer, &self.auth_resp)
        }else{
            let len = self.auth_resp.len() as u8;
            buffer.put_u8(len);
            buffer.extend_from_slice(self.auth_resp.as_bytes());
        }
        if self.caps.contains(Capabilities::CLIENT_CONNECT_WITH_DB) {
            write_null_term_str(buffer, self.database.as_deref().unwrap_or("default"));
        }
        if self.caps.contains(Capabilities::CLIENT_PLUGIN_AUTH) {
            write_null_term_str(buffer, self.plugin_name.as_deref().unwrap_or_default());
        }
        if self.caps.contains(Capabilities::CLIENT_CONNECT_ATTRS) {
            let len = VLenInt::new(self.connect_attrs.len() as u64);
            len.encode(buffer);
            for (k, v) in self.connect_attrs.iter() {
                write_var_str(buffer, k.as_str());
                write_var_str(buffer, v.as_str());
            }
        }
        buffer.put_u8(self.zstd_level as u8);
    }
}

#[derive(Debug, Clone)]
pub struct AuthSwitchReq {
    pub plugin_name: String,
    pub plugin_data: BytesMut,
}

impl AuthSwitchReq {
    pub const STATUS: u8 = 254;
}

impl Decoder for AuthSwitchReq {
    fn decode(input: &[u8]) -> IResult<&[u8], Self> where Self: Sized {
        let (i, tag) =take_int1(input)?;
        //println!("auth switch: {:?}", input);
        if tag != 0xfe {
            //let (_, err_pack) = ErrPacket::decode(input).expect("bhbhbhbh");
            let (_, ok_pack) = OkPacket::decode(input).expect("decode ok error");
            println!("err pack:{:?}", ok_pack);
            return Err(NomErr::Error(Error::new("".as_ref(), ErrorKind::Fail)));
        }
        let (i, plugin_name) = take_utf8_end_of_null(i)?;
        let plugin_data = if i.len()>0 {
            BytesMut::from_iter(i)
        }else{
            BytesMut::new()
        };
        Ok((i, AuthSwitchReq {
            plugin_name,
            plugin_data,
        }))
    }
}

#[derive(Debug, Clone)]
pub struct AuthSwitchResp {
    pub data: BytesMut,
}

impl Encoder for AuthSwitchResp {
    fn encode(&self, buffer: &mut BytesMut) {
        buffer.extend_from_slice(self.data.as_bytes());
    }
}

#[derive(Debug, Clone)]
pub struct ComQuery {
    pub query: String,
}

impl Encoder for ComQuery {
    fn encode(&self, buf: &mut BytesMut) {
        //buf.extend_from_slice(u32::to_le_bytes(1 + self.query.len() as u32).as_bytes());
        buf.put_u8(0x03);
        buf.extend_from_slice(self.query.as_bytes());
        //buf.put_u8(b'\0');
    }
}

impl<T: ToString> From<T> for ComQuery {
    fn from(value: T) -> Self {
        Self {
            query: value.to_string(),
        }
    }
}


#[derive(Debug, Clone)]
pub struct TextResultSet {
    pub column_count: VLenInt,
    pub col_defs: Vec<ColDef>,
    pub rows: Vec<TextResult>
}

#[derive(Debug, Clone)]
pub struct TextResult {
    pub columns: Vec<Vec<u8>>,
}
impl Decoder for TextResult {
    fn decode(input: &[u8]) -> IResult<&[u8], Self> where Self: Sized {
        let mut columns = vec![];
        let mut i = input;
        while i.len() > 0 {
            if i[0] != 0x00 {
                if i[0] == 0xfb {
                    i = &i[1..];
                    columns.push(Vec::from("NULL".as_bytes()));
                }else {
                    let (ip, col) = take_var_bytes(i)?;
                    columns.push(Vec::from(col));
                    i = ip;
                }
            }else{
                i = &i[1..];
                columns.push(Vec::from([]));
            }
        }
        Ok((i, Self { columns }))
    }
}

#[derive(Debug, Clone)]
pub struct ColDef {
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub original_table: String,
    pub name: String,
    pub original_name: String,
    pub length_of_fixed_length_fields: VLenInt,
    pub charset: u16,
    pub column_length: u32,
    pub ty: u8,
    pub flags: u16,
    pub decimals: u8,
}

impl Decoder for ColDef {
    fn decode(input: &[u8]) -> IResult<&[u8], Self> where Self: Sized {
        let (i, catalog) = take_var_string(input)?;
        let (i, schema) = take_var_string(i)?;
        let (i, table) = take_var_string(i)?;
        let (i, original_table) = take_var_string(i)?;
        let (i, name) = take_var_string(i)?;
        let (i, original_name) = take_var_string(i)?;
        let (i, length_of_fixed_length_fields) = VLenInt::decode(i)?;
        let (i, charset) = take_int2(i)?;
        let (i, column_length) = take_int4(i)?;
        let (i, ty) = take_int1(i)?;
        let (i, flags) = take_int2(i)?;
        let (i, decimals) = take_int1(i)?;
        Ok((i, Self {
            catalog,
            schema,
            table,
            original_table,
            name,
            original_name,
            length_of_fixed_length_fields,
            charset: charset as u16,
            column_length,
            ty: ty as u8,
            flags: flags as u16,
            decimals: decimals as u8,
        }))
    }
}


#[derive(Debug, Clone)]
pub struct ComBinLogDump {
    pub pos: u32,
    pub flags: u16,
    pub server_id: u32,
    pub filename: String,
}

impl Encoder for ComBinLogDump {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(0x12);
        buf.extend_from_slice(u32::to_le_bytes(self.pos).as_bytes());
        buf.extend_from_slice(u16::to_le_bytes(self.flags).as_bytes());
        buf.extend_from_slice(u32::to_le_bytes(self.server_id).as_bytes());
        buf.extend_from_slice(self.filename.as_bytes());
    }
}


#[derive(Debug, Clone)]
pub struct OkPacket {
    pub header: u8,
    pub affected_rows: VLenInt,
    pub last_insert_id: VLenInt,
    pub status_flags: u16,
    pub warnings: u16,
    pub info: String,
}

impl OkPacket {
    pub fn is_ok(&self) -> bool {
        self.header == 0x00
    }

    pub fn is_eof(&self) -> bool {
        self.header == 0xfe
    }
}

impl Decoder for OkPacket {
    fn decode(input: &[u8]) -> IResult<&[u8], Self> where Self: Sized {
        let (i, header) = take_int1(input)?;
        if header == 0xff {
            let (_, err_pack) = ErrPacket::decode(input).expect("Error Pack");
            println!("err pack:{:?}", err_pack);
            return Err(NomErr::Error(Error::new(input, ErrorKind::Fail)));
        }
        let (i, affected_rows) = VLenInt::decode(i)?;
        let (i, last_insert_id) = VLenInt::decode(i)?;
        let (i, status_flags) = take_int2(i)?;
        let (i, warnings) = take_int2(i)?;
        let info = String::new();
        Ok((&[], Self {
            header: header as u8,
            affected_rows,
            last_insert_id,
            status_flags: status_flags as u16,
            warnings: warnings as u16,
            info,
        }))

    }
}

#[derive(Debug, Clone)]
pub struct ErrPacket {
    pub header: u8,
    pub code: u16,
    pub sql_state_marker: u8,
    pub sql_state: String,
    pub error_msg: String,
}

impl Decoder for ErrPacket {
    fn decode(input: &[u8]) -> IResult<&[u8], Self> where Self: Sized {
        let (i, header) = take_int1(input)?;
        let (i, code) = take_int2(i)?;
        let (i, marker) = take_int1(i)?;
        let (i, state) = take_bytes(i,5usize)?;
        let sql_state = String::from_utf8(Vec::from(state)).expect("err state");
        let error_msg = String::from_utf8(Vec::from(i)).expect("err msg");
        Ok((i, Self {
            header: header as u8,
            code: code as u16,
            sql_state,
            sql_state_marker:marker as u8,
            error_msg,
        }))
    }
}



bitflags::bitflags! {
    /// [doc](https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__capabilities__flags.html#ga07344a4eb8f5c74ea8875bb4e9852fb0)
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub struct Capabilities: u32 {
        /// Use the improved version of Old Password Authentication. More...
        const  	CLIENT_LONG_PASSWORD =  1;

        /// Send found rows instead of affected rows in EOF_Packet. More...
        const  	CLIENT_FOUND_ROWS =  2;

        /// Get all column flags. More...
        const  	CLIENT_LONG_FLAG =  4;

        /// Database (schema) name can be specified on connect in Handshake Response Packet. More...
        const  	CLIENT_CONNECT_WITH_DB =  8;

        /// DEPRECATED: Don't allow database.table.column. More...
        const  	CLIENT_NO_SCHEMA =   16;

        /// Compression protocol supported. More...
        const  	CLIENT_COMPRESS =  32;

        /// Special handling of ODBC behavior. More...
        const  	CLIENT_ODBC =  64;

        /// Can use LOAD DATA LOCAL. More...
        const  	CLIENT_LOCAL_FILES =  128;

        /// Ignore spaces before '('. More...
        const  	CLIENT_IGNORE_SPACE =  256;

        /// New 4.1 protocol. More...
        const  	CLIENT_PROTOCOL_41 =  512;

        /// This is an interactive client. More...
        const  	CLIENT_INTERACTIVE =  1024;

        /// Use SSL encryption for the session. More...
        const  	CLIENT_SSL =  2048;

        /// Client only flag. More...
        const  	CLIENT_IGNORE_SIGPIPE =  4096;

        /// Client knows about transactions. More...
        const  	CLIENT_TRANSACTIONS =  8192;

        /// DEPRECATED: Old flag for 4.1 protocol
        const  	CLIENT_RESERVED =  16384;

        /// DEPRECATED: Old flag for 4.1 authentication \ CLIENT_SECURE_CONNECTION. More...
        const  	CLIENT_RESERVED2 =   32768;

        /// Enable/disable multi-stmt support. More...
        const  	CLIENT_MULTI_STATEMENTS =  (1u32 << 16);

        /// Enable/disable multi-results. More...
        const  	CLIENT_MULTI_RESULTS =  (1u32 << 17);

        /// Multi-results and OUT parameters in PS-protocol. More...
        const  	CLIENT_PS_MULTI_RESULTS =  (1u32 << 18);

        /// Client supports plugin authentication. More...
        const  	CLIENT_PLUGIN_AUTH =  (1u32 << 19);

        /// Client supports connection attributes. More...
        const  	CLIENT_CONNECT_ATTRS =  (1u32 << 20);

        /// Enable authentication response packet to be larger than 255 bytes. More...
        const  	CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA =  (1u32 << 21);

        /// Don't close the connection for a user account with expired password. More...
        const  	CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS =  (1u32 << 22);

        /// Capable of handling server state change information. More...
        const  	CLIENT_SESSION_TRACK =  (1u32 << 23);

        /// Client no longer needs EOF_Packet and will use OK_Packet instead. More...
        const  	CLIENT_DEPRECATE_EOF =  (1u32 << 24);

        /// The client can handle optional metadata information in the resultset. More...
        const  	CLIENT_OPTIONAL_RESULTSET_METADATA =  (1u32 << 25);

        /// Compression protocol extended to support zstd compression method. More...
        const  	CLIENT_ZSTD_COMPRESSION_ALGORITHM =  (1u32 << 26);

        /// Support optional extension for query parameters into the COM_QUERY and COM_STMT_EXECUTE packets. More...
        const  	CLIENT_QUERY_ATTRIBUTES =  (1u32 << 27);

        /// Support Multi factor authentication. More...
        const  	MULTI_FACTOR_AUTHENTICATION =  (1u32 << 28);

        /// This flag will be reserved to extend the 32bit capabilities structure to 64bits. More...
        const  	CLIENT_CAPABILITY_EXTENSION =  (1u32 << 29);

        /// Verify server certificate. More...
        const  	CLIENT_SSL_VERIFY_SERVER_CERT =  (1u32 << 30);

        /// Don't reset the options after an unsuccessful connect. More...
        const  	CLIENT_REMEMBER_OPTIONS =  (1u32 << 31);
    }
}

#[derive(Default, Debug, Clone, Copy)]
pub struct VLenInt(pub u64);

impl VLenInt {
    pub fn new(val: u64) -> Self {
        Self(val)
    }

    pub fn int(&self) -> u64 {
        self.0
    }

    pub fn bytes(&self) -> BytesMut {
        let mut data = BytesMut::new();
        self.encode(&mut data);
        data
    }
}
impl Decoder for VLenInt {
    fn decode(input: &[u8]) -> IResult<&[u8], Self> where Self: Sized {
        match input[0] {
            val @ 0..=0xfb => Ok((&input[1..], Self(input[0] as u64))),
            0xfc => {
                let (i, val) = take_int2(input)?;
                Ok((&[], Self(val as u64)))
            }
            0xfd => {
                let (i, val) = take_int3(input)?;
                Ok((&[], Self(val as u64)))
            }
            0xfe => {
                let (i, val) = take_int4(input)?;
                Ok((&[], Self(val as u64)))
            },
            0xff => Err(NomErr::Error(Error::new("".as_ref(), ErrorKind::Fail))),
        }
    }
}

impl Encoder for VLenInt {
    fn encode(&self, buf: &mut BytesMut) {
        match self.0 {
            0..=250 => buf.put_u8(self.0 as u8),
            251..=65535 => {
                buf.put_u8(0xfc);
                buf.extend_from_slice(&(self.0 as u16).to_le_bytes());
            }
            65536..=16777215 => {
                buf.put_u8(0xfd);
                buf.extend_from_slice(&(self.0 as u32).to_le_bytes()[..2]);
            }
            16777216.. => {
                buf.put_u8(0xfe);
                buf.extend_from_slice(&self.0.to_le_bytes());
            }
        }
    }
}