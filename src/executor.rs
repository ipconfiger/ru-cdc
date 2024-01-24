use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::thread;
use hex;
use serde_json::Value;
use serde::Serialize;
use std::time::{SystemTime, UNIX_EPOCH};
use rand::Rng;
use crate::config::{Config, Instance};
use crate::message_queue::{MessageQueues, QueueMessage};
use crate::mysql::{Decoder, MySQLConnection};
use std::sync::mpsc::{channel, Sender, Receiver};
use nom::AsBytes;
use crate::binlog::{ColMeta, DeleteRowEvent, EventRaw, TableMap, TableMapEvent, UpdateRowEvent, WriteRowEvent};

fn current_ms_ts() -> u128 {
    let now = SystemTime::now();
    let timestamp = now.duration_since(UNIX_EPOCH).expect("Time went backwards").as_millis();
    timestamp
}

pub fn current_ts() -> u64 {
    let now = SystemTime::now();
    let timestamp = now.duration_since(UNIX_EPOCH).expect("Time went backwards").as_secs();
    timestamp * 1000
}

pub fn generate_random_number() -> u32 {
    let mut rng = rand::thread_rng();
    rng.gen_range(1..=7)
}

#[derive(Debug, Clone)]
pub struct RowEvents {
    pub table_map: EventRaw,
    pub row_event: Option<EventRaw>,
    pub seq_idx: u64
}

impl RowEvents {
    pub fn new(table_map: EventRaw) -> Self {
        Self{ table_map, row_event: None, seq_idx:0 }
    }
    pub fn append(&mut self, ev: EventRaw, idx: u64) {
        self.row_event = Some(ev);
        self.seq_idx = idx;
    }
}



#[derive(Debug, Clone)]
pub struct DmlData {
    pub id: u64,
    pub table_id: u32,
    pub database: String,
    pub table: String,
    pub dml_type: String,
    pub es: u64,
    pub data: Vec<Vec<Value>>,
    pub old_data: Vec<Vec<Value>>,
    pub pos: u32
}

impl DmlData {
    pub fn new_data(table_id: u32, database: String, table: String) -> Self {
        Self{
            id: 0,
            table_id,
            database,
            table,
            dml_type: "".to_string(),
            es: 0u64,
            data: Vec::new(),
            old_data: Vec::new(),
            pos:0
        }
    }
    pub fn append_data(&mut self, idx: u64, dml_type: String, data: Vec<Vec<Value>>, old_data: Vec<Vec<Value>>, pos: u32) {
        self.id = idx;
        self.dml_type = dml_type;
        self.es = current_ts();
        self.data.extend_from_slice(data.as_slice());
        self.old_data.extend_from_slice(old_data.as_slice());
        self.pos = pos
    }
}


#[derive(Debug, Clone, Serialize)]
pub struct DmlMessage {
    pub id: u64,
    pub database: String,
    pub table: String,
    pub pkNames: Option<Vec<String>>,
    pub isDdl: bool,
    pub r#type: String,
    pub es: u64,
    pub ts: u128,
    pub sql: Option<String>,
    pub sqlType: HashMap<String, i16>,
    pub mysqlType: HashMap<String, String>,
    pub data: Vec<HashMap<String, Value>>,
    pub old: Option<Vec<HashMap<String, Value>>>
}

impl DmlMessage {
    fn format_val(val: &Value) -> String {
        if val.is_null() {
            "null".to_string()
        }else{
            if val.is_string(){
                format!("{}", val.to_string())
            }else{
                format!("\"{}\"", val.to_string())
            }
        }
    }

    fn format_json(&mut self,  fields: &mut Vec<FieldMeta>) -> String {
        let mut buffer:Vec<String> = Vec::new();
        buffer.push("{".to_string());
        buffer.push("\"data\":[".to_string());
        let data_count = &self.data.len();
        for (i, record) in self.data.iter_mut().enumerate(){
            buffer.push("{".to_string());
            let mut key_buffer:Vec<String> = Vec::new();
            for (idx, meta) in fields.iter().enumerate(){
                let val = record.get(&meta.name).unwrap();
                let data_value = format!("\"{}\":{}", &meta.name, Self::format_val(val));
                key_buffer.push(data_value);
            }
            let dict_inner = key_buffer.join(",");
            buffer.push(dict_inner);
            if i != (*data_count - 1usize){
                buffer.push("},".to_string());
            }else{
                buffer.push("}".to_string());
            }
        }
        buffer.push("],".to_string());
        buffer.push(format!("\"database\":\"{}\",", &self.database));
        buffer.push(format!("\"es\":{},", self.es));
        buffer.push(format!("\"id\":{},", self.id));
        buffer.push("\"isDdl\": false,".to_string());
        buffer.push("\"mysqlType\":{".to_string());
        for (idx, meta) in fields.iter().enumerate(){
            let tp = self.mysqlType.get(&meta.name).unwrap();
            if idx != (fields.len() - 1usize){
                buffer.push(format!("\"{}\":\"{}\",", meta.name, tp));
            }else{
                buffer.push(format!("\"{}\":\"{}\"", meta.name, tp));
            }
        }
        buffer.push("},".to_string());
        if self.old.is_some(){
            buffer.push("\"old\":[".to_string());
            let old_count = self.old.clone().unwrap().len();
            for (i, record) in self.old.clone().unwrap().iter().enumerate(){
                buffer.push("{".to_string());
                let mut key_buffer:Vec<String> = Vec::new();
                for (idx, meta) in fields.iter().enumerate(){
                    let val = &record.get::<String>(&meta.name);
                    if let Some(val_ok) = val {
                        let val_str = Self::format_val(val_ok);
                        key_buffer.push(format!("\"{}\":{}", meta.name, val_str));
                    }
                }
                let inner_str = key_buffer.join(",");
                buffer.push(inner_str);
                if i != (old_count - 1usize){
                    buffer.push("},".to_string());
                }else{
                    buffer.push("}".to_string());
                }
            }
            buffer.push("],".to_string());
        }
        if self.pkNames.is_none() {
            buffer.push("\"pkNames\":null,".to_string());
        }else{
            buffer.push("\"pkNames\":[".to_string());
            for (idx, name) in self.pkNames.clone().unwrap().iter().enumerate() {
                if idx != (self.pkNames.clone().unwrap().len() - 1usize){
                    buffer.push(format!("\"{name}\","));
                }else{
                    buffer.push(format!("\"{name}\""));
                }
            }
            buffer.push("],".to_string())
        }
        buffer.push("\"sql\":\"\",".to_string());
        buffer.push("\"sqlType\":{".to_string());
        for (idx, meta) in fields.iter().enumerate(){
            let sqlType = self.sqlType.get::<String>(&meta.name).unwrap();
            if idx!= (fields.len() - 1usize) {
                buffer.push(format!("\"{}\":{},", meta.name, sqlType));
            }else{
                buffer.push(format!("\"{}\":{}", meta.name, sqlType));
            }
        }
        buffer.push("},".to_string());
        buffer.push(format!("\"table\":\"{}\",", self.table));
        buffer.push(format!("\"ts\":{},", self.ts));
        buffer.push(format!("\"type\":\"{}\"", &self.r#type));
        buffer.push("}".to_string());
        buffer.join("")
    }

    fn text_field_data(val: &Value) -> String{
        match val.as_array(){
            Some(s)=>{String::from_utf8_lossy(s.iter().map(|n| n.as_u64().unwrap() as u8).collect::<Vec<u8>>().as_slice()).to_string()},
            None=>String::from("")
        }
    }

    fn blob_field_data(val: &Value) -> String {
        match val.as_array(){
            Some(s)=>{ String::from_utf16(s.iter().map(|n| n.as_u64().unwrap() as u16).collect::<Vec<u16>>().as_slice()).unwrap()},
            None=>String::from("")
        }
    }

    fn from_dml(mut dml: DmlData, fields: &mut Vec<FieldMeta>) -> Self {
        let mut ins = Self::new(dml.id, dml.database, dml.table, dml.dml_type, dml.es);
        let mut pks: Vec<String> = Vec::new();
        let record_count = dml.data.len();
        let mut old_record_vec: Vec<HashMap<String, Value>> = Vec::new();
        for record_id in 0..record_count{
            let old_vals = dml.old_data.get(record_id);
            let hash_old_val = old_vals.is_none();
            let new_vals = dml.data.get(record_id).unwrap();

            let mut record_data:HashMap<String, Value> = HashMap::new();
            let mut record_old:HashMap<String, Value> = HashMap::new();

            for (idx, file_meta) in fields.iter_mut().enumerate(){
                let sql_tp = file_meta.get_sql_type();
                if record_id == 0 {
                    ins.mysqlType.insert(file_meta.name.clone(), file_meta.field_type.clone());
                    ins.sqlType.insert(file_meta.name.clone(), sql_tp);
                    if file_meta.is_pk {
                        if !pks.contains(&file_meta.name){
                            pks.insert(0, file_meta.name.clone());
                        }
                    }
                }
                let data_val = new_vals.get(idx).unwrap();
                let is_same = match old_vals {
                    Some(old_values)=>{
                        let ov = old_values.get(idx).unwrap();
                        ov.eq(data_val)
                    },
                    None=>true
                };

                if sql_tp == 2005 {
                    let val_s = Self::text_field_data(data_val);
                    record_data.insert(file_meta.name.clone(), Value::from(val_s));
                    if !is_same{
                        let old_val = old_vals.unwrap().get(idx).unwrap();
                        let old_val_s = Self::text_field_data(old_val);
                        record_old.insert(file_meta.name.clone(), Value::from(old_val_s));
                    }
                }else{
                    if sql_tp == 2004 {
                        let val_s = Self::blob_field_data(data_val);
                        record_data.insert(file_meta.name.clone(), Value::from(val_s));
                        if !is_same {
                            let old_val = old_vals.unwrap().get(idx).unwrap();
                            let old_val_s = Self::blob_field_data(old_val);
                            record_data.insert(file_meta.name.clone(), Value::from(old_val_s));
                        }
                    }else{
                        record_data.insert(file_meta.name.clone(), data_val.clone());
                        if !is_same {
                            let old_val = old_vals.unwrap().get(idx).unwrap();
                            record_old.insert(file_meta.name.clone(), old_val.clone());
                        }
                    }
                }
            }
            ins.data.push(record_data);
            old_record_vec.push(record_old);
        }
        if pks.len() > 0{
            ins.pkNames = Some(pks);
        }
        ins.old = Some(old_record_vec);
        ins
    }

    fn new(mid: u64, database: String, table: String, dml_type: String, es: u64) -> Self{
        let ts = current_ms_ts();
        Self{
            id: mid,
            database,
            table,
            pkNames: None,
            isDdl: false,
            r#type: dml_type,
            es,
            ts,
            sql: None,
            sqlType: HashMap::new(),
            mysqlType: HashMap::new(),
            data: Vec::new(),
            old: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct FieldMeta {
    pub name: String,
    pub field_type: String,
    pub is_pk: bool
}

impl FieldMeta{
    pub fn get_sql_type(&mut self) -> i16 {
        if self.field_type.starts_with("tinyint") {
            return -6;
        }
        if self.field_type.starts_with("smallint") {
            return 5;
        }
        if self.field_type.starts_with("mediumint") || self.field_type.starts_with("int") {
            return 4
        }
        if self.field_type.starts_with("bigint") {
            return -5;
        }
        if self.field_type.starts_with("float") {
            return 7;
        }
        if self.field_type.starts_with("double") {
            return 8;
        }
        if self.field_type.starts_with("decimal") {
            return 3;
        }
        if self.field_type.eq("date") {
            return 91;
        }
        if self.field_type.eq("time") {
            return 92;
        }
        if self.field_type.starts_with("year") {
            return 12;
        }
        if self.field_type.starts_with("datetime") || self.field_type.starts_with("timestamp") {
            return 93
        }
        if self.field_type.starts_with("char") {
            return 1;
        }
        if self.field_type.starts_with("varchar") {
            return 12;
        }
        if self.field_type.ends_with("blob") {
            return 2004;
        }
        if self.field_type.ends_with("text") {
            return 2005;
        }
        println!("invalid:{:?}", self);
        -999
    }
}


#[derive(Clone)]
struct TableMetaMapping {
    mapping: Arc<Mutex<HashMap<u32, Vec<FieldMeta>>>>
}

impl TableMetaMapping {
    fn new()->Self{
        Self{ mapping: Arc::new(Mutex::new(HashMap::new())) }
    }

    fn update_mapping(&mut self, conn: &mut MySQLConnection, tid: u32, db: String, table: String, table_map: &Vec<ColMeta>) -> Result<Vec<FieldMeta>, ()> {
        loop {
            if let Ok(mut mp) = self.mapping.lock() {
                if !mp.contains_key(&tid) {
                    let mut cols = Vec::new();
                    //println!("Check Mapping {tid} {db} {table} in {:?}", mp.contains_key(&tid));
                    if conn.desc_table(db.clone(), table.clone(), &mut cols, table_map){
                        mp.insert(tid, cols.clone());
                        return Ok(cols.clone());
                    }else{
                        mp.insert(tid, Vec::new());
                        return Ok(Vec::new())
                    }
                }else{
                    if let Some(fm) = mp.get(&tid) {
                        return Ok(fm.clone());
                    }else{
                        return Err(());
                    }
                }
            }else{
                thread::sleep(std::time::Duration::from_micros(generate_random_number() as u64));
            }
        }
    }



}

#[derive(Debug)]
struct Pool {
    pub tx_channel : HashMap<u32, Arc<Mutex<Sender<RowEvents>>>>
}

impl Pool {
    pub fn regist_tx(&mut self, key: u32, tx: Arc<Mutex<Sender<RowEvents>>>) {
        self.tx_channel.insert(key, tx);
    }

    pub fn push(&mut self, data: &RowEvents) {
        let i = ((data.seq_idx + 1) % self.tx_channel.len() as u64) as u32;
        if let Some(tx_ref) = self.tx_channel.get_mut(&i) {
            if let Ok(tx) = tx_ref.lock(){
                tx.send(data.clone()).expect("send error");
            }else{
                println!("==========>(夭寿啦，获取锁失败了)");
            }
        }
    }
}


pub struct Workers {
    pool:Pool
}

impl Workers {
    pub fn new()->Self{
        Self{ pool: Pool{ tx_channel: HashMap::new() } }
    }


    pub fn start(&mut self, size: usize, queue: MessageQueues, instances: Vec<Instance>, config: Config){

        let mut mapping = TableMetaMapping::new();
        for thread_id in 0..size {
            let (tx, rx) = channel::<RowEvents>();
            let tx = Arc::new(Mutex::new(tx));
            self.pool.regist_tx(thread_id as u32, tx);
            let mut the_mapping = mapping.clone();
            let the_queue = queue.clone();
            let the_ins = instances.clone();
            let the_config = config.clone();
            thread::spawn(move || {
                worker_body(thread_id, rx, &mut the_mapping, the_queue, the_ins, the_config);
            });

        }
    }

    pub fn push(&mut self, data: &RowEvents) {
        self.pool.push(data);
    }
}

fn worker_body(thread_id: usize, rx: Receiver<RowEvents>, mapping: &mut TableMetaMapping, mut queue: MessageQueues, mut instances: Vec<Instance>, config: Config) {
    println!("[t:{thread_id}] Worker Started");
    let mut table_map = TableMap::new();
    let mut conn = MySQLConnection::get_connection(config.db_ip.as_str(), config.db_port as u32, config.max_packages as u32, config.user_name, config.passwd);
    loop {
        if let Ok(data) = rx.recv() {
            let mut ports: Vec<(String, String)> = Vec::new();
            let (_, tablemap) = TableMapEvent::decode(data.table_map.payload.as_slice()).expect("table map error");
            let tm = tablemap.clone();
            table_map.decode_columns(tm.header.table_id, tm.column_types, tm.column_metas.as_bytes());
            let mut current_data = DmlData::new_data(tablemap.header.table_id as u32, tablemap.schema_name.clone(), tablemap.table_name.clone());
            for instance in instances.iter_mut(){
                if let Some((mq_name, topic)) = instance.check_if_need_a_mq(current_data.database.clone(), current_data.table.clone()) {
                    ports.push((mq_name, topic));
                }
            }
            if ports.len() < 1 {
                //println!("未匹配到实例：{}.{}", &current_data.database, &current_data.table);
                continue;
            }
            if let Some(ev) = data.row_event {
                let pos = ev.header.log_pos;
                if ev.header.event_type == 30 {
                    let (i, event) = WriteRowEvent::decode(ev.payload.as_bytes()).unwrap();
                    let (_, rows) = WriteRowEvent::decode_column_multirow_vals(&table_map, i, event.header.table_id, event.col_map_len).expect("解码数据错误");
                    current_data.append_data(data.seq_idx, "INSERT".to_string(), rows, Vec::new(), ev.header.log_pos);
                }
                if ev.header.event_type == 31{
                    let (i, event) = UpdateRowEvent::decode(ev.payload.as_bytes()).unwrap();
                    let (_, (old_val, new_val)) = match UpdateRowEvent::fetch_rows(i, table_map.clone(), event.header.table_id, event.col_map_len){
                        Ok((i, (old_values, new_values)))=> (i, (old_values, new_values)),
                        Err(err)=>{
                            println!("exec table:{} fail with: {err:?}", current_data.table);
                            panic!("{err:?}")
                        }
                    };
                    current_data.append_data(data.seq_idx, "UPDATE".to_string(), new_val, old_val, ev.header.log_pos);
                }
                if ev.header.event_type == 32{
                    let (i, event) = DeleteRowEvent::decode(ev.payload.as_bytes()).unwrap();
                    let (_, old_values) = DeleteRowEvent::fetch_rows(i, table_map.clone(), event.header.table_id, event.col_map_len).expect("解码 Delete Val错误");
                    current_data.append_data(data.seq_idx, "DELETE".to_string(), Vec::new(), old_values, ev.header.log_pos);
                }
                if vec![32u8, 31u8, 30u8].contains(&ev.header.event_type) {
                    let tm = tablemap.clone();
                    if let Ok(mut meta) = mapping.update_mapping(&mut conn,
                                                                 current_data.table_id,
                                                                 current_data.database.clone(),
                                                                 current_data.table.clone(),
                                                                 &table_map.metas[&tm.header.table_id]

                    ) {
                        if meta.len() == 0usize {
                            println!("表{}.{} 不存在", current_data.database, current_data.table);
                            continue
                        }
                        let mut message = DmlMessage::from_dml(current_data, &mut meta);
                        let json_str = message.format_json(&mut meta);
                        if ports.len() > 0 {
                            for (mq_name, topic) in ports {
                                let msg_qu = QueueMessage { topic, payload: json_str.clone(), pos };
                                queue.push(&mq_name, msg_qu);
                            }
                        }else{
                            println!("没有可用发送端口");
                        }
                    }

                }
            }
        }
        //println!("[t:{thread_id}]DML Data: {data:?}");
    }
}