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
use crate::binlog::{DeleteRowEvent, EventRaw, TableMap, TableMapEvent, UpdateRowEvent, WriteRowEvent};

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
    fn from_dml(mut dml: DmlData, fields: &mut Vec<FieldMeta>) -> Self {
        let mut ins = Self::new(dml.id, dml.database, dml.table, dml.dml_type, dml.es);
        let mut pks: Vec<String> = Vec::new();

        for vals in dml.data.iter_mut(){
            let mut record:HashMap<String, Value> = HashMap::new();
            for (idx, val) in vals.iter().enumerate(){
                if let Some(meta) = fields.get_mut(idx){
                    ins.mysqlType.insert(meta.name.clone(), meta.field_type.clone());
                    let sql_tp = meta.get_sql_type();
                    ins.sqlType.insert(meta.name.clone(), sql_tp);
                    if sql_tp == 2005 {
                        let val_s = match val.as_array(){
                            Some(s)=>{String::from_utf8_lossy(s.iter().map(|n| n.as_u64().unwrap() as u8).collect::<Vec<u8>>().as_slice()).to_string()},
                            None=>String::from("")
                        };
                        record.insert(meta.name.clone(), Value::from(val_s));
                    }else{
                        if sql_tp == 2004 {
                            let val_s = match val.as_array(){
                                Some(s)=>{ String::from_utf16(s.iter().map(|n| n.as_u64().unwrap() as u16).collect::<Vec<u16>>().as_slice()).unwrap()},
                                None=>String::from("")
                            };
                            record.insert(meta.name.clone(), Value::from(val_s));
                        }else {
                            record.insert(meta.name.clone(), val.clone());
                        }
                    }

                    if meta.is_pk {
                        if !pks.contains(&meta.name){
                            pks.insert(0, meta.name.clone());
                        }
                    }
                }
            }
            ins.data.push(record);
        }
        let mut data_old: Vec<HashMap<String, Value>> = Vec::new();
        for vals in dml.old_data.iter_mut() {
            let mut record:HashMap<String, Value> = HashMap::new();
            for (idx, val) in vals.iter().enumerate() {
                if let Some(meta) = fields.get(idx){
                    record.insert(meta.name.clone(), val.clone().take());
                }
            }
            data_old.push(record)
        }
        if data_old.len() > 0usize{
            ins.old = Some(data_old);
        }
        if pks.len() > 0usize{
            ins.pkNames = Some(pks);
        }
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
            return 5;
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
        if self.field_type.eq("datetime") || self.field_type.eq("timestamp") {
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

    fn update_mapping(&mut self, conn: &mut MySQLConnection, tid: u32, db: String, table: String) -> Result<Vec<FieldMeta>, ()> {
        loop {
            if let Ok(mut mp) = self.mapping.lock() {
                if !mp.contains_key(&tid) {
                    let mut cols = Vec::new();
                    //println!("Check Mapping {tid} {db} {table} in {:?}", mp.contains_key(&tid));
                    if conn.desc_table(db.clone(), table.clone(), &mut cols){
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
            table_map.decode_columns(tablemap.header.table_id, tablemap.column_types, tablemap.column_metas.as_bytes());
            let mut current_data = DmlData::new_data(tablemap.header.table_id as u32, tablemap.schema_name.clone(), tablemap.table_name.clone());
            for instance in instances.iter_mut(){
                if let Some((mq_name, topic)) = instance.check_if_need_a_mq(current_data.database.clone(), current_data.table.clone()) {
                    ports.push((mq_name, topic));
                }
            }
            if ports.len() < 1 {
                //println!("未匹配到实例：{}.{}", &data.database, &data.table);
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
                    if let Ok(mut meta) = mapping.update_mapping(&mut conn, current_data.table_id, current_data.database.clone(), current_data.table.clone()) {
                        if meta.len() == 0usize {
                            println!("表{}.{} 不存在", current_data.database, current_data.table);
                            continue
                        }
                        let message = DmlMessage::from_dml(current_data, &mut meta);
                        if let Ok(json_message) = serde_json::to_string(&message) {
                            //println!("Canal JSON:\n{}", &json_message);
                            for (mq_name, topic) in ports {
                                let msg_qu = QueueMessage { topic, payload: json_message.clone(), pos };
                                queue.push(&mq_name, msg_qu);
                            }
                        }
                    }

                }
            }
        }
        //println!("[t:{thread_id}]DML Data: {data:?}");
    }
}