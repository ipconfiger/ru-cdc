use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::thread;
use serde_json::Value;
use serde::Serialize;
use std::time::{SystemTime, UNIX_EPOCH};
use rand::Rng;
use crate::config::{Config, Instance};
use crate::message_queue::{MessageQueues, QueueMessage};
use crate::mysql::MySQLConnection;

type OnDML = dyn Fn(String, QueueMessage) -> () + Send;


struct Counter(u64);

impl Counter{
    fn increase(&mut self) -> u64 {
        self.0+=1;
        self.0
    }
}

pub fn generate_random_number() -> u32 {
    let mut rng = rand::thread_rng();
    rng.gen_range(79..=113)
}

#[derive(Debug, Clone)]
pub struct DmlData {
    pub id: u64,
    pub table_id: u32,
    pub database: String,
    pub table: String,
    pub dml_type: String,
    pub es: u64,
    pub data: Vec<Value>,
    pub old_data: Vec<Value>
}

impl DmlData {
    const COUNTER: Counter = Counter(0);
    pub fn new_data(table_id: u32, database: String, table: String) -> Self {
        let new_id = Self::COUNTER.increase();
        Self{
            id: new_id,
            table_id,
            database,
            table,
            dml_type: "".to_string(),
            es: 0u64,
            data: Vec::new(),
            old_data: Vec::new(),
        }
    }
    pub fn append_data(&mut self, dml_type: String, es: u64, data: Vec<Value>, old_data: Vec<Value>) {
        self.dml_type = dml_type;
        self.es = es;
        self.data.extend_from_slice(data.as_slice());
        self.old_data.extend_from_slice(old_data.as_slice());
    }
}


#[derive(Debug, Clone, Serialize)]
pub struct DmlMessage {
    pub id: u64,
    pub database: String,
    pub table: String,
    pub pkNames: String,
    pub isDdl: bool,
    pub r#type: String,
    pub es: u64,
    pub ts: u64,
    pub sql: Option<String>,
    pub sqlType: Option<HashMap<String, Value>>,
    pub mysqlType: HashMap<String, String>,
    pub data: HashMap<String, Value>,
    pub old: HashMap<String, Value>
}

impl DmlMessage {
    fn from_dml(dml: DmlData, fields: &Vec<FieldMeta>) -> Self {
        let mut ins = Self::new(dml.id, dml.database, dml.table, dml.dml_type, dml.es);
        let mut pks: Vec<String> = Vec::new();
        for (idx, val) in dml.data.iter().enumerate(){
            if let Some(meta) = fields.get(idx){
                ins.mysqlType.insert(meta.name.clone(), meta.field_type.clone());
                ins.data.insert(meta.name.clone(), val.clone().take());
                if meta.is_pk {
                    pks.insert(0, meta.name.clone());
                }
            }
        }
        for (idx, val) in dml.old_data.iter().enumerate() {
            if let Some(meta) = fields.get(idx){
                ins.old.insert(meta.name.clone(), val.clone().take());
            }
        }
        ins.pkNames = pks.join(" ");
        ins
    }

    fn new(mid: u64, database: String, table: String, dml_type: String, es: u64) -> Self{
        let start = SystemTime::now();
        let since_the_epoch = start.duration_since(UNIX_EPOCH).expect("Time went backwards");
        let ts = since_the_epoch.as_secs();
        Self{
            id: mid,
            database,
            table,
            pkNames: "id".to_string(),
            isDdl: false,
            r#type: dml_type,
            es,
            ts,
            sql: None,
            sqlType: None,
            mysqlType: HashMap::new(),
            data: HashMap::new(),
            old: HashMap::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct FieldMeta {
    pub name: String,
    pub field_type: String,
    pub is_pk: bool
}

#[derive(Clone)]
struct TableMetaMapping {
    mapping: Arc<Mutex<HashMap<u32, Vec<FieldMeta>>>>
}

impl TableMetaMapping {
    fn new()->Self{
        Self{ mapping: Arc::new(Mutex::new(HashMap::new())) }
    }

    fn update_mapping(&mut self, conn: &mut MySQLConnection, tid: u32, db: String, table: String) -> Vec<FieldMeta> {
        loop {
            if let Ok(mut mp) = self.mapping.lock() {
                if !mp.contains_key(&tid) {
                    let mut cols = Vec::new();
                    conn.desc_table(db.clone(), table.clone(), &mut cols);
                    mp.insert(tid, cols.clone());
                    return cols.clone();
                }else{
                    if let Some(fm) = mp.get(&tid) {
                        return fm.clone();
                    }
                }
            }else{
                thread::sleep(std::time::Duration::from_millis(generate_random_number() as u64));
            }
        }
    }



}

#[derive(Debug, Clone)]
struct Pool {
    pub pool: Arc<Mutex<VecDeque<DmlData>>>
}

impl Pool {
    fn poll_data(&mut self) -> DmlData {
        loop {
            let data = if let Ok(mut queue) = self.pool.lock() {
                queue.pop_front()
            }else{None};
            if let Some(d) = data {
                return d;
            }else{
                thread::sleep(std::time::Duration::from_millis(generate_random_number() as u64));
            }
        }
    }

    pub fn push(&mut self, data: &DmlData) {
        loop {
            if let Ok(mut queue) = self.pool.lock() {
                queue.push_back(data.clone());
                return;
            }
        }
    }
}


pub struct Workers {
    pool:Pool
}

impl Workers {
    pub fn new()->Self{
        Self{ pool: Pool{ pool: Arc::new(Mutex::new(VecDeque::new())) } }
    }

    pub fn start(&mut self, size: usize, queue: MessageQueues, instances: Vec<Instance>, config: Config){
        let mut mapping = TableMetaMapping::new();
        for thread_id in 0..size {
            let mut the_pool = self.pool.clone();
            let mut the_mapping = mapping.clone();
            let the_queue = queue.clone();
            let the_ins = instances.clone();
            let the_config = config.clone();
            thread::spawn(move || {
                worker_body(thread_id, &mut the_pool, &mut the_mapping, the_queue, the_ins, the_config);
            });
        }
    }

    pub fn push(&mut self, data: &DmlData) {
        self.pool.push(data);
    }
}

fn worker_body(thread_id: usize, pool: &mut Pool, mapping: &mut TableMetaMapping, mut queue: MessageQueues, mut instances: Vec<Instance>, config: Config) {
    println!("[t:{thread_id}] Worker Started");
    let mut conn = MySQLConnection::get_connection(config.db_ip.as_str(), config.db_port as u32, config.max_packages as u32, config.user_name, config.passwd);
    loop {
        let data = pool.poll_data();
        println!("[t:{thread_id}]DML Data: {data:?}");

        let mut ports: Vec<(String, String)> = Vec::new();

        for instance in instances.iter_mut(){
            if let Some((mq_name, topic)) = instance.check_if_need_a_mq(data.database.clone(), data.table.clone()) {
                ports.push((mq_name, topic));
            }
        }
        if ports.len() < 1 {
            println!("未匹配到实例：{}.{}", &data.database, &data.table);
            continue;
        }

        let meta = mapping.update_mapping(&mut conn, data.table_id, data.database.clone(), data.table.clone());
        //println!("\n\n table fields meta:{meta:?}");
        let message = DmlMessage::from_dml(data, &meta);
        if let Ok(json_message) = serde_json::to_string(&message) {
            println!("Canal JSON:\n{}", &json_message);
            for (mq_name, topic) in ports {
                let msg_qu = QueueMessage{ topic, payload: json_message.clone() };
                queue.push(&mq_name, msg_qu);
            }

        }
    }
}