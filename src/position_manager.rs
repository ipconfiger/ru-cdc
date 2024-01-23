use std::sync::{Arc, Mutex};
use std::{fs, thread};
use std::path::Path;
use std::sync::mpsc::{channel, Sender};
use serde::{Deserialize, Serialize};
use crate::config::get_abs_path;
use crate::executor::current_ts;
use crate::protocal::{TextResult, TextResultSet};

#[derive(Serialize, Deserialize)]
pub struct PositionSet {
    pub binlog: String,
    pub position: u32,
    pub saving_ts: u64
}

pub struct PositionMng {
    pub binlog: String,
    pub position: u32,
    pub loaded: bool,
    pub tx: Sender<PositionSet>,
}

impl PositionMng {
    fn new(tx: Sender<PositionSet>)->Self {
        Self{ binlog: "".to_string(), position: 0, loaded: false, tx }
    }

    pub fn thread_safe_new() -> Arc<Mutex<Self>> {
        let (tx, rx) = channel::<PositionSet>();
        let ins = Arc::new(Mutex::new(Self::new(tx)));
        thread::spawn(move || {
            loop {
                if let Ok(pos) = rx.recv(){
                    if let Ok(json_str) = serde_json::to_string(&pos){
                        write_file_content(get_abs_path("~/.ru_cdc/meta.json".to_string()), json_str.as_bytes()).expect("保存状态错误");
                    }
                }
            }
        });
        ins
    }

    fn update(&mut self, binlog: &String, position: u32) {
        self.binlog = binlog.to_string();
        self.position = position;
        self.loaded = true;
    }

    fn new_pos(&mut self, position: u32) {
        self.position = position;
    }
}

pub fn load_from_file(p: Arc<Mutex<PositionMng>>) -> bool {
    let abs_path = get_abs_path("~/.ru_cdc/meta.json".to_string());
    let position = match read_file_content(abs_path) {
        Ok(s)=>serde_json::from_str::<PositionSet>(s.as_str()),
        Err(err)=>{
            println!("读取索引meta失败：{err:?}");
            return false;
        }
    };
    if let Ok(pos) = position{
        update_name_pos(p.clone(), &pos.binlog, pos.position);
    }
    return false;
}

pub fn update_name_pos(p: Arc<Mutex<PositionMng>>, binlog: &String, position: u32) {
    loop {
        if let Ok(mut p) = p.lock() {
            p.update(binlog, position);
            p.tx.send(PositionSet{
                binlog: p.binlog.clone(),
                position: p.position,
                saving_ts: current_ts(),
            }).expect("发送到队列错误");
            break
        }else{
            thread::sleep(std::time::Duration::from_micros(17));
        }
    }
}

pub fn update_pos(p: Arc<Mutex<PositionMng>>, position: u32) {
    loop {
        if let Ok(mut p) = p.lock() {
            p.new_pos(position);
            p.tx.send(PositionSet{
                binlog: p.binlog.clone(),
                position: p.position,
                saving_ts: current_ts(),
            }).expect("发送到队列错误");
            break
        }else{
            thread::sleep(std::time::Duration::from_micros(23));
        }
    }
}

fn read_from_row(row: &TextResult) -> (String, u32) {
    let file = String::from_utf8(row.columns[0].clone()).unwrap();
    let pos: u32 = String::from_utf8(row.columns[1].clone())
        .unwrap()
        .parse()
        .unwrap();
    (file, pos)
}

pub fn check_valid_pos(p: Arc<Mutex<PositionMng>>, rd: TextResultSet, from_start: bool) -> (String, u32) {
    let record_count = rd.rows.len();
    loop {
        if let Ok(mut p) = p.lock() {
            if p.loaded {
                //如果加载了状态文件，，就检测 from_start标识是否强制覆盖
                if from_start{
                    return (read_from_row(&rd.rows[0]).0, 4);
                }else{
                    return (p.binlog.clone(), p.position);
                }
            }else{
                // 如果没有状态文件，就要根据 from_start标识来判断是从头加载还是加载最后一段
                if from_start {
                    return (read_from_row(&rd.rows[0]).0, 4);
                }else{
                    return read_from_row(&rd.rows[record_count-1]);
                }
            }
        }else{
            thread::sleep(std::time::Duration::from_micros(23));
        }
    }


    ("".to_string(), 0)
}


fn read_file_content(file_path: String) -> Result<String, std::io::Error> {
    let path = Path::new(&file_path);

    // Create directory if it doesn't exist
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    // Check if file exists and read its content
    if path.exists() {
        fs::read_to_string(path)
    } else {
        Err(std::io::Error::new(std::io::ErrorKind::NotFound, "File not found"))
    }
}


fn write_file_content(file_path: String, file_data: &[u8]) -> Result<(), std::io::Error> {
    let path = Path::new(&file_path);
    fs::write(path, file_data)
}