use std::env;
use dirs;
use std::path::{Path, PathBuf};
use nom::combinator::into;
use serde::{Serialize, Deserialize};
use serde_json::to_string;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConfig {
    pub brokers: String,
    pub queue_buffering_max: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisConfig {
    pub ip: String,
    pub port: u16
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MqConfig {
    KAFKA(KafkaConfig),
    REDIS(RedisConfig)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Mq {
    pub mq_name: String,
    pub mq_cfg: MqConfig
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Instance {
    pub mq: String,
    pub schemas: String,
    pub tables: String,
    pub black_list: Vec<String>,
    pub topic: String
}

impl Instance {
    pub fn check_if_need_a_mq(&mut self, db: String, table: String) -> Option<(String, String)> {
        if match_pattern(self.schemas.as_str(), db.as_str()) {
            for p in &self.black_list{
                if match_pattern(p.as_str(), table.as_str()){
                    return None
                }
            }
            if match_pattern(self.tables.as_str(), table.as_str()) {
                return Some((self.mq.clone(), self.topic.clone()));
            }
        }
        return None;
    }
}

fn match_pattern(pattern: &str, input: &str) -> bool {
    if pattern.ends_with('*') {
        let prefix = &pattern[..pattern.len() - 1];
        input.starts_with(prefix)
    } else if pattern.starts_with('*') {
        let suffix = &pattern[1..];
        input.ends_with(suffix)
    } else {
        let parts: Vec<&str> = pattern.split('*').collect();
        if parts.len() == 2 {
            input.starts_with(parts[0]) && input.ends_with(parts[1])
        } else {
            input == pattern
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub db_ip: String,
    pub db_port: u16,
    pub max_packages: u64,
    pub user_name: String,
    pub passwd: String,
    pub workers: u8,
    pub from_start: Option<bool>,
    pub mqs: Vec<Mq>,
    pub instances: Vec<Instance>
}

impl Config {
    pub fn to_json(&mut self) -> String {
        if let Ok(j_txt) = serde_json::to_string_pretty(self) {
            j_txt
        }else{
            "生成配置JSON失败".to_string()
        }
    }

    pub fn save_to(&mut self, path: String) {
        std::fs::write::<String, String>(path, self.to_json()).expect("保存配置出错")
    }

    pub fn load_from(path: String) -> Self {
        let abs_path = get_abs_path(path);
        if let Ok(bs) = std::fs::read(abs_path) {
            match serde_json::from_slice::<Config>(bs.as_slice()){
                Ok(cfg)=>{
                    return cfg;
                },
                Err(err)=>{
                    println!("配置文件损坏：{err:?}");
                    panic!("配置文件损坏");
                }
            }
        }
        panic!("配置文件损坏：文件不存在")
    }

    pub fn gen_default() -> Self {
        Self{
            db_ip: "127.0.0.1".to_string(),
            db_port: 3306,
            max_packages: 4294967295,
            user_name: "canal".to_string(),
            passwd: "canal".to_string(),
            workers: 0,
            from_start: Some(false),
            mqs: vec![Mq{ mq_name: "the_kafka".to_string(), mq_cfg: MqConfig::KAFKA(KafkaConfig{ brokers: "127.0.0.1:9092".to_string(), queue_buffering_max: 333 }) }],
            instances: vec![Instance{
                mq: "the_kafka".to_string(),
                schemas: "test*".to_string(),
                tables: "s*".to_string(),
                black_list: vec!["tb01".to_string(), "tb02".to_string()],
                topic: "db_change".to_string(),
            }],
        }
    }

}



pub fn get_abs_path(address: String) -> String{
    let absolute_path = if Path::new(&address).is_relative() {
        if address.starts_with("~"){
            return if let Some(mut path) = dirs::home_dir() {
                path.push(&address[2..].to_string());
                path.to_str().unwrap().to_string()
            } else {
                address
            }
        }else{
            let current_dir = env::current_dir().expect("Failed to get current directory");
            let mut path_buf = PathBuf::from(current_dir);
            path_buf.push(address);
            path_buf
        }
    } else {
        PathBuf::from(address)
    };
    return absolute_path.to_str().unwrap().to_string();
}