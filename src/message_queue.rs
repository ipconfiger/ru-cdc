use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::thread;
use rdkafka::ClientConfig;
use rdkafka::producer::{BaseRecord, ThreadedProducer, DefaultProducerContext};
use crate::config::{KafkaConfig, Mq, MqConfig, RedisConfig};
use redis::{AsyncCommands, Client, Commands};
use crate::executor::generate_random_number;
use std::sync::mpsc::{channel, Receiver, Sender};


#[derive(Debug, Clone)]
pub struct QueueMessage {
    pub topic: String,
    pub payload: String
}


#[derive(Debug, Clone)]
pub struct MessageQueues {
    chanels: HashMap<String, Arc<Mutex<Sender<QueueMessage>>>>
}

impl MessageQueues {
    pub fn new()->Self{
        Self{
            chanels: HashMap::new()
        }
    }

    pub fn register_tx(&mut self, chn: &String, tx: Arc<Mutex<Sender<QueueMessage>>>){
        self.chanels.insert(chn.clone(), tx);
    }

    pub fn push(&mut self, chn: &String, msg: QueueMessage) {
        if let Some(chn_ref) = self.chanels.get_mut(chn) {
            if let Ok(chn) = chn_ref.lock(){
                chn.send(msg).expect("send error");
            }else{
                println!("==========>(夭寿啦，获取锁失败了)");
            }
        }
    }

    pub fn start_message_queue_from_config(&mut self, queue_cfg: Vec<Mq>) {
        let queue_cfg = queue_cfg.clone();
        for cfg in queue_cfg {
            let (tx, rx) = channel();
            let tx = Arc::new(Mutex::new(tx));
            self.register_tx(&cfg.mq_name, tx);
            let config = cfg.clone();
            thread::spawn(move || {
                println!("Outgiving thread [{}]", &cfg.mq_name);
                let mut mq_ins: Box<dyn QueueClient> = match config.mq_cfg {
                    MqConfig::KAFKA(kfk)=>{
                        let kc = KafkaClient::init_from_config(&kfk);
                        Box::new(kc)
                    },
                    MqConfig::REDIS(rds)=>{
                        let rd = RedisClient::init_from_config(&rds);
                        Box::new(rd)
                    }
                };
                outgiving_body(rx, mq_ins.as_mut());
            });

        }
    }
}

fn outgiving_body(rx: Receiver<QueueMessage>, mq_ins: &mut dyn QueueClient) {
    loop{
        if let Ok(msg) = rx.recv() {
            mq_ins.queue_message(&msg);
        }
    }
}

trait QueueClient : Send{
    fn queue_message(&mut self, message: &QueueMessage);
}



struct KafkaClient {
    producer: Option<ThreadedProducer<DefaultProducerContext>>
}

impl KafkaClient{
    fn init_from_config(config: &KafkaConfig) -> Self {
        let servers = config.brokers.clone();
        let producer = if servers.is_empty() {
            None
        } else{
            let pd: Option<ThreadedProducer<DefaultProducerContext>> = match ClientConfig::new()
                .set("bootstrap.servers", servers)
                .set("message.timeout.ms", "5000")
                .set("queue.buffering.max.ms", format!("{}", config.queue_buffering_max))
                .create() {
                Ok(p)=>{
                    Some(p)
                },
                Err(err)=>{
                    println!("kafka producer error:{:?}", err);
                    None
                }
            };
            pd
        };
        Self{ producer }
    }
}

impl QueueClient for KafkaClient {
    fn queue_message(&mut self, message: &QueueMessage) {
        if let Some(producer) = &self.producer {
            match producer.send(BaseRecord::<String, String>::to(message.topic.as_str()).payload(&message.payload)) {
                Ok(_)=>{
                    //println!("Kafka Message Sent!");
                }
                Err((err, _))=>{
                    println!("Kafka sent error:{:?}", err);
                }
            }
        }else{
            println!("Kafka Not Connected");
        }
    }
}

struct RedisClient {
    redis: Option<Client>
}

impl RedisClient {
    fn init_from_config(config: &RedisConfig) -> Self {
        let (ip, port) = (config.ip.clone(), config.port);
        let redis = if let Ok(client) = redis::Client::open(format!("redis://{ip}:{port}")){
            Some(client)
        }else{ None };
        Self{redis}
    }
}

impl QueueClient for RedisClient {

    fn queue_message(&mut self, message: &QueueMessage) {
        if let Some(redis) = &self.redis {
            if let Ok(mut conn) = redis.get_connection() {
                match conn.rpush::<String, String, ()>(message.topic.clone(), message.payload.clone()) {
                    Ok(_)=>{
                        //println!("Redis Message Sent!");
                    },
                    Err(err)=>{
                        println!("Redis sent error:{:?}", err);
                    }
                }
            }else{
                println!("Redis Connect Error");
            }
        }else{
            println!("Redis Not Configured");
        }

    }
}