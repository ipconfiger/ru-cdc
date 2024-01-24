use crate::executor::current_ts;
use chrono::Local;

pub struct Statistics{
    pub all_bytes: u128,
    pub check_bytes: u128,
    pub last_checkpoint: u64
}

impl Statistics {
    pub fn new()->Self{
        Self{
            all_bytes: 0,
            check_bytes: 0,
            last_checkpoint: 0,
        }
    }
    pub fn feed_bytes(&mut self, seq_idx:u64, bytes_count: usize) {
        let ts = current_ts() / 1000;
        self.all_bytes += bytes_count as u128;
        if self.last_checkpoint > 0 {
            if ts - self.last_checkpoint > 5 {
                let local_time = Local::now();
                let rest_bytes = self.all_bytes - self.check_bytes;
                let time_used = ts - self.last_checkpoint;
                let byte_rate = rest_bytes / time_used as u128;
                let mb_rate = byte_rate as f64 / (1024f64 * 1024f64);
                let total = self.all_bytes as f64 / (1024f64 * 1024f64);
                self.check_bytes = self.all_bytes;
                self.last_checkpoint = ts;
                let dts = local_time.format("%Y/%m/%d %H:%M:%S");
                println!("{dts} |=> 处理包计数:{seq_idx}，总流量:{total:02}MB 当前速率:{mb_rate:0.2} MB/s");
            }
        }else{
            self.last_checkpoint = ts;
        }
    }
}