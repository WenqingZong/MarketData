mod market_data;
mod utils;
use env_logger;
use log::{LevelFilter, info, warn};

fn main() {
    env_logger::builder()
        .filter_level(LevelFilter::Debug)
        .init();
    info!("Logging system initialized");
    let data = market_data::MarketDataCache::with_file("./market_data.json");
    dbg!(&data.count());
    dbg!(&data.buckets.len());
    for i in 0..data.buckets.len() {
        if data.buckets[i].count > 0 {
            dbg!(&data.buckets[i]);
        }
    }
    let start_time = data.buckets[0].start_time_ns;
    let end_time = data.buckets.back().unwrap().end_time_ns - 10000;
    dbg!(data.count());
    dbg!(data.count_range(start_time, end_time));
    dbg!(data.max_spread(start_time, end_time));
    dbg!(data.min_spread(start_time, end_time));
}
