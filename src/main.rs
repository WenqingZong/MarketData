mod market_data;
mod utils;
mod types;

// System libraries.
use env_logger;
use log::{LevelFilter, info};

// Project libraries.
use crate::types::MarketDataCache;

fn main() {
    env_logger::builder()
        .filter_level(LevelFilter::Debug)
        .init();
    info!("Logging system initialized");
    let mut data = MarketDataCache::with_file("./market_data.json");
    dbg!(&data.count());
    dbg!(&data.buckets.len());
    // let start_time = data.buckets[0].start_time_ns;
    // let end_time = data.buckets.back().unwrap().end_time_ns - 10000;

    let start_time = 1731496040100000000;
    let end_time = 1731496040200000000 - 1000;
    dbg!(&data.spread_percentiles(start_time, end_time));

    dbg!(data.count());
    dbg!(data.count_range(start_time, end_time));
    dbg!(data.max_spread(start_time, end_time));
    dbg!(data.min_spread(start_time, end_time));
}
