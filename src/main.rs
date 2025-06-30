//! Just a sample main implementation. I used the provided json file to do some basic testing.

mod types;
mod utils;

// System libraries.
use log::{LevelFilter, info};

// Third party libraries.
use rayon::ThreadPoolBuilder;

// Project libraries.
use crate::types::MarketDataCache;

fn main() {
    env_logger::builder()
        .filter_level(LevelFilter::Debug)
        .init();
    info!("Logging system initialized");

    ThreadPoolBuilder::new()
        .num_threads(num_cpus::get())
        .build_global()
        .unwrap();

    let cache = MarketDataCache::with_file("./market_data.json");
    dbg!(&cache.count());
    dbg!(&cache.buckets.len());

    let lock = cache.buckets[0].read().unwrap();
    let start_time = lock.start_time_ns;
    let lock = cache.buckets.back().unwrap().read().unwrap();
    let end_time = lock.end_time_ns - 10000;

    dbg!(&cache.spread_percentiles(start_time, end_time));
    dbg!(cache.count());
    dbg!(cache.count_range(start_time, end_time));
    dbg!(cache.max_spread(start_time, end_time));
    dbg!(cache.min_spread(start_time, end_time));
}
