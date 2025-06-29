mod market_data;
use env_logger;
use log::{LevelFilter, info, warn};

fn main() {
    env_logger::builder()
        .filter_level(LevelFilter::Debug)
        .init();
    info!("Logging system initialized");
    let data = market_data::MarketDataCache::with_file("./market_data.json");
}
