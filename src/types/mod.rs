pub mod bucket;
pub mod market_data;

// System libraries.
use std::cell::RefCell;
use std::collections::VecDeque;

// Third party libraries.
use serde::Deserialize;
use tdigest::TDigest;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq)]
pub struct BidAsk {
    pub price: f64,
    pub amount: f64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct MarketDataEntry {
    pub utc_epoch_ns: u64,
    pub spread: f64,
}

#[derive(Clone, Debug, Default)]
pub struct Bucket {
    pub start_time_ns: u64,
    pub end_time_ns: u64,
    pub count: usize,
    pub tdigest: RefCell<Option<TDigest>>,
    pub min_spread: f64,
    pub max_spread: f64,
    pub entries: Vec<MarketDataEntry>,
}

#[derive(Debug)]
pub struct MarketDataCache {
    pub buckets: VecDeque<Bucket>, // for 100ms buckets
    pub bucket_ns: u64,
    pub num_buckets: usize,
    pub count: usize,
}
