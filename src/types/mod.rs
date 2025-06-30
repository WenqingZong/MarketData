//! Data structure definition. The general idea is we keep a bucket that will hold all market data in 100ms, and provide
//! some cache result for each 100ms time period. Note that each 100ms bucket is aligned to 100ms boundary for easier
//! implementation.
//!
//! It's highly likely that our actual query need to take multiple buckets
//! into consideration, we will handle each query in three parts:
//! 1. The bucket that contains start time, get everything in this bucket that happens after start time.
//! 2. The buckets in the middle of start to end. These are whole buckets, and their result are already calculated and
//!    cached in themselves.
//! 3. The bucket that contains end time. get everything in this bucket that happens before end time.

pub mod bucket;
pub mod market_data;

// System libraries.
use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};

// Third party libraries.
use serde::Deserialize;
use tdigest::TDigest;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq)]
pub struct BidAsk {
    pub price: f64,
    pub amount: f64,
}

/// One entry can have multiple [BidAsk] record, but we only care about its spread, so no need to store [BidAsk] array.
#[derive(Clone, Debug, Deserialize)]
pub struct MarketDataEntry {
    pub utc_epoch_ns: u64,
    pub spread: f64,
}

/// A [Bucket] will keep a record of its start and end time just for easier implementation. (I know end_time_ns is not
/// really needed). Count is the number of data entries contained in this bucket, tdigest is a fast algorithm to help us
/// calculate rank based statistics. min and max are our cache of each bucket.
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

/// A [MarketDataCache] uses a deque to hold all its [Bucket]s, O(1) for indexing, pop front and push back operations.
/// bucket_ns and num_buckets are just two helper variables to make calculations easier. Count is the total number of
/// [MarketDataEntry] stored in this cache. The total time duration represented by [MarketDataCache] is bucket_ns *
/// num_buckets. Note that bucket_ns and num_buckets never change.
#[derive(Debug)]
pub struct MarketDataCache {
    pub buckets: VecDeque<Arc<RwLock<Bucket>>>, // for 100ms buckets
    pub bucket_ns: u64,
    pub num_buckets: usize,
    pub count: AtomicUsize,
}
