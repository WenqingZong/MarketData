use log::{debug, info, warn};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;
use tdigest::TDigest;
use std::collections::VecDeque;
use std::fs::File;
use std::io::BufReader;
use crate::utils::{decimal_from_json, parse_bid_ask_array};

#[derive(Debug, Deserialize)]
pub struct BidAsk {
    #[serde(deserialize_with = "decimal_from_json")]
    pub price: Decimal,

    #[serde(deserialize_with = "decimal_from_json")]
    pub amount: Decimal,
}

#[derive(Debug, Deserialize)]
pub struct MarketDataEntry {
    pub utc_epoch_ns: u64,
    pub bids: Vec<BidAsk>,
    pub asks: Vec<BidAsk>,
}

#[derive(Debug, Default)]
pub struct Bucket {
    pub start_time_ns: u64,
    pub count: u64,
    pub tdigest: TDigest,
    pub min_spread: Decimal, 
    pub max_spread: Decimal,
}

impl Bucket {
    pub fn new(start_time_ns: u64) -> Self {
        Self {
            start_time_ns,
            count: 0,
            tdigest: TDigest::default(),
            min_spread: Decimal::MAX,
            max_spread: Decimal::MIN,
        }
    }

    pub fn add(&mut self, market_data_entry: &MarketDataEntry) {
        self.count += 1;
        let spread = market_data_entry.asks[0].price - market_data_entry.bids[0].price;
        self.tdigest.merge_unsorted(vec![spread.to_f64().unwrap()]);
        self.min_spread = self.min_spread.min(spread);
        self.max_spread = self.max_spread.max(spread);
    }
}

#[derive(Debug)]
pub struct MarketDataCache {
    pub fine: VecDeque<Bucket>, // for 100ms buckets
    pub coarse: VecDeque<Bucket>, // for 1 min buckets
    pub fine_ns: u64,
    pub coarse_ns: u64,
    pub count: u64,
}

impl MarketDataCache {
    pub fn new() -> Self {
        Self {
            fine: VecDeque::with_capacity(36000),
            coarse: VecDeque::with_capacity(60),
            fine_ns: 100_000_000, // 100ms
            coarse_ns: 60 * 1_000_000_000, // 1 minute
            count: 0,
        }
    }

    // Pre-populate with data for testing.
    pub fn with_file(file_path: &str) -> Self {
        info!("Reading json file {}", file_path);
        let file = File::open(file_path).unwrap();
        let reader = BufReader::new(file);

        // Some fields in input json are invalid, so first read everything as raw json values and filter them out later.
        let json: Value = serde_json::from_reader(reader).unwrap();
        let entries = json["market_data_entries"].as_array().unwrap();
        let mut market_data_entries = vec![];

        for entry in entries {
            // Handle timestamp.
            let utc_epoch_ns = match entry.get("utc_epoch_ns") {
                Some(Value::Number(n)) if n.as_i64().unwrap() <= 0 => {
                    warn!("Skipping entry due to invalid timestamp {}", n);
                    continue;
                }
                Some(Value::Number(n)) => {
                    if let Some(ts) = n.as_u64() {
                        ts
                    } else {
                        warn!("Skipping entry due to non-u64 timestamp {}", n);
                        continue;
                    }
                }
                _ => {
                    warn!("Skipping entry due to missing timestamp");
                    continue;
                }
            };

            // Handle bids.
            // Note that the raw data is already sorted from highest to lowest.
            let bids = match entry.get("bids") {
                Some(Value::Array(arr)) => parse_bid_ask_array(arr),
                _ => {
                    warn!("Skipping entry due to missing or invalid bids array");
                    Vec::new()
                }
            };

            // Handle asks.
            // Note that the raw data is already sorted, from lowest to highest.
            let asks = match entry.get("asks") {
                Some(Value::Array(arr)) => parse_bid_ask_array(arr),
                _ => {
                    warn!("Skipping entry due to missing or invalid asks array");
                    Vec::new()
                }
            };

            market_data_entries.push(MarketDataEntry {
                utc_epoch_ns,
                bids,
                asks,
            });
        }

        info!("Finished reading json file, {} raw entries are identified and {} are valid", entries.len(), market_data_entries.len());
        let mut cache = Self::new();

        for entry in market_data_entries {
            cache.insert(&entry);
        }
        cache
    }

    // Insert an entry into the cache.
    pub fn insert(&mut self, data: &MarketDataEntry) {
        unimplemented!()
    }

    // Remove all entries older or the same age as the specified time.
    // This function is only used for some periodic cleanup.
    pub fn remove_up_to(&mut self, time: i64) {
        unimplemented!()
    }

    // Get the total number of entries in the cache.
    pub fn count(&self) -> i64 {
        unimplemented!()
    }

    // Get the number of entries in the given time range.
    // start_time and end_time may be any time within the last 1 hour.
    pub fn count_range(&self, start_time: i64, end_time: i64) -> i64 {
        unimplemented!()
    }

    // Get the 10th, 50th, and 90th percentiles of the spread in the given time range.
    // Spread is defined as the difference between the lowest ask price and highest bid price.
    // start_time and end_time may be any time within the last 1 hour.
    pub fn spread_percentiles(&self, start_time: i64, end_time: i64) -> (f64, f64, f64) {
        unimplemented!()
    }

    // Get the minimum spread in the given time range.
    // start_time and end_time may be any time within the last 1 hour.
    pub fn min_spread(&self, start_time: i64, end_time: i64) -> f64 {
        unimplemented!()
    }

    // Get the maximum spread in the given time range.
    // start_time and end_time may be any time within the last 1 hour.
    pub fn max_spread(&self, start_time: i64, end_time: i64) -> f64 {
        unimplemented!()
    }
}
