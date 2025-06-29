use crate::utils::{find_bucket_index, parse_bid_ask_array};
use log::{debug, info, warn};
use serde::Deserialize;
use serde_json::Value;
use std::collections::VecDeque;
use std::fs::File;
use std::io::BufReader;
use tdigest::TDigest;

#[derive(Clone, Copy, Debug, Deserialize)]
pub struct BidAsk {
    pub price: f64,
    pub amount: f64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct MarketDataEntry {
    pub utc_epoch_ns: u64,
    pub bids: Vec<BidAsk>,
    pub asks: Vec<BidAsk>,
}

#[derive(Clone, Debug, Default)]
pub struct Bucket {
    pub start_time_ns: u64,
    pub end_time_ns: u64,
    pub count: usize,
    pub tdigest: TDigest,
    pub min_spread: f64,
    pub max_spread: f64,
    // TODO: change to reference? Or just store spread?
    pub entries: Vec<MarketDataEntry>,
}

impl Bucket {
    pub fn new(start_time_ns: u64, end_time_ns: u64) -> Self {
        Self {
            start_time_ns,
            end_time_ns,
            count: 0,
            tdigest: TDigest::default(),
            min_spread: f64::MAX,
            max_spread: f64::MIN,
            entries: Vec::new(),
        }
    }

    pub fn insert(&mut self, market_data_entry: MarketDataEntry) {
        self.count += 1;
        let spread = market_data_entry.asks[0].price - market_data_entry.bids[0].price;
        self.tdigest.merge_unsorted(vec![spread]);
        self.min_spread = self.min_spread.min(spread);
        self.max_spread = self.max_spread.max(spread);
        self.entries.push(market_data_entry);
    }

    pub fn remove_up_to(&mut self, time: u64) {
        if time < self.start_time_ns || time > self.end_time_ns {
            return;
        }
        self.entries.retain(|entry| entry.utc_epoch_ns > time);

        // Update counter, min and max.
        self.count = self.entries.len();
        let spreads: Vec<f64> = self
            .entries
            .iter()
            .map(|entry| entry.asks[0].price - entry.bids[0].price)
            .filter(|v| v.is_finite()) // 过滤 NaN、inf
            .collect();

        self.min_spread = *spreads
            .iter()
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        self.max_spread = *spreads
            .iter()
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap(); // self.max_spread = self.entries.iter().max();
        self.tdigest = TDigest::default();
        self.tdigest.merge_unsorted(spreads);
    }

    pub fn count_start_from(&self, start_time_ns: u64) -> usize {
        if self.start_time_ns <= start_time_ns && start_time_ns <= self.end_time_ns {
            self.entries
                .iter()
                .filter(|entry| entry.utc_epoch_ns >= start_time_ns)
                .count()
        } else {
            0
        }
    }

    pub fn count_end_before(&self, end_time_ns: u64) -> usize {
        if self.start_time_ns <= end_time_ns && end_time_ns <= self.end_time_ns {
            self.entries
                .iter()
                .filter(|entry| entry.utc_epoch_ns <= end_time_ns)
                .count()
        } else {
            0
        }
    }
}

#[derive(Debug)]
pub struct MarketDataCache {
    pub buckets: VecDeque<Bucket>, // for 100ms buckets
    pub bucket_ns: u64,
    pub count: u64,
}

impl MarketDataCache {
    pub fn new() -> Self {
        let mut buckets = VecDeque::with_capacity(36000);
        buckets.resize(36000, Bucket::default());
        Self {
            buckets,
            bucket_ns: 100_000_000, // 100ms
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

        for (i, entry) in entries.iter().enumerate() {
            // Handle timestamp.
            let utc_epoch_ns = match entry.get("utc_epoch_ns") {
                Some(Value::Number(n)) if n.as_i64().unwrap() <= 0 => {
                    warn!("Skipping entry {} due to invalid timestamp {}", i, n);
                    continue;
                }
                Some(Value::Number(n)) => {
                    if let Some(ts) = n.as_u64() {
                        ts
                    } else {
                        warn!("Skipping entry {} due to non-u64 timestamp {}", i, n);
                        continue;
                    }
                }
                _ => {
                    warn!("Skipping entry {} due to missing timestamp in json", i);
                    continue;
                }
            };

            // Handle bids.
            // Note that the raw data is already sorted from highest to lowest.
            let bids = match entry.get("bids") {
                Some(Value::Array(arr)) => parse_bid_ask_array(arr),
                _ => {
                    warn!("Skipping entry {} due to missing bids array in json", i);
                    continue;
                }
            };

            // Handle asks.
            // Note that the raw data is already sorted, from lowest to highest.
            let asks = match entry.get("asks") {
                Some(Value::Array(arr)) => parse_bid_ask_array(arr),
                _ => {
                    warn!("Skipping entry {} due to missing asks array in json", i);
                    continue;
                }
            };

            if utc_epoch_ns > 0 && bids.len() > 0 && asks.len() > 0 {
                market_data_entries.push(MarketDataEntry {
                    utc_epoch_ns,
                    bids,
                    asks,
                });
            } else {
                warn!("Skipping entry {} due to empty bids or asks array", i);
            }
        }

        info!(
            "Finished reading json file, {} raw entries are identified and {} are valid",
            entries.len(),
            market_data_entries.len()
        );

        let mut cache = Self::new();
        for entry in market_data_entries {
            cache.insert(entry);
        }
        cache
    }

    // Insert an entry into the cache.
    pub fn insert(&mut self, data: MarketDataEntry) {
        if self.count == 0 {
            // We need to initialize all buckets, because now all bucket start time is 0ns.
            let remainder = data.utc_epoch_ns % self.bucket_ns;
            let mut aligned_start_time_ns = data.utc_epoch_ns - remainder;
            for bucket in &mut self.buckets {
                bucket.start_time_ns = aligned_start_time_ns;
                bucket.end_time_ns = aligned_start_time_ns + self.bucket_ns;
                aligned_start_time_ns += self.bucket_ns;
            }
        }

        self.count += 1;
        let bucket_idx = find_bucket_index(
            self.buckets[0].start_time_ns,
            data.utc_epoch_ns,
            self.bucket_ns,
        )
        .unwrap();
        if bucket_idx >= self.buckets.len() {
            let hour_in_ns = 3_600_000_000_000;
            let threshold = data.utc_epoch_ns - hour_in_ns;
            self.remove_up_to(threshold);
        }
        let bucket_idx = find_bucket_index(
            self.buckets[0].start_time_ns,
            data.utc_epoch_ns,
            self.bucket_ns,
        )
        .unwrap();
        self.buckets[bucket_idx].insert(data);
    }

    // Remove all entries older or the same age as the specified time.
    // This function is only used for some periodic cleanup.
    pub fn remove_up_to(&mut self, time: u64) {
        let mut bucket_end_time = self.buckets[0].end_time_ns;
        while bucket_end_time <= time {
            let popped = self.buckets.pop_front().unwrap();
            bucket_end_time = popped.end_time_ns;
        }
        self.buckets[0].remove_up_to(time);

        // Insert new buckets.
        while self.buckets.len() < 36000 {
            let start = self.buckets.back().unwrap().end_time_ns;
            self.buckets
                .push_back(Bucket::new(start, start + self.bucket_ns));
        }
    }

    // Get the total number of entries in the cache.
    pub fn count(&self) -> u64 {
        self.count
    }

    // Get the number of entries in the given time range.
    // start_time and end_time may be any time within the last 1 hour.
    pub fn count_range(&self, start_time: u64, end_time: u64) -> usize {
        let cache_start_time_ns = self.buckets[0].start_time_ns;
        let start_idx =
            find_bucket_index(self.buckets[0].start_time_ns, start_time, 100_000_000).unwrap();
        let end_idx =
            find_bucket_index(self.buckets[0].start_time_ns, end_time, 100_000_000).unwrap();

        let mut cnt = 0;
        cnt += self.buckets[start_idx].count_start_from(start_time);
        for i in start_idx + 1..end_idx {
            cnt += self.buckets[i].count;
        }
        cnt += self.buckets[end_idx].count_end_before(end_time);
        cnt
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
