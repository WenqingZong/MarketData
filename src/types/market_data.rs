use crate::types::{Bucket, MarketDataCache, MarketDataEntry};
use crate::utils::{calculate_ave_price, f64_max, f64_min, find_bucket_index, parse_bid_ask_array};
use log::{info, warn};
use serde_json::Value;
use std::collections::VecDeque;
use std::fs::File;
use std::io::BufReader;
use tdigest::TDigest;

impl MarketDataCache {
    pub fn new(num_buckets: usize, bucket_ns: u64) -> Self {
        let buckets = VecDeque::with_capacity(num_buckets);
        Self {
            buckets,
            bucket_ns,
            num_buckets,
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

            if bids.len() == 0 || asks.len() == 0 {
                warn!("Skipping entry {} due to empty bids or asks array", i);
                continue;
            }
            let spread = asks[0].price - bids[0].price;
            // Safe unwrap here, because we already checked 0.
            let ave_bid = calculate_ave_price(&bids).unwrap();
            let ave_ask = calculate_ave_price(&asks).unwrap();
            if spread.abs() >= ave_ask * 0.03 || spread.abs() > ave_bid * 0.03 {
                warn!(
                    "Skipping entry {} due to outlier, spread is {} but ave bid is {} and ave ask is {}",
                    i, spread, ave_bid, ave_ask
                );
                continue;
            }
            market_data_entries.push(MarketDataEntry {
                utc_epoch_ns,
                spread: asks[0].price - bids[0].price,
            });
        }

        info!(
            "Finished reading json file, {} raw entries are identified and {} are valid",
            entries.len(),
            market_data_entries.len()
        );

        let mut cache = Self::new(36000, 100_000_000);
        for entry in market_data_entries {
            cache.insert(entry);
        }
        cache
    }

    // Insert an entry into the cache.
    pub fn insert(&mut self, data: MarketDataEntry) {
        if self.buckets.len() == 0 {
            // We need to initialize all buckets.
            let remainder = data.utc_epoch_ns % self.bucket_ns;
            let aligned_start_time_ns = data.utc_epoch_ns - remainder;
            for i in 0..self.num_buckets {
                self.buckets.push_back(Bucket::new(
                    aligned_start_time_ns + self.bucket_ns * i as u64,
                    aligned_start_time_ns + self.bucket_ns * (i + 1) as u64,
                ));
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
            let total_cache_time_in_ns = self.num_buckets as u64 * self.bucket_ns;
            let cache_start_time_ns = self.buckets[0].start_time_ns;
            let threshold = cache_start_time_ns + self.bucket_ns * (bucket_idx + 1) as u64
                - total_cache_time_in_ns;
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
    // Returns the number of entries deleted.
    pub fn remove_up_to(&mut self, time: u64) -> usize {
        let original_count = self.count;
        let mut bucket_end_time = self.buckets[0].end_time_ns;
        while bucket_end_time <= time {
            let popped = self.buckets.pop_front().unwrap();
            bucket_end_time = self.buckets.front().unwrap().end_time_ns;
            self.count -= popped.count;
        }
        let deleted = self.buckets[0].remove_up_to(time);
        self.count -= deleted;

        // Insert new buckets.
        while self.buckets.len() < self.num_buckets {
            let start = self.buckets.back().unwrap().end_time_ns;
            self.buckets
                .push_back(Bucket::new(start, start + self.bucket_ns));
        }
        original_count - self.count
    }

    // Get the total number of entries in the cache.
    pub fn count(&self) -> usize {
        self.count
    }

    // Get the number of entries in the given time range, including both ends.
    // start_time and end_time may be any time within the last 1 hour.
    pub fn count_range(&self, start_time: u64, end_time: u64) -> usize {
        let cache_start_time_ns = self.buckets[0].start_time_ns;
        let start_idx = find_bucket_index(cache_start_time_ns, start_time, self.bucket_ns).unwrap();
        let end_idx = find_bucket_index(cache_start_time_ns, end_time, self.bucket_ns).unwrap();
        dbg!(start_idx, end_idx);

        let mut cnt = 0;
        cnt += self.buckets[start_idx].count_start_from(start_time);
        for i in start_idx + 1..end_idx {
            cnt += self.buckets[i].count;
        }
        if end_idx != start_idx {
            cnt += self.buckets[end_idx].count_end_before(end_time);
        }
        cnt
    }

    // Get the 10th, 50th, and 90th percentiles of the spread in the given time range.
    // Spread is defined as the difference between the lowest ask price and highest bid price.
    // start_time and end_time may be any time within the last 1 hour.
    pub fn spread_percentiles(&mut self, start_time: u64, end_time: u64) -> (f64, f64, f64) {
        let mut tdigests = vec![];

        let cache_start_time_ns = self.buckets[0].start_time_ns;
        let start_idx = find_bucket_index(cache_start_time_ns, start_time, self.bucket_ns).unwrap();
        let end_idx = find_bucket_index(cache_start_time_ns, end_time, self.bucket_ns).unwrap();

        let entries1: Vec<f64> = self.buckets[start_idx]
            .get_start_from(start_time)
            .iter()
            .map(|entry| entry.spread)
            .collect();
        let mut tdigest1 = TDigest::new_with_size(1000);
        tdigest1 = tdigest1.merge_unsorted(entries1);
        tdigests.push(tdigest1);

        for i in start_idx + 1..end_idx {
            tdigests.push(self.buckets[i].get_tdigest());
        }

        if start_idx != end_idx {
            let entries2: Vec<f64> = self.buckets[end_idx]
                .get_end_before(end_time)
                .iter()
                .map(|entry| entry.spread)
                .collect();
            let mut tdigest2 = TDigest::new_with_size(1000);
            tdigest2 = tdigest2.merge_unsorted(entries2);
            tdigests.push(tdigest2);
        }

        let tdigest = TDigest::merge_digests(tdigests);
        (
            tdigest.estimate_quantile(0.1),
            tdigest.estimate_quantile(0.5),
            tdigest.estimate_quantile(0.9),
        )
    }

    // Get the minimum spread in the given time range.
    // start_time and end_time may be any time within the last 1 hour.
    pub fn min_spread(&self, start_time: u64, end_time: u64) -> f64 {
        let start_idx =
            find_bucket_index(self.buckets[0].start_time_ns, start_time, self.bucket_ns).unwrap();
        let end_idx =
            find_bucket_index(self.buckets[0].start_time_ns, end_time, self.bucket_ns).unwrap();
        let mut min = f64::MAX;

        // Get the entries after start time in start_idx bucket.
        let entries1: Vec<f64> = self.buckets[start_idx]
            .get_start_from(start_time)
            .iter()
            .map(|entry| entry.spread)
            .collect();
        if entries1.len() > 0 {
            let min1 = *f64_min(&entries1).unwrap();
            min = min.min(min1);
        }

        for i in start_idx + 1..end_idx {
            min = min.min(self.buckets[i].min_spread);
        }

        // Get the entries before end time in end_idx bucket.
        if start_idx != end_idx {
            let entries2: Vec<f64> = self.buckets[end_idx]
                .get_end_before(end_time)
                .iter()
                .map(|entry| entry.spread)
                .collect();
            if entries2.len() > 0 {
                let min2 = *f64_min(&entries2).unwrap();
                min = min.min(min2);
            }
        }

        min
    }

    // Get the maximum spread in the given time range.
    // start_time and end_time may be any time within the last 1 hour.
    pub fn max_spread(&self, start_time: u64, end_time: u64) -> f64 {
        let start_idx =
            find_bucket_index(self.buckets[0].start_time_ns, start_time, self.bucket_ns).unwrap();
        let end_idx =
            find_bucket_index(self.buckets[0].start_time_ns, end_time, self.bucket_ns).unwrap();
        let mut max = -1.0 * f64::MAX;

        // Get the entries after start time in start_idx bucket.
        let entries1: Vec<f64> = self.buckets[start_idx]
            .get_start_from(start_time)
            .iter()
            .map(|entry| entry.spread)
            .collect();
        if entries1.len() > 0 {
            let max1 = *f64_max(&entries1).unwrap();
            max = max.max(max1);
        }

        for i in start_idx + 1..end_idx {
            max = max.max(self.buckets[i].min_spread);
        }

        // Get the entries before end time in end_idx bucket.
        if start_idx != end_idx {
            let entries2: Vec<f64> = self.buckets[end_idx]
                .get_end_before(end_time)
                .iter()
                .map(|entry| entry.spread)
                .collect();
            if entries2.len() > 0 {
                let max2 = *f64_max(&entries2).unwrap();
                max = max.max(max2);
            }
        }

        max
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_market_data_cache() {
        let mut cache = MarketDataCache::new(10, 10);
        let entry = MarketDataEntry {
            utc_epoch_ns: 0,
            spread: 1.0,
        };

        cache.insert(entry);
        assert_eq!(cache.count(), 1);

        for (i, bucket) in cache.buckets.iter().enumerate() {
            assert_eq!(bucket.start_time_ns, i as u64 * 10);
            assert_eq!(bucket.end_time_ns, (i + 1) as u64 * 10);
        }
        assert_eq!(cache.buckets.len(), 10);
    }

    #[test]
    fn test_remove_up_to() {
        let mut cache = MarketDataCache::new(4, 10);
        let entries: Vec<MarketDataEntry> = (0..16)
            .map(|i| MarketDataEntry {
                utc_epoch_ns: i * 5,
                spread: i as f64,
            })
            .collect();
        for entry in entries {
            cache.insert(entry);
        }
        assert_eq!(cache.count(), 7);
        cache.remove_up_to(60);
        assert_eq!(cache.count(), 3);
    }

    #[test]
    fn test_count_range() {
        let mut cache = MarketDataCache::new(4, 10);
        let entries: Vec<MarketDataEntry> = (0..16)
            .map(|i| MarketDataEntry {
                utc_epoch_ns: i * 5,
                spread: i as f64,
            })
            .collect();
        for entry in entries {
            cache.insert(entry);
        }
        let count = cache.count_range(45, 60);
        assert_eq!(count, 4);
    }

    #[test]
    fn test_min_spread() {
        let mut cache = MarketDataCache::new(10, 10);
        let entries: Vec<MarketDataEntry> = (0..100)
            .map(|i| MarketDataEntry {
                utc_epoch_ns: i,
                spread: i as f64,
            })
            .collect();
        for entry in entries {
            cache.insert(entry);
        }
        let min_spread = cache.min_spread(30, 70);
        assert_eq!(min_spread, 30.0);
    }

    #[test]
    fn test_max_spread() {
        let mut cache = MarketDataCache::new(10, 10);
        let entries: Vec<MarketDataEntry> = (0..100)
            .map(|i| MarketDataEntry {
                utc_epoch_ns: i,
                spread: i as f64,
            })
            .collect();
        for entry in entries {
            cache.insert(entry);
        }
        let max_spread = cache.max_spread(30, 70);
        assert_eq!(max_spread, 70.0);
    }

    #[test]
    fn test_spread_percentiles() {
        let mut cache = MarketDataCache::new(10, 10);
        let entries: Vec<MarketDataEntry> = (0..100)
            .map(|i| MarketDataEntry {
                utc_epoch_ns: i,
                spread: i as f64,
            })
            .collect();
        for entry in entries {
            cache.insert(entry);
        }
        let (a, b, c) = cache.spread_percentiles(0, 99);

        assert_eq!(a, 9.5);
        assert_eq!(b, 49.5);
        assert_eq!(c, 89.5);
    }
}
