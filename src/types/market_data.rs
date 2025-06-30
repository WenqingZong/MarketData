//! Our main logic of this in-memory cache structure. A [MarketDataCache] consists of a Deque of continues [Bucket]s,
//! with O(1) time for pop front, push back, and indexing. Also, each [Bucket] object is warped in a [RwLock] for faster
//! multithreading access. Counter itself is Atomic as it's expected that this value will be updated often.

// System libraries.
use log::{info, warn};
use std::collections::VecDeque;
use std::fs::File;
use std::io::BufReader;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

// Third party libraries.
use rayon::prelude::*;
use serde_json::Value;
use tdigest::TDigest;

// Project libraries.
use crate::types::{Bucket, MarketDataCache, MarketDataEntry};
use crate::utils::{calculate_ave_price, find_bucket_index, parse_bid_ask_array};

impl MarketDataCache {
    /// A [MarketDataCache] object can hold data in the last num_buckets * bucket_ns ns.
    pub fn new(num_buckets: usize, bucket_ns: u64) -> Self {
        let buckets = VecDeque::with_capacity(num_buckets);
        Self {
            buckets,
            bucket_ns,
            num_buckets,
            count: AtomicUsize::new(0),
        }
    }

    /// Pre-populate with data for testing. This method will assume bucket size of 100ms and 36000 buckets, which is
    /// 1 hour of data. This method also handles some errors in input data, e.g. missing expected json fields, apparent
    /// outliers, etc.
    pub fn with_file(file_path: &str) -> Self {
        info!("Reading json file {file_path}");
        let file = File::open(file_path).unwrap();
        let reader = BufReader::new(file);

        // Some entries in input json are invalid, so first read everything as raw json values and filter them out later.
        let json: Value = serde_json::from_reader(reader).unwrap();
        let entries = json["market_data_entries"].as_array().unwrap();
        let mut market_data_entries = vec![];

        for (i, entry) in entries.iter().enumerate() {
            // Handle timestamp.
            let utc_epoch_ns = match entry.get("utc_epoch_ns") {
                Some(Value::Number(n)) if n.as_i64().unwrap() <= 0 => {
                    warn!("Skipping entry {i} due to invalid timestamp {n}");
                    continue;
                }
                Some(Value::Number(n)) => {
                    if let Some(ts) = n.as_u64() {
                        ts
                    } else {
                        warn!("Skipping entry {i} due to non-u64 timestamp {n}");
                        continue;
                    }
                }
                _ => {
                    warn!("Skipping entry {i} due to missing timestamp in json");
                    continue;
                }
            };

            // Handle bids.
            // Note that the raw data is already sorted from highest to lowest.
            let bids = match entry.get("bids") {
                Some(Value::Array(arr)) => parse_bid_ask_array(arr),
                _ => {
                    warn!("Skipping entry {i} due to missing bids array in json");
                    continue;
                }
            };

            // Handle asks.
            // Note that the raw data is already sorted, from lowest to highest.
            let asks = match entry.get("asks") {
                Some(Value::Array(arr)) => parse_bid_ask_array(arr),
                _ => {
                    warn!("Skipping entry {i} due to missing asks array in json");
                    continue;
                }
            };

            if bids.is_empty() || asks.is_empty() {
                warn!("Skipping entry {i} due to empty bids or asks array");
                continue;
            }
            let spread = asks[0].price - bids[0].price;

            // Safe unwrap here, because we already checked 0.
            let ave_bid = calculate_ave_price(&bids).unwrap();
            let ave_ask = calculate_ave_price(&asks).unwrap();
            if spread.abs() >= ave_ask * 0.03 || spread.abs() > ave_bid * 0.03 {
                warn!(
                    "Skipping entry {i} due to outlier, spread is {spread} but ave bid is {ave_bid} and ave ask is {ave_ask}"
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

        // 1 hour data, and each bucket is 100ms.
        let mut cache = Self::new(36000, 100_000_000);
        for entry in market_data_entries {
            cache.insert(entry);
        }
        cache
    }

    /// Insert an entry into the cache.
    pub fn insert(&mut self, data: MarketDataEntry) {
        if self.buckets.is_empty() {
            // Need to initialize all buckets.
            // We use aligned bucket start time for easier implementation.
            let remainder = data.utc_epoch_ns % self.bucket_ns;
            let aligned_start_time_ns = data.utc_epoch_ns - remainder;
            for i in 0..self.num_buckets {
                self.buckets.push_back(Arc::new(RwLock::new(Bucket::new(
                    aligned_start_time_ns + self.bucket_ns * i as u64,
                    aligned_start_time_ns + self.bucket_ns * (i + 1) as u64,
                ))));
            }
        }

        self.count.fetch_add(1, Ordering::SeqCst);
        let first_bucket_start_ns = {
            let first_bucket = self.buckets[0].read().unwrap();
            first_bucket.start_time_ns
        };

        // Find the desired bucket to insert into.
        let bucket_idx =
            match find_bucket_index(first_bucket_start_ns, data.utc_epoch_ns, self.bucket_ns) {
                Some(idx) => idx,
                None => {
                    return
                }
            };

        if bucket_idx >= self.buckets.len() {
            // So the new data is out of our cache time, need to delete some old data now!
            let total_cache_time_in_ns = self.num_buckets as u64 * self.bucket_ns;
            let cache_start_time_ns = first_bucket_start_ns;
            let threshold = cache_start_time_ns + self.bucket_ns * (bucket_idx + 1) as u64
                - total_cache_time_in_ns;
            self.remove_up_to(threshold);
        }
        // self.buckets changed, so need to re calculate index!
        let first_bucket_start_ns = {
            let first_bucket = self.buckets[0].read().unwrap();
            first_bucket.start_time_ns
        };
        let bucket_idx =
            find_bucket_index(first_bucket_start_ns, data.utc_epoch_ns, self.bucket_ns).unwrap();

        // Get write lock on the target bucket.
        let bucket = &self.buckets[bucket_idx];
        let mut bucket_lock = bucket.write().unwrap();
        bucket_lock.insert(data);
    }

    /// Remove all entries older or the same age as the specified time.
    /// This function is only used for some periodic cleanup.
    /// Returns the number of entries deleted.
    pub fn remove_up_to(&mut self, time: u64) -> usize {
        let original_count = self.count.load(Ordering::SeqCst);
        let mut bucket_end_time = {
            let first_bucket = self.buckets[0].read().unwrap();
            first_bucket.end_time_ns
        };
        while bucket_end_time <= time {
            // Delete the whole bucket.
            let popped = self.buckets.pop_front().unwrap();
            let removed_count = {
                let popped_bucket = popped.read().unwrap();
                popped_bucket.count
            };
            self.count.fetch_sub(removed_count, Ordering::SeqCst);

            bucket_end_time = {
                let new_first = self.buckets.front().unwrap().read().unwrap();
                new_first.end_time_ns
            };
        }

        // Now, cannot just delete the whole next Bucket, but only a small portion of its data.
        let deleted = {
            let mut first_bucket = self.buckets[0].write().unwrap();
            first_bucket.remove_up_to(time)
        };
        self.count.fetch_sub(deleted, Ordering::SeqCst);

        // We deleted some old buckets, time to insert new buckets to keep our total cache duration unchanged.
        while self.buckets.len() < self.num_buckets {
            // Get the end time of the last bucket.
            let last_end = {
                let last_bucket = self.buckets.back().unwrap().read().unwrap();
                last_bucket.end_time_ns
            };

            self.buckets.push_back(Arc::new(RwLock::new(Bucket::new(
                last_end,
                last_end + self.bucket_ns,
            ))));
        }
        original_count - self.count.load(Ordering::SeqCst)
    }

    /// Get the total number of entries in the cache.
    pub fn count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }

    /// Get the number of entries in the given time range, including both ends.
    /// start_time and end_time may be any time within the last 1 hour.
    pub fn count_range(&self, start_time: u64, end_time: u64) -> usize {
        // No sanity check here because we assumed start and end time are valid.
        // Get the start time of the first bucket.
        let cache_start_time_ns = {
            let first_bucket = self.buckets[0].read().unwrap();
            first_bucket.start_time_ns
        };

        let start_idx = find_bucket_index(cache_start_time_ns, start_time, self.bucket_ns).unwrap();
        let end_idx = find_bucket_index(cache_start_time_ns, end_time, self.bucket_ns).unwrap();

        let mut cnt = 0;

        // Handle the starting bucket, partial data.
        cnt += {
            let bucket = self.buckets[start_idx].read().unwrap();
            bucket.count_start_from(start_time)
        };

        // Handle the middle, complete bucket. Use rayon to speedup.
        if start_idx + 1 < end_idx {
            cnt += (start_idx + 1..end_idx)
                .into_par_iter()
                .map(|i| {
                    let bucket = self.buckets[i].read().unwrap();
                    bucket.count
                })
                .sum::<usize>();
        }

        // Handle the ending bucket, partial data.
        if start_idx != end_idx {
            cnt += {
                let bucket = self.buckets[end_idx].read().unwrap();
                bucket.count_end_before(end_time)
            };
        }

        cnt
    }

    /// Get the 10th, 50th, and 90th percentiles of the spread in the given time range.
    /// Spread is defined as the difference between the lowest ask price and highest bid price.
    /// start_time and end_time may be any time within the last 1 hour.
    pub fn spread_percentiles(&self, start_time: u64, end_time: u64) -> (f64, f64, f64) {
        // No sanity check here because we assumed start and end time are valid.
        let cache_start_time_ns = {
            let first_bucket = self.buckets[0].read().unwrap();
            first_bucket.start_time_ns
        };

        let start_idx = find_bucket_index(cache_start_time_ns, start_time, self.bucket_ns).unwrap();
        let end_idx = find_bucket_index(cache_start_time_ns, end_time, self.bucket_ns).unwrap();
        let mut tdigests = Vec::new();

        // Handle the starting bucket, partial data.
        {
            let bucket = self.buckets[start_idx].read().unwrap();
            let entries = bucket.get_start_from(start_time);
            if !entries.is_empty() {
                let spreads: Vec<f64> = entries.iter().map(|e| e.spread).collect();
                tdigests.push(TDigest::new_with_size(1000).merge_unsorted(spreads));
            }
        }

        // Handle the middle, complete buckets. Use rayon to speedup.
        let middle_tdigests: Vec<_> = (start_idx + 1..end_idx)
            .into_par_iter()
            .map(|i| {
                let bucket = self.buckets[i].read().unwrap();
                bucket.get_tdigest()
            })
            .collect();
        tdigests.extend(middle_tdigests);

        // Handle the last bucket, partial data.
        if start_idx != end_idx {
            let bucket = self.buckets[end_idx].read().unwrap();
            let entries = bucket.get_end_before(end_time);
            if !entries.is_empty() {
                let spreads: Vec<f64> = entries.iter().map(|e| e.spread).collect();
                tdigests.push(TDigest::new_with_size(1000).merge_unsorted(spreads));
            }
        }

        let merged = TDigest::merge_digests(tdigests);
        (
            merged.estimate_quantile(0.1),
            merged.estimate_quantile(0.5),
            merged.estimate_quantile(0.9),
        )
    }

    /// Get the minimum spread in the given time range.
    /// start_time and end_time may be any time within the last 1 hour.
    pub fn min_spread(&self, start_time: u64, end_time: u64) -> f64 {
        let cache_start_time_ns = {
            let first_bucket = self.buckets[0].read().unwrap();
            first_bucket.start_time_ns
        };

        let start_idx = find_bucket_index(cache_start_time_ns, start_time, self.bucket_ns).unwrap();
        let end_idx = find_bucket_index(cache_start_time_ns, end_time, self.bucket_ns).unwrap();
        let mut min = f64::MAX;

        // Handle the starting bucket, partial data.
        {
            let bucket = self.buckets[start_idx].read().unwrap();
            let entries = bucket.get_start_from(start_time);
            if !entries.is_empty() {
                let bucket_min = entries
                    .iter()
                    .map(|e| e.spread)
                    .min_by(|a, b| a.partial_cmp(b).unwrap())
                    .unwrap();
                min = min.min(bucket_min);
            }
        }

        // Handle the middle, complete buckets. Use rayon to speedup.
        let middle_part_min = (start_idx + 1..end_idx)
            .into_par_iter()
            .map(|i| {
                let bucket = self.buckets[i].read().unwrap();
                bucket.min_spread
            })
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap_or(f64::MAX);
        min = min.min(middle_part_min);

        // Handle the last bucket, partial data.
        if start_idx != end_idx {
            let bucket = self.buckets[end_idx].read().unwrap();
            let entries = bucket.get_end_before(end_time);
            if !entries.is_empty() {
                let bucket_min = entries
                    .iter()
                    .map(|e| e.spread)
                    .min_by(|a, b| a.partial_cmp(b).unwrap())
                    .unwrap();
                min = min.min(bucket_min);
            }
        }

        min
    }

    // Get the maximum spread in the given time range.
    // start_time and end_time may be any time within the last 1 hour.
    pub fn max_spread(&self, start_time: u64, end_time: u64) -> f64 {
        let cache_start_time_ns = {
            let first_bucket = self.buckets[0].read().unwrap();
            first_bucket.start_time_ns
        };

        let start_idx = find_bucket_index(cache_start_time_ns, start_time, self.bucket_ns).unwrap();
        let end_idx = find_bucket_index(cache_start_time_ns, end_time, self.bucket_ns).unwrap();
        let mut max = -f64::MAX;

        // Handle the starting bucket, partial data.
        {
            let bucket = self.buckets[start_idx].read().unwrap();
            let entries = bucket.get_start_from(start_time);
            if !entries.is_empty() {
                let bucket_max = entries
                    .iter()
                    .map(|e| e.spread)
                    .max_by(|a, b| a.partial_cmp(b).unwrap())
                    .unwrap();
                max = max.max(bucket_max);
            }
        }

        // Handle the middle, complete buckets. Use rayon to speedup.
        let middle_part_max = (start_idx + 1..end_idx)
            .into_par_iter()
            .map(|i| {
                let bucket = self.buckets[i].read().unwrap();
                bucket.max_spread
            })
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap_or_else(|| -f64::MAX);
        max = max.max(middle_part_max);

        // Handle the last bucket, partial data.
        if start_idx != end_idx {
            let bucket = self.buckets[end_idx].read().unwrap();
            let entries = bucket.get_end_before(end_time);
            if !entries.is_empty() {
                let bucket_max = entries
                    .iter()
                    .map(|e| e.spread)
                    .max_by(|a, b| a.partial_cmp(b).unwrap())
                    .unwrap();
                max = max.max(bucket_max);
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
            let read_lock = bucket.read().unwrap();
            assert_eq!(read_lock.start_time_ns, i as u64 * 10);
            assert_eq!(read_lock.end_time_ns, (i + 1) as u64 * 10);
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
