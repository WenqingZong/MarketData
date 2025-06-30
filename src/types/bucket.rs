//! [Bucket] is our smallest cache unit, it holds the cached result of small amount of time.

// System libraries.
use std::cell::RefCell;

// Third party libraries.
use tdigest::TDigest;

// Project libraries.
use crate::types::{Bucket, MarketDataEntry};
use crate::utils::{f64_max, f64_min};

// Should be safe, as we have a RwLock outside of each Bucket.
unsafe impl Send for Bucket {}
unsafe impl Sync for Bucket {}

impl Bucket {
    /// A [Bucket] is defined by its start and end time, represented by u64 in ns.
    pub fn new(start_time_ns: u64, end_time_ns: u64) -> Self {
        Self {
            start_time_ns,
            end_time_ns,
            count: 0,
            // We will use a lazy calculation, so most of the time, tdigest will remain None.
            tdigest: RefCell::new(None),
            min_spread: f64::MAX,
            max_spread: -f64::MAX,
            entries: Vec::new(),
        }
    }

    /// Insert one more [MarketDataEntry] to [Bucket]. If entry utc time is not in the range of this bucket, insert will
    /// return false. Otherwise true.
    pub fn insert(&mut self, market_data_entry: MarketDataEntry) -> bool {
        // A quick check the new data indeed belongs to this bucket.
        if !(self.start_time_ns <= market_data_entry.utc_epoch_ns
            && market_data_entry.utc_epoch_ns < self.end_time_ns)
        {
            return false;
        }
        // We'll use lazy calculation here.
        self.tdigest = RefCell::new(None);
        self.count += 1;
        let spread = market_data_entry.spread;

        // Update our cache results.
        self.min_spread = self.min_spread.min(spread);
        self.max_spread = self.max_spread.max(spread);

        // Original values will be used when we only want to select a part of this bucket's data, so still need to store
        // them.
        self.entries.push(market_data_entry);

        true
    }

    /// If threshold is in the range of [Bucket] start and end timestamp, then remove everything happens before
    /// threshold and return the number of elements removed. Otherwise, return 0.
    pub fn remove_up_to(&mut self, threshold: u64) -> usize {
        if threshold < self.start_time_ns || threshold > self.end_time_ns {
            // If <, everything should be kept, if >, then the whole bucket should be removed from our cache.
            return 0;
        }

        let original_count = self.count;
        // Filter out.
        self.entries.retain(|entry| entry.utc_epoch_ns > threshold);

        // Update count, min and max.
        self.count = self.entries.len();
        let spreads: Vec<f64> = self
            .entries
            .iter()
            .map(|entry| entry.spread)
            .filter(|v| v.is_finite()) // Filter out NaN、inf
            .collect();

        if self.count > 0 {
            self.min_spread = *f64_min(&spreads).unwrap();
            self.max_spread = *f64_max(&spreads).unwrap();
        } else {
            self.min_spread = f64::MAX;
            self.max_spread = -f64::MAX;
        }

        // Lazy calculation again.
        self.tdigest = RefCell::new(None);
        original_count - self.count
    }

    /// Get everything between [threshold time, bucket end time].
    pub fn get_start_from(&self, threshold: u64) -> Vec<&MarketDataEntry> {
        if self.start_time_ns <= threshold && threshold <= self.end_time_ns {
            self.entries
                .iter()
                .filter(|entry| entry.utc_epoch_ns >= threshold)
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Count number of elements in between [threshold time, bucket end time].
    pub fn count_start_from(&self, threshold: u64) -> usize {
        self.get_start_from(threshold).len()
    }

    /// Get everything between [bucket start time, threshold].
    pub fn get_end_before(&self, threshold: u64) -> Vec<&MarketDataEntry> {
        if self.start_time_ns <= threshold && threshold <= self.end_time_ns {
            self.entries
                .iter()
                .filter(|entry| entry.utc_epoch_ns <= threshold)
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Count number of elements in between [bucket start time, threshold].
    pub fn count_end_before(&self, threshold: u64) -> usize {
        self.get_end_before(threshold).len()
    }

    /// Lazy calculate of TDigest.
    pub fn get_tdigest(&self) -> TDigest {
        let mut tdigest_opt = self.tdigest.borrow_mut();
        if let Some(tdigest) = &*tdigest_opt {
            return tdigest.clone();
        }

        let spreads = self.entries.iter().map(|e| e.spread).collect();
        let new_tdigest = TDigest::new_with_size(100).merge_unsorted(spreads);
        *tdigest_opt = Some(new_tdigest.clone());
        new_tdigest
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_bucket() {
        let bucket = Bucket::default();
        assert_eq!(bucket.count, 0);
        assert_eq!(bucket.start_time_ns, 0);
        assert_eq!(bucket.end_time_ns, 0);
        assert!(bucket.tdigest.borrow().is_none());
    }

    #[test]
    fn test_new_bucket() {
        let bucket = Bucket::new(10, 100);
        assert_eq!(bucket.count, 0);
        assert_eq!(bucket.start_time_ns, 10);
        assert_eq!(bucket.end_time_ns, 100);
        assert!(bucket.tdigest.borrow().is_none());
        assert_eq!(bucket.min_spread, f64::MAX);
        assert_eq!(bucket.max_spread, -1.0 * f64::MAX);
    }

    #[test]
    fn test_insert() {
        let market_data_entries: Vec<MarketDataEntry> = (0..20)
            .map(|i| MarketDataEntry {
                utc_epoch_ns: i,
                spread: i as f64,
            })
            .collect();
        let mut bucket = Bucket::new(0, 10);
        for (i, entry) in market_data_entries.into_iter().enumerate() {
            let result = bucket.insert(entry);
            if i <= 9 {
                assert!(result);
            } else {
                assert!(!result);
            }
        }
        assert_eq!(bucket.count, 10);
        assert_eq!(bucket.min_spread, 0.0);
        assert_eq!(bucket.max_spread, 9.0);
        assert!(bucket.tdigest.borrow().is_none());
    }

    #[test]
    fn test_remove_up_to() {
        let market_data_entries: Vec<MarketDataEntry> = (0..20)
            .map(|i| MarketDataEntry {
                utc_epoch_ns: i,
                spread: i as f64,
            })
            .collect();
        let mut bucket = Bucket::new(5, 20);
        for entry in market_data_entries {
            bucket.insert(entry);
        }
        assert_eq!(bucket.count, 15);

        let deleted = bucket.remove_up_to(30);
        assert_eq!(bucket.count, 15);
        assert_eq!(deleted, 0);

        let deleted = bucket.remove_up_to(3);
        assert_eq!(bucket.count, 15);
        assert_eq!(deleted, 0);

        let deleted = bucket.remove_up_to(10);
        assert_eq!(deleted, 6);
        assert_eq!(bucket.count, 9);
        assert_eq!(bucket.max_spread, 19.0);
        assert_eq!(bucket.min_spread, 11.0);
        assert!(bucket.tdigest.borrow().is_none());
    }

    #[test]
    fn test_get_start_from() {
        let market_data_entries: Vec<MarketDataEntry> = (0..20)
            .map(|i| MarketDataEntry {
                utc_epoch_ns: i,
                spread: i as f64,
            })
            .collect();
        let mut bucket = Bucket::new(0, 20);
        for entry in market_data_entries {
            bucket.insert(entry);
        }

        assert_eq!(bucket.count_start_from(10), 10);
        let entries = bucket.get_start_from(10);
        let spreads: Vec<f64> = entries.iter().map(|entry| entry.spread).collect();
        let min_spread = *f64_min(&spreads).unwrap();
        let max_spread = *f64_max(&spreads).unwrap();

        assert_eq!(min_spread, 10.0);
        assert_eq!(max_spread, 19.0);
    }

    #[test]
    fn test_get_end_before() {
        let market_data_entries: Vec<MarketDataEntry> = (0..20)
            .map(|i| MarketDataEntry {
                utc_epoch_ns: i,
                spread: i as f64,
            })
            .collect();
        let mut bucket = Bucket::new(0, 20);
        for entry in market_data_entries {
            bucket.insert(entry);
        }

        assert_eq!(bucket.count_end_before(10), 11);
        let entries = bucket.get_end_before(10);
        let spreads: Vec<f64> = entries.iter().map(|entry| entry.spread).collect();
        let min_spread = *f64_min(&spreads).unwrap();
        let max_spread = *f64_max(&spreads).unwrap();

        assert_eq!(min_spread, 0.0);
        assert_eq!(max_spread, 10.0);
    }

    #[test]
    fn test_get_tdigest() {
        let market_data_entries: Vec<MarketDataEntry> = (0..20)
            .map(|i| MarketDataEntry {
                utc_epoch_ns: i,
                spread: i as f64,
            })
            .collect();
        let mut bucket = Bucket::new(0, 20);
        for entry in market_data_entries {
            bucket.insert(entry);
        }
        assert!(bucket.tdigest.borrow().is_none());
        let tdigest = bucket.get_tdigest();
        let ten_th = tdigest.estimate_quantile(0.1);
        assert_eq!(ten_th, 1.5);
        assert!(bucket.tdigest.borrow().is_some());
        bucket.insert(MarketDataEntry {
            utc_epoch_ns: 1,
            spread: 1.0,
        });
        assert!(bucket.tdigest.borrow().is_none());
    }
}
