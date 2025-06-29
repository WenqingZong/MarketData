// Third party libraries.
use tdigest::TDigest;

// Project libraries.
use crate::types::{Bucket, MarketDataEntry};
use crate::utils::{f64_max, f64_min};

impl Bucket {
    pub fn new(start_time_ns: u64, end_time_ns: u64) -> Self {
        Self {
            start_time_ns,
            end_time_ns,
            count: 0,
            tdigest: None,
            min_spread: f64::MAX,
            max_spread: f64::MIN,
            entries: Vec::new(),
        }
    }

    pub fn insert(&mut self, market_data_entry: MarketDataEntry) -> bool {
        // A quick check the new data indeed belongs to this bucket.
        if !(self.start_time_ns <= market_data_entry.utc_epoch_ns && market_data_entry.utc_epoch_ns < self.end_time_ns) {
            return false
        }
        // We'll use lazy calculation here.
        self.tdigest = None;
        self.count += 1;
        let spread = market_data_entry.spread;
        self.min_spread = self.min_spread.min(spread);
        self.max_spread = self.max_spread.max(spread);
        self.entries.push(market_data_entry);
        true
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
            .map(|entry| entry.spread)
            .filter(|v| v.is_finite()) // 过滤 NaN、inf
            .collect();

        self.min_spread = *f64_min(&spreads).unwrap();
        self.max_spread = *f64_max(&spreads).unwrap(); // self.max_spread = self.entries.iter().max();
        self.tdigest = None;
    }

    pub fn get_start_from(&self, start_time_ns: u64) -> Vec<&MarketDataEntry> {
        if self.start_time_ns <= start_time_ns && start_time_ns <= self.end_time_ns {
            self.entries
                .iter()
                .filter(|entry| entry.utc_epoch_ns >= start_time_ns)
                .collect()
        } else {
            Vec::new()
        }
    }

    pub fn count_start_from(&self, start_time_ns: u64) -> usize {
        self.get_start_from(start_time_ns).len()
    }

    pub fn get_end_before(&self, end_time_ns: u64) -> Vec<&MarketDataEntry> {
        if self.start_time_ns <= end_time_ns && end_time_ns <= self.end_time_ns {
            self.entries
                .iter()
                .filter(|entry| entry.utc_epoch_ns <= end_time_ns)
                .collect()
        } else {
            Vec::new()
        }
    }

    pub fn count_end_before(&self, end_time_ns: u64) -> usize {
        self.get_end_before(end_time_ns).len()
    }

    pub fn get_tdigest(&mut self) -> TDigest {
        if let Some(ref tdigest) = self.tdigest {
            return tdigest.clone();
        }

        // Lazy calculation here, because adding each spread iteratively is time consuming, so best to do batch processing.
        let mut tdigest = TDigest::new_with_size(self.entries.len());
        let spreads = self.entries.iter().map(|entry| entry.spread).collect();
        dbg!(&spreads);
        tdigest = tdigest.merge_unsorted(spreads);
        self.tdigest = Some(tdigest.clone());
        tdigest
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
        assert!(bucket.tdigest.is_none());
    }

    #[test]
    fn test_new_bucket() {
        let bucket = Bucket::new(10, 100);
        assert_eq!(bucket.count, 0);
        assert_eq!(bucket.start_time_ns, 10);
        assert_eq!(bucket.end_time_ns, 100);
        assert!(bucket.tdigest.is_none());
    }

    #[test]
    fn test_insert() {
        let market_data_entries: Vec<MarketDataEntry> = (0..20).map(|i| MarketDataEntry {
            utc_epoch_ns: i,
            spread: i as f64,
        }).collect();
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
        assert!(bucket.tdigest.is_none());
    }

    #[test]
    fn test_remove_up_to() {
        let market_data_entries: Vec<MarketDataEntry> = (0..20).map(|i| MarketDataEntry {
            utc_epoch_ns: i,
            spread: i as f64,
        }).collect();
        let mut bucket = Bucket::new(5, 20);
        for entry in market_data_entries {
            bucket.insert(entry);
        }
        assert_eq!(bucket.count, 15);

        bucket.remove_up_to(30);
        assert_eq!(bucket.count, 15);

        bucket.remove_up_to(3);
        assert_eq!(bucket.count, 15);

        bucket.remove_up_to(10);
        assert_eq!(bucket.count, 9);
        assert_eq!(bucket.max_spread, 19.0);
        assert_eq!(bucket.min_spread, 11.0);
        assert!(bucket.tdigest.is_none());

    }

    #[test]
    fn test_get_start_from() {
        let market_data_entries: Vec<MarketDataEntry> = (0..20).map(|i| MarketDataEntry {
            utc_epoch_ns: i,
            spread: i as f64,
        }).collect();
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
        let market_data_entries: Vec<MarketDataEntry> = (0..20).map(|i| MarketDataEntry {
            utc_epoch_ns: i,
            spread: i as f64,
        }).collect();
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
        let market_data_entries: Vec<MarketDataEntry> = (0..20).map(|i| MarketDataEntry {
            utc_epoch_ns: i,
            spread: i as f64,
        }).collect();
        let mut bucket = Bucket::new(0, 20);
        for entry in market_data_entries {
            bucket.insert(entry);
        }
        assert!(bucket.tdigest.is_none());
        let tdigest = bucket.get_tdigest();
        let ten_th = tdigest.estimate_quantile(0.1);
        assert_eq!(ten_th, 1.5);
        assert!(bucket.tdigest.is_some());
        bucket.insert(MarketDataEntry {
            utc_epoch_ns: 1,
            spread: 1.0
        });
        assert!(bucket.tdigest.is_none());
    }
}