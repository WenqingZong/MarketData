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

    pub fn insert(&mut self, market_data_entry: MarketDataEntry) {
        // We'll use lazy calculation here.
        self.tdigest = None;
        self.count += 1;
        let spread = market_data_entry.spread;
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
        dbg!();
        let tdigest = TDigest::new_with_size(self.entries.len());
        let spreads = self.entries.iter().map(|entry| entry.spread).collect();
        tdigest.merge_unsorted(spreads);
        self.tdigest = Some(tdigest.clone());
        tdigest
    }
}
