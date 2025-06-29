use log::{debug, info, warn};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;
use std::fs::File;
use std::io::BufReader;

fn decimal_from_json<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: serde::Deserializer<'de>,
{
    rust_decimal::serde::arbitrary_precision::deserialize(deserializer)
}

#[derive(Debug, Deserialize)]
pub struct BidAsk {
    #[serde(deserialize_with = "decimal_from_json")]
    pub price: Decimal,

    #[serde(deserialize_with = "decimal_from_json")]
    pub amount: Decimal,
}

#[derive(Debug, Deserialize)]
pub struct MarketDataEntry {
    utc_epoch_ns: u64,
    bids: Vec<BidAsk>,
    asks: Vec<BidAsk>,
}

#[derive(Debug, Deserialize)]
pub struct MarketDataCache {
    pub market_data_entries: Vec<MarketDataEntry>,
}

impl MarketDataCache {
    pub fn new() -> Self {
        unimplemented!()
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
            let bids = match entry.get("bids") {
                Some(Value::Array(arr)) => parse_bid_ask_array(arr),
                _ => {
                    warn!("Skipping entry due to missing or invalid bids array");
                    Vec::new()
                }
            };

            // Handle asks.
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
        Self {
            market_data_entries,
        }
    }

    // Insert an entry into the cache.
    pub fn insert(&mut self, data: &Self) {
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

fn parse_bid_ask_array(arr: &[Value]) -> Vec<BidAsk> {
    let mut result = Vec::new();
    for item in arr {
        // Skip if entry is not an object.
        let obj = match item.as_object() {
            Some(o) => o,
            None => {
                warn!("Skipping bid/ask entry due to non-object entry in bid/ask array");
                continue;
            }
        };

        // Check if BOTH price and amount exist and are not null.
        let is_valid = obj.get("price").map_or(false, |v| !v.is_null())
            && obj.get("amount").map_or(false, |v| !v.is_null());

        if !is_valid {
            warn!("Skipping bid/ask entry due to missing or null for price/amount");
            continue;
        }

        // The actual deserialize.
        match serde_json::from_value::<BidAsk>(item.clone()) {
            Ok(bid_ask) => result.push(bid_ask),
            Err(e) => warn!("Skipping bid/ask entry due to deserialization error: {}", e),
        }
    }
    result
}