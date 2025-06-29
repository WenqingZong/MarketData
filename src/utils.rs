// use rust_decimal::Decimal;
use crate::market_data::BidAsk;
use log::warn;
use serde_json::Value;

// All data are financial data so best to use Decimal rather than f64 to represent them.
// pub fn decimal_from_json<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
// where
//     D: serde::Deserializer<'de>,
// {
//     rust_decimal::serde::arbitrary_precision::deserialize(deserializer)
// }

pub fn parse_bid_ask_array(arr: &[Value]) -> Vec<BidAsk> {
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

pub fn find_bucket_index(first_bucket_start_ns: u64, query_ns: u64, bucket_duration_ns: u64) -> Option<usize> {
    if query_ns < first_bucket_start_ns {
        // Query time is before the first bucket
        return None;
    }

    let elapsed_ns = query_ns - first_bucket_start_ns;
    let index = (elapsed_ns / bucket_duration_ns) as usize;
    Some(index)
}