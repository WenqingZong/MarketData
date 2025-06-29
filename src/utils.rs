// System libraries.
use log::warn;

// Third party libraries.
use serde_json::Value;

// Project libraries.
use crate::types::{BidAsk};

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

pub fn find_bucket_index(
    first_bucket_start_ns: u64,
    query_ns: u64,
    bucket_duration_ns: u64,
) -> Option<usize> {
    if query_ns < first_bucket_start_ns {
        // Query time is before the first bucket
        return None;
    }

    let elapsed_ns = query_ns - first_bucket_start_ns;
    let index = (elapsed_ns / bucket_duration_ns) as usize;
    Some(index)
}

pub fn calculate_ave_price(bidask: &Vec<BidAsk>) -> Option<f64> {
    let num = bidask.len();
    if num == 0 {
        return None
    }
    let sum: f64 = bidask.iter().map(|ba| ba.price).sum();
    Some(sum / num as f64)
}

pub fn f64_min(array: &Vec<f64>) -> Option<&f64> {
    array.iter().min_by(|a, b| a.partial_cmp(b).unwrap())
}

pub fn f64_max(array: &Vec<f64>) -> Option<&f64> {
    array.iter().max_by(|a, b| a.partial_cmp(b).unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_bid_ask_array() {
        let input_str = r#"
        [
            { "price": 1.0, "amount": 2.0 },
            { "price": 3.5, "amount": 4.5 },
            null,
            { "price": null, "amount": 5.0 },
            { "price": 6.0 },
            { "price": 7.0, "amount": 8.0 }
        ]
        "#;
        let input: Vec<Value> = serde_json::from_str(input_str).unwrap();

        let result = parse_bid_ask_array(&input);

        let expected = vec![
            BidAsk { price: 1.0, amount: 2.0 },
            BidAsk { price: 3.5, amount: 4.5 },
            BidAsk { price: 7.0, amount: 8.0 },
        ];

        assert_eq!(result, expected);
    }

    #[test]
    fn test_find_bucket_index() {
        let first_bucket_start_ns = 10;
        let bucket_duration_ns = 10;
        let inputs = vec![0_u64, 5, 10, 15, 20, 25, 30];
        let expected_outputs = vec![None, None, Some(0), Some(0), Some(1), Some(1), Some(2)];
        for (input, expected) in inputs.into_iter().zip(expected_outputs.into_iter()) {
            let output = find_bucket_index(first_bucket_start_ns, input, bucket_duration_ns);
            assert_eq!(output, expected);
        }
    }

    #[test]
    fn test_calculate_ave_price() {
        let input: Vec<BidAsk> = (1..=10).map(|price| BidAsk {price: price as f64, amount: 1.0}).collect();
        let output = calculate_ave_price(&input);
        assert_eq!(output, Some(5.5));
        assert_eq!(calculate_ave_price(&vec![]), None);
    }

    #[test]
    fn test_f64_max() {
        let input = vec![1.0, 2.0, 3.0];
        let max = f64_max(&input);
        assert_eq!(max, Some(&3.0));

        let input = vec![];
        let max = f64_max(&input);
        assert_eq!(max, None);
    }

        #[test]
    fn test_f64_min() {
        let input = vec![1.0, 2.0, 3.0];
        let min = f64_min(&input);
        assert_eq!(min, Some(&1.0));

        let input = vec![];
        let min = f64_min(&input);
        assert_eq!(min, None);
    }

}
