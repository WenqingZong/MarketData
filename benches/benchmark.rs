use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use market_data::{MarketDataCache, MarketDataEntry};
use rand::Rng;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

const NUM_BUCKETS: usize = 36000; // 1 hour data
const BUCKET_NS: u64 = 100_000_000; // 100ms

// Generate random market data entries
fn generate_random_entry(time_offset: u64) -> MarketDataEntry {
    let mut rng = rand::thread_rng();
    // 生成合理的买卖价差 (0.1-10.0)
    let spread = rng.gen_range(0.1..10.0);

    MarketDataEntry {
        utc_epoch_ns: time_offset,
        spread,
    }
}

// Initialize our cache
fn setup_test_cache(num_entries: usize) -> MarketDataCache {
    let mut cache = MarketDataCache::new(NUM_BUCKETS, BUCKET_NS);
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    // Generate random market data entries.
    for i in 0..num_entries {
        let time_offset = now - (num_entries as u64 - i as u64) * BUCKET_NS;
        let entry = generate_random_entry(time_offset);
        cache.insert(entry);
    }

    cache
}

fn insert_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("Insert Operations");

    // With different data size.
    for size in [100, 1_000, 10_000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::new("insert", size), size, |b, &size| {
            let mut cache = setup_test_cache(0);
            let entries: Vec<MarketDataEntry> = (0..size)
                .map(|i| generate_random_entry(i as u64 * BUCKET_NS))
                .collect();

            b.iter(|| {
                for entry in &entries {
                    cache.insert(entry.clone());
                }
            });
        });
    }

    group.finish();
}

fn query_benchmarks(c: &mut Criterion) {
    // With different data set size.
    let datasets = [("small", 1_000), ("medium", 10_000)];

    for (name, size) in datasets.iter() {
        let cache = Arc::new(setup_test_cache(*size));
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        // test range：the last 1 min
        let start_time = now - 60_000_000_000;
        let end_time = now;

        let mut group = c.benchmark_group(format!("Query Operations - {} dataset", name));
        group.sample_size(100);

        group.bench_function("count_range", |b| {
            let cache = cache.clone();
            b.iter(|| cache.count_range(start_time, end_time));
        });

        group.bench_function("min_spread", |b| {
            let cache = cache.clone();
            b.iter(|| cache.min_spread(start_time, end_time));
        });

        group.bench_function("max_spread", |b| {
            let cache = cache.clone();
            b.iter(|| cache.max_spread(start_time, end_time));
        });

        group.bench_function("spread_percentiles", |b| {
            let cache = cache.clone();
            b.iter(|| cache.spread_percentiles(start_time, end_time));
        });

        group.finish();
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(3))
        .measurement_time(Duration::from_secs(10));
    targets =
        insert_benchmarks,
        query_benchmarks,
}

criterion_main!(benches);
