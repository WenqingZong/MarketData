# MarketData
A fast utility to analyze the latest MarketData within the last hour

## General Idea
we keep a bucket that will hold all market data in 100ms, and provide some cache result for each 100ms time period. Then our cache is just a deque of continuous buckets. Note that deque has O(1) performance on pop front, push back, and indexing.

It's highly likely that our actual query need to take multiple buckets into consideration, we will handle each query in three parts:
1. The bucket that contains start time, get everything in this bucket that happens after start time.
2. The buckets in the middle of start to end. These are whole buckets, and their result are already calculated and cached in themselves.
3. The bucket that contains end time. get everything in this bucket that happens before end time.

## Multi-thread
Each Bucket is warped in a RwLock for multi-threading processing, and also, `rayon` is used to handle part 2 in the above paragraph, as each bucket process is logically independent and thus can be paralleled. 

## Env
Code is tested in Window 11, with `cargo 1.88.0 (873a06493 2025-05-10)`.

You can just do a `cargo run` to play with the sample data, or `cargo test` to see all of the unit tests. 

## TDigest
For calculating percentiles, I used a third party library, `tdigest`. It's believed to provide a good performance even with streaming input. However, my experiments shows that streaming calculation is a bit slower than off-line processing, so in my implementation, all tdigest calculation are done in a lazy manner: Nothing is calculated/updated while inserting new data into bucket, it's only calculated and get cached when asked for the result. 

It's also worth mentioning that TDigest algorithm comes with a small amount error. One hyper parameter in TDigest is the number of centroids, the more centroids, the higher accuracy, and also the longer computing time. In my implementation, a magic number `100` is used, which is also the default choice to balance speed and accuracy. Also, in General Idea step 2, I used the cached tdigest rather than recalculate from raw market data entries to avoid wasting time, but this double compression does come with an accuracy penalty. 

## Documentation
`cargo doc --open`

## Future Work
I believe the current solution could be further optimized, my idea is to use multiple layer of buckets, for example, we now have 100ms buckets, we can build on top of it with 1min buckets, this can further reduce our calculations theoretically, especially for handling step 2 in General Idea section. 
