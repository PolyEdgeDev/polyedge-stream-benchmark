# polyedge-stream-benchmark

[PolyEdge](https://polyedge.dev) provides ultra-low-latency Polymarket mempool trade streaming & real-time on-chain analytics API, built for copy-trading bots, professional Polymarket traders, and prediction market builders to capture pending trades pre-block and track smart money with institutional-grade on-chain analytics.

This repository is an open-source latency benchmark suite designed to test and verify the delivery speed of the PolyEdge Polymarket trade stream against Polygon on-chain block time, Polymarket official RTDS, and PolyNode.

## Verify Yourself

```bash
git clone https://github.com/polyedge/polyedge-stream-benchmark.git
cd polyedge-stream-benchmark
node bin/cli.js
```

## Benchmark Results

| Comparison Target | PolyEdge Faster % | P50 Lead | P90 Lead | P99 Lead | Max Lead |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **PolyEdge vs On-Chain Block Time** | 100.0% | +2,038 ms ✓ | +2,924 ms ✓ | +3,293 ms ✓ | +3,411 ms ✓ |
| **PolyEdge vs Polymarket RTDS** | 100.0% | +1,747 ms ✓ | +2,534 ms ✓ | +2,944 ms ✓ | +3,047 ms ✓ |
| **PolyEdge vs PolyNode WS** | 98.9% | +25 ms ✓ | +42 ms ✓ | +62 ms ✓ | +245 ms ✓ |

*\* Positive lead time (+) with ✓ indicates PolyEdge delivered the trade faster than the target.*
