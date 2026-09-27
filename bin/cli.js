#!/usr/bin/env node

/**
 * PolyEdge Stream Benchmark CLI
 * ---------------------------------------------------
 * Verified empirical latency benchmarks for Polymarket trade streaming:
 *   - Mode 'all'       : PolyEdge vs On-Chain Block Time, RTDS, and PolyNode
 *   - Mode 'blocktime' : PolyEdge vs Polygon On-Chain Block Time
 *   - Mode 'rtds'      : PolyEdge vs Polymarket Official RTDS WebSocket
 *   - Mode 'polynode'  : PolyEdge vs PolyNode Settlement WebSocket
 *
 * Methodology:
 *   Phase 1: Warmup   (default 3s) - Establishes sockets and flushes handshake backlogs
 *   Phase 2: Sampling (default 15s) - Active cohort observation window
 *   Phase 3: Drain    (default 3s) - Catches delayed arrivals on slower streams (anti-truncation)
 */

import { subscribePolyEdgeStream, DEFAULT_STREAM_ID, DEFAULT_POLYEDGE_KEY } from '../lib/polyedge.js';
import { resolveBlockTimes } from '../lib/block_time.js';
import { startRtdsStream, DEFAULT_RTDS_URL } from '../lib/rtds.js';
import { startPolyNodeStream, getOrCreatePolynodeKey, DEFAULT_POLYNODE_URL } from '../lib/polynode.js';
import { computeStats } from '../lib/stats.js';
import { colors, renderTable, formatLead } from '../lib/format.js';

const args = process.argv.slice(2);

// Parse CLI Flags
let mode = 'all'; // 'all' | 'blocktime' | 'rtds' | 'polynode'
let warmupSec = 3;
let durationSec = 15;
let drainSec = 3;
let streamId = process.env.POLYEDGE_STREAM_ID || DEFAULT_STREAM_ID;
let polyedgeKey = process.env.POLYEDGE_KEY || process.env.POLYEDGE_API_KEY || DEFAULT_POLYEDGE_KEY;
let customPolynodeKey = process.env.POLYNODE_API_KEY || null;
let customRpc = process.env.POLYGON_RPC || null;

for (let i = 0; i < args.length; i++) {
  const arg = args[i];
  if (arg === '--help' || arg === '-h') {
    printHelp();
    process.exit(0);
  } else if ((arg === '--mode' || arg === '-m') && i + 1 < args.length) {
    const val = args[++i].toLowerCase();
    if (!['all', 'blocktime', 'rtds', 'polynode'].includes(val)) {
      console.error(`${colors.red}Invalid --mode: "${val}". Must be "all", "blocktime", "rtds", or "polynode".${colors.reset}`);
      process.exit(1);
    }
    mode = val;
  } else if ((arg === '--warmup' || arg === '-w') && i + 1 < args.length) {
    warmupSec = Math.max(0, parseInt(args[++i], 10) || 0);
  } else if ((arg === '--duration' || arg === '-d') && i + 1 < args.length) {
    durationSec = Math.max(1, parseInt(args[++i], 10) || 15);
  } else if ((arg === '--drain' || arg === '-c') && i + 1 < args.length) {
    drainSec = Math.max(0, parseInt(args[++i], 10) || 0);
  } else if (arg === '--stream-id' && i + 1 < args.length) {
    streamId = args[++i];
  } else if (arg === '--polyedge-key' && i + 1 < args.length) {
    polyedgeKey = args[++i];
  } else if (arg === '--polynode-key' && i + 1 < args.length) {
    customPolynodeKey = args[++i];
  } else if (arg === '--rpc' && i + 1 < args.length) {
    customRpc = args[++i];
  }
}

const streamUrl = `https://stream.polyedge.dev/streams/${streamId}`;

function printHelp() {
  console.log(`
${colors.bold}PolyEdge Stream Latency Benchmark${colors.reset}

Usage:
  node bin/cli.js [options]

Options:
  --mode, -m <mode>         Benchmark mode: "all" (default), "blocktime", "rtds", or "polynode"
  --duration, -d <sec>      Active sampling duration in seconds (default: 15)
  --warmup, -w <sec>        Warmup duration to flush handshake backlog (default: 3)
  --drain, -c <sec>         Drain duration for slower streams to catch up (default: 3)
  --stream-id <id>          PolyEdge stream ID (default: ${DEFAULT_STREAM_ID})
  --polyedge-key <key>      PolyEdge API Key (header: X-PolyEdge-Key)
  --polynode-key <key>      Custom PolyNode API Key (auto-requested dynamically if omitted)
  --rpc <url>               Custom Polygon EVM JSON-RPC URL
  --help, -h                Display this help message

Environment Variables:
  POLYEDGE_STREAM_ID        PolyEdge stream ID
  POLYEDGE_KEY              PolyEdge API Key
  POLYNODE_API_KEY          Custom PolyNode API Key
  POLYGON_RPC               Polygon EVM RPC endpoint

Examples:
  node bin/cli.js
  node bin/cli.js --mode blocktime --duration 20
  node bin/cli.js --mode rtds --duration 20
  node bin/cli.js --mode polynode --duration 20
  node bin/cli.js --warmup 5 --duration 30 --drain 5
`);
}

async function runBenchmark() {
  const needsRtds = mode === 'all' || mode === 'rtds';
  const needsPolynode = mode === 'all' || mode === 'polynode';
  const needsBlocktime = mode === 'all' || mode === 'blocktime';

  let polynodeKey = '';
  if (needsPolynode) {
    polynodeKey = await getOrCreatePolynodeKey(customPolynodeKey);
    if (!customPolynodeKey) {
      console.log(`${colors.cyan}🔑 PolyNode API Key: ${colors.reset}${polynodeKey}\n`);
    }
  }

  const startMs = Date.now();
  const benchmarkStartMs = startMs + warmupSec * 1000;
  const benchmarkEndMs = benchmarkStartMs + durationSec * 1000;

  const controller = new AbortController();
  const polyedgeRecords = new Map();
  const rtdsRecords = new Map();
  const polynodeRecords = new Map();

  const benchmarkCohort = new Set();
  const warmupTxs = new Set();

  // 1. Subscribe to PolyEdge SSE Stream
  const polyedgePromise = subscribePolyEdgeStream(
    streamUrl,
    polyedgeKey,
    (event) => {
      const now = Date.now();
      const isWarmup = now < benchmarkStartMs;
      const isBenchmark = now >= benchmarkStartMs && now < benchmarkEndMs;

      if (!polyedgeRecords.has(event.txHash)) {
        polyedgeRecords.set(event.txHash, {
          ...event,
          phase: isWarmup ? 'warmup' : (isBenchmark ? 'benchmark' : 'drain')
        });

        if (isWarmup) {
          warmupTxs.add(event.txHash);
        } else if (isBenchmark) {
          if (!warmupTxs.has(event.txHash)) {
            benchmarkCohort.add(event.txHash);
            process.stdout.write(`${colors.green}.${colors.reset}`);
          }
        }
      }
    },
    controller.signal
  ).catch((err) => {
    console.log(`\n${colors.yellow}⚠️  PolyEdge stream note: ${err.message}${colors.reset}`);
  });

  // 2. Subscribe to RTDS WebSocket if needed
  let stopRtds = null;
  if (needsRtds) {
    stopRtds = startRtdsStream(DEFAULT_RTDS_URL, (event) => {
      const now = Date.now();
      const isWarmup = now < benchmarkStartMs;
      const isBenchmark = now >= benchmarkStartMs && now < benchmarkEndMs;

      if (!rtdsRecords.has(event.txHash)) {
        rtdsRecords.set(event.txHash, {
          ...event,
          phase: isWarmup ? 'warmup' : (isBenchmark ? 'benchmark' : 'drain')
        });

        if (isWarmup) {
          warmupTxs.add(event.txHash);
        } else if (isBenchmark) {
          const pe = polyedgeRecords.get(event.txHash);
          const isWarmupInPE = pe && pe.phase === 'warmup';
          if (!warmupTxs.has(event.txHash) && !isWarmupInPE) {
            benchmarkCohort.add(event.txHash);
          }
        }
      }
    });
  }

  // 3. Subscribe to PolyNode WebSocket if needed
  let stopPolyNode = null;
  if (needsPolynode) {
    stopPolyNode = startPolyNodeStream(DEFAULT_POLYNODE_URL, polynodeKey, (event) => {
      const now = Date.now();
      const isWarmup = now < benchmarkStartMs;
      const isBenchmark = now >= benchmarkStartMs && now < benchmarkEndMs;

      if (!polynodeRecords.has(event.txHash)) {
        polynodeRecords.set(event.txHash, {
          ...event,
          phase: isWarmup ? 'warmup' : (isBenchmark ? 'benchmark' : 'drain')
        });

        if (isWarmup) {
          warmupTxs.add(event.txHash);
        } else if (isBenchmark) {
          const pe = polyedgeRecords.get(event.txHash);
          const isWarmupInPE = pe && pe.phase === 'warmup';
          if (!warmupTxs.has(event.txHash) && !isWarmupInPE) {
            benchmarkCohort.add(event.txHash);
          }
        }
      }
    });
  }

  // Pipeline Execution
  if (warmupSec > 0) {
    console.log(`${colors.cyan}⏳ [1/3] Phase 1 Warmup: Flushing socket backlog for ${warmupSec}s...${colors.reset}`);
    await new Promise((r) => setTimeout(r, warmupSec * 1000));
  }

  console.log(`\n${colors.cyan}🚀 [2/3] Phase 2 Active Benchmark: Registering live cohort trades for ${durationSec}s...${colors.reset}`);
  await new Promise((r) => setTimeout(r, durationSec * 1000));

  if (drainSec > 0 && (needsRtds || needsPolynode)) {
    console.log(`\n${colors.cyan}⏳ [3/3] Phase 3 Drain: Waiting ${drainSec}s for slower peer streams to catch up...${colors.reset}`);
    await new Promise((r) => setTimeout(r, drainSec * 1000));
  }

  // Stop all streams
  controller.abort();
  if (stopRtds) stopRtds();
  if (stopPolyNode) stopPolyNode();

  // Filter cohort results
  const cohortPE = new Map();
  const cohortRTDS = new Map();
  const cohortPN = new Map();

  for (const txHash of benchmarkCohort) {
    const pe = polyedgeRecords.get(txHash);
    if (pe) cohortPE.set(txHash, pe);

    const rt = rtdsRecords.get(txHash);
    if (rt) cohortRTDS.set(txHash, rt);

    const pn = polynodeRecords.get(txHash);
    if (pn) cohortPN.set(txHash, pn);
  }

  if (cohortPE.size === 0) {
    console.log(`\n${colors.yellow}⚠️  No trades captured during the active test window.${colors.reset}`);
    console.log(`${colors.gray}   Please increase observation duration (e.g. --duration 20) or check network connectivity.${colors.reset}\n`);
    return;
  }

  // =========================================================================
  // UNIFIED BENCHMARK RESULTS TABLE
  // =========================================================================
  const unifiedRows = [];

  // 1. On-Chain Block Time Lead
  // lead = block_timestamp - stream_timestamp (positive = PolyEdge arrives BEFORE block confirmation)
  if (needsBlocktime) {
    console.log(`\n${colors.cyan}🔍 Resolving Polygon on-chain block timestamps via EVM RPC...${colors.reset}`);
    const sampleTxs = Array.from(cohortPE.keys()).slice(0, 30);
    const blockResults = await resolveBlockTimes(sampleTxs, customRpc);

    const blockLeadDiffs = [];
    for (const [txHash, blockInfo] of blockResults.entries()) {
      const peEvent = cohortPE.get(txHash);
      if (peEvent && blockInfo.blockTimestampMs) {
        blockLeadDiffs.push(blockInfo.blockTimestampMs - peEvent.serverTimestamp);
      }
    }

    if (blockLeadDiffs.length > 0) {
      const stats = computeStats(blockLeadDiffs);
      const preBlockCount = blockLeadDiffs.filter(d => d > 0).length;
      const winPct = ((preBlockCount / blockLeadDiffs.length) * 100).toFixed(1);

      unifiedRows.push([
        'PolyEdge vs On-Chain Block Time',
        colors.green + `${winPct}%` + colors.reset,
        formatLead(stats.p50),
        formatLead(stats.p90),
        formatLead(stats.p99),
        formatLead(stats.max)
      ]);
    }
  }

  // 2. Polymarket RTDS Comparison
  // lead = rtds_arrival - pe_arrival (positive = PolyEdge arrived FASTER than RTDS)
  if (needsRtds) {
    const rtdsDiffs = [];
    let peFasterRtds = 0;

    for (const [txHash, peEvent] of cohortPE.entries()) {
      const rtdsEvent = cohortRTDS.get(txHash);
      if (rtdsEvent) {
        const diff = rtdsEvent.receivedAt - peEvent.receivedAt;
        rtdsDiffs.push(diff);
        if (diff > 0) peFasterRtds++;
      }
    }

    if (rtdsDiffs.length > 0) {
      const stats = computeStats(rtdsDiffs);
      const winPct = ((peFasterRtds / rtdsDiffs.length) * 100).toFixed(1);

      unifiedRows.push([
        'PolyEdge vs Polymarket RTDS',
        colors.green + `${winPct}%` + colors.reset,
        formatLead(stats.p50),
        formatLead(stats.p90),
        formatLead(stats.p99),
        formatLead(stats.max)
      ]);
    }
  }

  // 3. PolyNode WS Comparison
  // lead = pn_arrival - pe_arrival (positive = PolyEdge arrived FASTER than PolyNode)
  if (needsPolynode) {
    const pnDiffs = [];
    let peFasterPn = 0;

    for (const [txHash, peEvent] of cohortPE.entries()) {
      const pnEvent = cohortPN.get(txHash);
      if (pnEvent) {
        const diff = pnEvent.receivedAt - peEvent.receivedAt;
        pnDiffs.push(diff);
        if (diff > 0) peFasterPn++;
      }
    }

    if (pnDiffs.length > 0) {
      const stats = computeStats(pnDiffs);
      const winPct = ((peFasterPn / pnDiffs.length) * 100).toFixed(1);

      unifiedRows.push([
        'PolyEdge vs PolyNode WS',
        colors.green + `${winPct}%` + colors.reset,
        formatLead(stats.p50),
        formatLead(stats.p90),
        formatLead(stats.p99),
        formatLead(stats.max)
      ]);
    }
  }

  console.log(`\n${colors.bold}=========================================================================================${colors.reset}`);
  console.log(`${colors.bold} 📊 BENCHMARK RESULTS SUMMARY${colors.reset}`);
  console.log(`${colors.bold}=========================================================================================${colors.reset}`);

  if (unifiedRows.length > 0) {
    renderTable(
      ['Comparison Target', 'PolyEdge Win Rate', 'P50 Lead', 'P90 Lead', 'P99 Lead', 'Max Lead'],
      unifiedRows
    );
    console.log(`\n${colors.gray}* Positive lead time (+) with ✓ indicates PolyEdge delivered the trade faster than the target.${colors.reset}`);
  } else {
    console.log(`${colors.yellow}   No common cohort overlap detected in this active window.${colors.reset}`);
  }

  console.log(`\n${colors.cyan}✨ Benchmark finished successfully.${colors.reset}\n`);
}

runBenchmark().catch((err) => {
  console.error(`\n${colors.red}Benchmark execution error: ${err.message}${colors.reset}`);
  process.exit(1);
});
