/**
 * Polygon On-Chain EVM RPC Block Time Resolver
 */

export const DEFAULT_RPC_LIST = [
  'https://polygon-bor-rpc.publicnode.com',
  'https://polygon.drpc.org',
  'https://1rpc.io/matic',
  'https://tenderly.rpc.polygon.community'
];

/**
 * Fetch transaction receipt & block timestamp from Polygon RPC
 */
export async function resolveBlockTimes(txHashes, customRpc) {
  const rpcUrls = customRpc ? [customRpc, ...DEFAULT_RPC_LIST] : DEFAULT_RPC_LIST;
  const results = new Map(); // txHash -> { blockNumber, blockTimestampMs }
  const blockTimeCache = new Map(); // blockNumber (hex) -> blockTimestampMs

  const BATCH_SIZE = 12;

  for (let i = 0; i < txHashes.length; i += BATCH_SIZE) {
    const chunk = txHashes.slice(i, i + BATCH_SIZE);
    await resolveChunk(chunk, rpcUrls, results, blockTimeCache);
  }

  return results;
}

async function resolveChunk(chunk, rpcUrls, results, blockTimeCache) {
  // Step 1: Batch eth_getTransactionByHash
  const txRequests = chunk.map((txHash, index) => ({
    jsonrpc: '2.0',
    id: index + 1,
    method: 'eth_getTransactionByHash',
    params: [txHash]
  }));

  let txResponses = null;
  let activeRpc = rpcUrls[0];

  for (const rpcUrl of rpcUrls) {
    try {
      const res = await fetch(rpcUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(txRequests),
        signal: AbortSignal.timeout(6000)
      });
      if (res.ok) {
        txResponses = await res.json();
        activeRpc = rpcUrl;
        break;
      }
    } catch (_) {}
  }

  if (!Array.isArray(txResponses)) return;

  // Map tx to blockNumber
  const txToBlock = new Map();
  const neededBlocks = new Set();

  for (const item of txResponses) {
    if (item && item.result && item.result.blockNumber && item.result.hash) {
      const hash = item.result.hash.toLowerCase();
      const blockNum = item.result.blockNumber;
      txToBlock.set(hash, blockNum);

      if (!blockTimeCache.has(blockNum)) {
        neededBlocks.add(blockNum);
      }
    }
  }

  // Step 2: Query block timestamps for missing blocks
  if (neededBlocks.size > 0) {
    const blockRequests = Array.from(neededBlocks).map((blockNum, idx) => ({
      jsonrpc: '2.0',
      id: idx + 100,
      method: 'eth_getBlockByNumber',
      params: [blockNum, false]
    }));

    try {
      const res = await fetch(activeRpc, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(blockRequests),
        signal: AbortSignal.timeout(6000)
      });
      if (res.ok) {
        const blockResponses = await res.json();
        if (Array.isArray(blockResponses)) {
          for (const item of blockResponses) {
            if (item && item.result && item.result.number && item.result.timestamp) {
              const bNum = item.result.number;
              const tsSec = parseInt(item.result.timestamp, 16);
              blockTimeCache.set(bNum, tsSec * 1000);
            }
          }
        }
      }
    } catch (_) {}
  }

  // Step 3: Populate results
  for (const [txHash, blockNum] of txToBlock.entries()) {
    const blockTs = blockTimeCache.get(blockNum);
    if (blockTs) {
      results.set(txHash, {
        blockNumber: parseInt(blockNum, 16),
        blockTimestampMs: blockTs
      });
    }
  }
}
