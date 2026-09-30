/**
 * PolyNode WebSocket Client
 * Protocol: WebSocket (wss://ws.polynode.dev/ws)
 * Dynamic API Key Provisioning via POST https://api.polynode.dev/v1/keys
 */

import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

export const DEFAULT_POLYNODE_URL = 'wss://ws.polynode.dev/ws';
const KEY_CACHE_FILE = path.join(os.tmpdir(), 'polynode_api_key.json');
const KEY_CACHE_TTL_MS = 60 * 60 * 1000; // 1 hour

/**
 * Obtain a PolyNode API key dynamically with 1-hour local cache
 *
 * @param {string} [specifiedKey]
 * @returns {Promise<string>}
 */
export async function getOrCreatePolynodeKey(specifiedKey) {
  if (specifiedKey) return specifiedKey;
  if (process.env.POLYNODE_API_KEY) return process.env.POLYNODE_API_KEY;

  // 1. Check if unexpired cached key exists (< 1 hour)
  if (fs.existsSync(KEY_CACHE_FILE)) {
    try {
      const data = JSON.parse(fs.readFileSync(KEY_CACHE_FILE, 'utf-8'));
      const cachedAt =
        typeof data.cached_at === 'number'
          ? data.cached_at
          : fs.statSync(KEY_CACHE_FILE).mtimeMs || 0;

      if (data && data.api_key && Date.now() - cachedAt < KEY_CACHE_TTL_MS) {
        return data.api_key;
      }
    } catch (_) {}
  }

  // 2. Request a fresh key if missing or expired
  try {
    const res = await fetch('https://api.polynode.dev/v1/keys', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: 'my-app' })
    });

    if (res.ok) {
      const data = await res.json();
      if (data && data.api_key) {
        try {
          fs.writeFileSync(
            KEY_CACHE_FILE,
            JSON.stringify({ ...data, cached_at: Date.now() }),
            'utf-8'
          );
        } catch (_) {}
        return data.api_key;
      }
    }
  } catch (_) {}

  throw new Error('Failed to obtain PolyNode API key (rate limited or expired). Please pass --polynode-key <key>.');
}

/**
 * Connect to PolyNode WebSocket (/ws settlements pending)
 *
 * @param {string} [wsUrl]
 * @param {string} apiKey
 * @param {(event: { source: 'polynode', txHash: string, receivedAt: number, serverTimestamp: number, payload: any }) => void} onEvent
 * @returns {() => void} unsubscribe / close function
 */
export function startPolyNodeStream(wsUrl, apiKey, onEvent) {
  if (!apiKey) {
    throw new Error('startPolyNodeStream requires a valid PolyNode API key');
  }

  let ws = null;
  let pingInterval = null;

  const fullUrl = `${wsUrl || DEFAULT_POLYNODE_URL}?key=${encodeURIComponent(apiKey)}`;

  try {
    ws = new WebSocket(fullUrl);

    ws.onopen = () => {
      ws.send(JSON.stringify({
        action: 'subscribe',
        type: 'settlements',
        filters: { status: 'pending', snapshot_count: 0 }
      }));

      // PolyNode keepalive ping
      pingInterval = setInterval(() => {
        if (ws && ws.readyState === WebSocket.OPEN) {
          ws.send('ping');
        }
      }, 5000);
    };

    ws.onmessage = (e) => {
      const recvAt = Date.now();
      if (typeof e.data !== 'string' || !e.data.startsWith('{')) return;

      try {
        const msg = JSON.parse(e.data);
        if (msg.type === 'settlement' && msg.data && msg.data.status === 'pending') {
          const txHash = (msg.data.tx_hash || '').toLowerCase();
          if (!txHash || !txHash.startsWith('0x')) return;

          const serverTimestamp = msg.data.detected_at || msg.timestamp || recvAt;

          onEvent({
            source: 'polynode',
            txHash,
            receivedAt: recvAt,
            serverTimestamp,
            payload: msg.data
          });
        }
      } catch (_) {}
    };

    ws.onerror = () => {};
  } catch (_) {}

  return () => {
    if (pingInterval) clearInterval(pingInterval);
    try {
      if (ws) ws.close();
    } catch (_) {}
  };
}
