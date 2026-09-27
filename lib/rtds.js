/**
 * Polymarket RTDS (Real-Time Data Streaming) Client
 * Protocol: WebSocket (wss://ws-live-data.polymarket.com)
 */

export const DEFAULT_RTDS_URL = 'wss://ws-live-data.polymarket.com';

/**
 * Connect to Polymarket RTDS WebSocket
 * Subscribes to activity topic ('trades' and 'orders_matched')
 *
 * @param {string} [wsUrl]
 * @param {(event: { source: 'rtds', txHash: string, receivedAt: number, serverTimestamp: number, payload: any }) => void} onEvent
 * @returns {() => void} unsubscribe / close function
 */
export function startRtdsStream(wsUrl, onEvent) {
  let ws = null;
  let pingInterval = null;

  try {
    ws = new WebSocket(wsUrl || DEFAULT_RTDS_URL);

    ws.onopen = () => {
      ws.send(JSON.stringify({
        action: 'subscribe',
        subscriptions: [
          { topic: 'activity', type: 'trades' },
          { topic: 'activity', type: 'orders_matched' }
        ]
      }));

      // RTDS keepalive ping
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
        const payload = msg.payload || msg.data;
        if (payload) {
          const txHash = (payload.transactionHash || payload.transaction_hash || payload.hash || '').toLowerCase();
          if (!txHash || !txHash.startsWith('0x')) return;

          const serverTimestamp = msg.timestamp || (payload.timestamp ? (payload.timestamp > 1e11 ? payload.timestamp : payload.timestamp * 1000) : recvAt);

          onEvent({
            source: 'rtds',
            txHash,
            receivedAt: recvAt,
            serverTimestamp,
            payload
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
