/**
 * PolyEdge SSE Stream Ingestion Client (Zero Dependencies)
 * Protocol: Server-Sent Events (SSE) over HTTPS
 */

export const DEFAULT_STREAM_ID = 'e8f3b207-9b4e-4b77-a8d1-1376d8b67cf9';
export const DEFAULT_POLYEDGE_KEY = 'polyedge-v2-7f9a1c8b3e5d2f4091a6c8e3b5d7f1a92c4e6b8d0a2f4e6b';

/**
 * Subscribe to PolyEdge SSE Stream
 *
 * @param {string} urlStr
 * @param {string} apiKey
 * @param {(event: { source: 'polyedge', txHash: string, receivedAt: number, serverTimestamp: number, raw: any }) => void} onEvent
 * @param {AbortSignal} [signal]
 */
export async function subscribePolyEdgeStream(urlStr, apiKey, onEvent, signal) {
  const headers = {
    'Accept': 'text/event-stream',
    'User-Agent': 'polyedge-benchmark/1.0'
  };

  if (apiKey) {
    headers['X-PolyEdge-Key'] = apiKey;
  }

  const response = await fetch(urlStr, {
    method: 'GET',
    headers,
    signal
  });

  if (!response.ok) {
    const errorText = await response.text().catch(() => '');
    throw new Error(`PolyEdge SSE connection failed (HTTP ${response.status}): ${errorText}`);
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder('utf-8');
  let buffer = '';

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      const receivedAt = Date.now();
      buffer += decoder.decode(value, { stream: true });

      const parts = buffer.split('\n\n');
      buffer = parts.pop() || '';

      for (const part of parts) {
        if (!part.trim() || part.startsWith(':')) continue;

        const lines = part.split('\n');
        let eventType = '';
        let dataStr = '';

        for (const line of lines) {
          if (line.startsWith('event: ')) {
            eventType = line.slice(7).trim();
          } else if (line.startsWith('data: ')) {
            dataStr = line.slice(6).trim();
          }
        }

        if ((eventType === 'tx' || eventType === 'order') && dataStr) {
          try {
            const parsed = JSON.parse(dataStr);
            const txHash = Array.isArray(parsed) 
              ? (parsed[0] || '').toLowerCase() 
              : ((parsed.tx_hash || parsed.txHash || (parsed.data && parsed.data.tx_hash)) || '').toLowerCase();

            const timeStr = Array.isArray(parsed) 
              ? parsed[1] 
              : (parsed.timestamp || parsed.stream_time || (parsed.data && (parsed.data.stream_timestamp || parsed.data.stream_time)));

            let serverTimestamp = 0;
            if (typeof timeStr === 'number') {
              serverTimestamp = timeStr;
            } else if (typeof timeStr === 'string') {
              serverTimestamp = new Date(timeStr).getTime();
            }

            if (txHash && txHash.startsWith('0x')) {
              onEvent({
                source: 'polyedge',
                txHash,
                receivedAt,
                serverTimestamp: serverTimestamp || receivedAt,
                raw: parsed
              });
            }
          } catch (_) {
            // Ignore malformed heartbeats
          }
        }
      }
    }
  } catch (err) {
    if (signal?.aborted) return;
    throw err;
  }
}
