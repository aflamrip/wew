/**
 * src/lib/cf-bindings.ts
 *
 * Unified Cloudflare Bindings access layer.
 *
 * Strategy (priority order):
 *   1. KV cache  — fast, edge-local, avoids repeated R2 reads
 *   2. R2 bucket — direct object storage read (zero egress within CF)
 *   3. HTTP fetch — fallback for local dev without workerd (uses static.ma3ak.top)
 *
 * NOTE: In wrangler.jsonc the static R2 bucket is bound as "R2" (not "CDN_STATIC").
 * KV namespace is bound as "SESSION".
 */

import { env } from 'cloudflare:workers';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface CFEnv {
  /** R2 bucket: static CDN — ndjson + poster webp */
  R2: R2Bucket;
  /** R2 bucket: video CDN — mp4 + vtt */
  CDN_VIDEO: R2Bucket;
  /** KV namespace: ndjson page cache */
  SESSION: KVNamespace;
  /** Assets binding */
  ASSETS: Fetcher;
  /** Cloudflare Images binding */
  IMAGES: ImagesBinding;
}

const cfEnv = env as unknown as CFEnv;

// ---------------------------------------------------------------------------
// KV TTL constants (seconds)
// ---------------------------------------------------------------------------
const KV_TTL_INDEX  = 60 * 60 * 4;   // 4 h — index pages change infrequently
const KV_TTL_DETAIL = 60 * 60 * 12;  // 12 h — detail files are even more stable
const KV_TTL_SEASON = 60 * 30;        // 30 min — season lists update more often

// ---------------------------------------------------------------------------
// Low-level helpers
// ---------------------------------------------------------------------------

async function r2GetText(bucket: R2Bucket, key: string): Promise<string | null> {
  try {
    const obj = await bucket.get(key);
    if (!obj) return null;
    return await obj.text();
  } catch {
    return null;
  }
}

async function r2GetArrayBuffer(
  bucket: R2Bucket,
  key: string
): Promise<ArrayBuffer | null> {
  try {
    const obj = await bucket.get(key);
    if (!obj) return null;
    return await obj.arrayBuffer();
  } catch {
    return null;
  }
}

/**
 * KV-cached R2 text read.
 * Priority: KV cache → R2 bucket → HTTP fallback (dev only)
 */
async function cachedText(
  kvKey: string,
  r2Key: string,
  httpFallbackUrl: string,
  ttl: number
): Promise<string | null> {
  // 1. KV cache hit
  try {
    const cached = await cfEnv.SESSION.get(kvKey);
    if (cached !== null) return cached;
  } catch {
    // KV not available — skip to R2
  }

  // 2. R2 read (primary data source — no external domain involved)
  let text: string | null = null;
  try {
    text = await r2GetText(cfEnv.R2, r2Key);
  } catch {
    // R2 not available — skip to HTTP fallback
  }

  // 3. HTTP fallback for local dev / astro preview without real R2
  if (text === null) {
    try {
      const res = await fetch(httpFallbackUrl);
      if (res.ok) text = await res.text();
    } catch {
      return null;
    }
  }

  // Write to KV (fire-and-forget)
  if (text !== null) {
    try {
      cfEnv.SESSION.put(kvKey, text, { expirationTtl: ttl }).catch(() => {});
    } catch {
      // non-fatal
    }
  }

  return text;
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/** Fallback HTTP base for local dev (images CDN also hosts ndjson in dev) */
const STATIC_BASE = 'https://static.ma3ak.top';

/**
 * Fetch a paginated ndjson index file.
 * R2 key: `{type}/index.{page}.ndjson`
 */
export async function fetchIndexPage(
  type: 'movies' | 'tv',
  page: number
): Promise<string | null> {
  const r2Key  = `${type}/index.${page}.ndjson`;
  const kvKey  = `idx:${type}:${page}`;
  const httpUrl = `${STATIC_BASE}/${r2Key}`;
  return cachedText(kvKey, r2Key, httpUrl, KV_TTL_INDEX);
}

/**
 * Fetch a detail ndjson for a specific item.
 * R2 key: `{type}/{prefix}/{id}/{id}.ndjson`
 */
export async function fetchDetailNdjson(
  type: 'movies' | 'tv',
  prefix: string,
  id: string
): Promise<string | null> {
  const r2Key   = `${type}/${prefix}/${id}/${id}.ndjson`;
  const kvKey   = `det:${type}:${id}`;
  const httpUrl = `${STATIC_BASE}/${r2Key}`;
  return cachedText(kvKey, r2Key, httpUrl, KV_TTL_DETAIL);
}

/**
 * Fetch cdn.ndjson (video metadata) for a specific item.
 * R2 key: `{type}/{prefix}/{id}/cdn.ndjson`
 */
export async function fetchCdnNdjson(
  type: 'movies' | 'tv',
  prefix: string,
  id: string
): Promise<string | null> {
  const r2Key   = `${type}/${prefix}/${id}/cdn.ndjson`;
  const kvKey   = `cdn:${type}:${id}`;
  const httpUrl = `${STATIC_BASE}/${r2Key}`;
  return cachedText(kvKey, r2Key, httpUrl, KV_TTL_DETAIL);
}

/**
 * Fetch a season episode list ndjson.
 * R2 key: `tv/{prefix}/{id}/cdn.s{season}.ndjson`
 */
export async function fetchSeasonNdjson(
  prefix: string,
  id: string,
  seasonPadded: string
): Promise<string | null> {
  const r2Key   = `tv/${prefix}/${id}/cdn.s${seasonPadded}.ndjson`;
  const kvKey   = `sea:tv:${id}:s${seasonPadded}`;
  const httpUrl = `${STATIC_BASE}/${r2Key}`;
  return cachedText(kvKey, r2Key, httpUrl, KV_TTL_SEASON);
}

/**
 * Check whether a season ndjson exists (HEAD-equivalent via R2).
 */
export async function seasonExists(
  prefix: string,
  id: string,
  seasonPadded: string
): Promise<boolean> {
  // Try R2 first
  try {
    const head = await cfEnv.R2.head(
      `tv/${prefix}/${id}/cdn.s${seasonPadded}.ndjson`
    );
    if (head !== null) return true;
  } catch {
    // R2 unavailable — fall through to HTTP
  }
  // HTTP fallback
  try {
    const res = await fetch(
      `${STATIC_BASE}/tv/${prefix}/${id}/cdn.s${seasonPadded}.ndjson`,
      { method: 'HEAD' }
    );
    return res.ok;
  } catch {
    return false;
  }
}

/**
 * Serve a poster image directly from R2.
 * R2 key: `{type}/{prefix}/{id}/{id}.webp`
 */
export async function fetchPosterFromR2(
  type: 'movies' | 'tv',
  prefix: string,
  id: string
): Promise<Response | null> {
  try {
    const obj = await cfEnv.R2.get(`${type}/${prefix}/${id}/${id}.webp`);
    if (!obj) return null;
    const headers = new Headers();
    headers.set('Content-Type', 'image/webp');
    headers.set('Cache-Control', 'public, max-age=86400, s-maxage=86400');
    if (obj.httpEtag) headers.set('ETag', obj.httpEtag);
    return new Response(obj.body, { headers });
  } catch {
    return null;
  }
}

/**
 * Invalidate a cached ndjson entry in KV.
 */
export async function invalidateCache(kvKey: string): Promise<void> {
  try {
    await cfEnv.SESSION.delete(kvKey);
  } catch {
    // non-fatal
  }
}

/** Expose raw env for advanced use-cases. */
export { cfEnv };
