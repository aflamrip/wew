// src/pages/api/cast/[type].ts
//
// Returns cast search results scoped strictly to the requested content type.
// If type is "movie" or "movies" → searches movies index only.
// If type is "tv" or "series"    → searches tv index only.
// The two content types are never mixed in a single response.

import type { APIRoute } from 'astro';
import { getPrefix } from '../../../lib/constants';
import { fetchIndexPage, fetchDetailNdjson } from '../../../lib/cf-bindings';

export const GET: APIRoute = async ({ params, url }) => {
  const { type }  = params;
  const actorName = url.searchParams.get('name')?.trim();

  if (!actorName || !type) {
    return new Response(JSON.stringify([]), {
      headers: { 'Content-Type': 'application/json' },
    });
  }

  // Resolve the canonical plural type used in R2 keys
  const validTypes = ['movie', 'movies', 'tv', 'series'];
  if (!validTypes.includes(type as string)) {
    return new Response(JSON.stringify({ error: 'Invalid type' }), {
      status:  400,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  const isMovie    = type === 'movie' || type === 'movies';
  const pluralType = isMovie ? 'movies' : 'tv';
  const routeType  = isMovie ? 'movie' : 'tv';   // used in slug URLs

  try {
    // Fetch first 2 index pages for the requested type only
    const [page1, page2] = await Promise.all([
      fetchIndexPage(pluralType as 'movies' | 'tv', 1),
      fetchIndexPage(pluralType as 'movies' | 'tv', 2),
    ]);

    const allEntries: any[] = [];
    for (const text of [page1, page2]) {
      if (!text) continue;
      text
        .split('\n')
        .filter(l => l.trim().length > 0)
        .forEach(l => {
          try {
            const v = JSON.parse(l);
            if (v?.id) allEntries.push(v);
          } catch { /* skip */ }
        });
    }

    // Fetch detail files in parallel (max 45 to stay within CF subrequest limits)
    const entriesToCheck = allEntries.slice(0, 45);
    const matchedItems: any[] = [];

    const detailPromises = entriesToCheck.map(async (entry: any) => {
      try {
        const id     = entry.id.toString();
        const prefix = getPrefix(id);

        const detailText = await fetchDetailNdjson(pluralType as 'movies' | 'tv', prefix, id);
        if (!detailText) return null;

        const lines = detailText
          .split('\n')
          .filter(l => l.trim().length > 0);
        if (lines.length === 0) return null;

        let d: any;
        try { d = JSON.parse(lines[0]); } catch { return null; }
        if (!d) return null;

        const raw = d.data && typeof d.data === 'object' ? { ...d, ...d.data } : d;
        if (Array.isArray(raw.cast) && raw.cast.some((c: string) => c.trim() === actorName)) {
          return {
            id,
            slug:  raw.slug  || entry.slug,
            title: raw.title || entry.title,
            year:  raw.year  || entry.year,
            lang:  raw.lang  || entry.lang,
            // Always mark with the requested content type so the UI routes correctly
            type:  routeType,
          };
        }
        return null;
      } catch {
        return null;
      }
    });

    const results = await Promise.allSettled(detailPromises);
    for (const r of results) {
      if (r.status === 'fulfilled' && r.value) {
        matchedItems.push(r.value);
      }
    }

    return new Response(JSON.stringify(matchedItems), {
      headers: {
        'Content-Type':  'application/json',
        'Cache-Control': 'public, s-maxage=7200, stale-while-revalidate=14400',
      },
    });
  } catch (e) {
    console.error('Cast API error:', e);
    return new Response(JSON.stringify([]), {
      status:  500,
      headers: { 'Content-Type': 'application/json' },
    });
  }
};
