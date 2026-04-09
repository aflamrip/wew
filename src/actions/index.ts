import { defineAction } from 'astro:actions';
import { z } from 'astro:schema';
import { create, insert, search } from '@orama/orama';
import { fetchIndexPage } from '../lib/cf-bindings';

export const server = {
  searchOrama: defineAction({
    input: z.object({
      term: z.string(),
    }),
    handler: async (input) => {
      try {
        const db = await create({
          schema: {
            id:    'string',
            title: 'string',
            slug:  'string',
            type:  'string',
          },
        });

        // Fetch movie and tv index via R2/KV binding (HTTP fallback in dev)
        const movieText = await fetchIndexPage('movies', 1);
        if (movieText) {
          const lines = movieText
            .split('\n')
            .filter(l => l.trim().length > 0)
            .map(l => { try { return JSON.parse(l); } catch { return null; } })
            .filter(Boolean);
          for (const m of lines) {
            await insert(db, {
              id:    m.id.toString(),
              title: m.title,
              slug:  m.slug,
              type:  'movie',
            });
          }
        }

        const tvText = await fetchIndexPage('tv', 1);
        if (tvText) {
          const lines = tvText
            .split('\n')
            .filter(l => l.trim().length > 0)
            .map(l => { try { return JSON.parse(l); } catch { return null; } })
            .filter(Boolean);
          for (const t of lines) {
            await insert(db, {
              id:    t.id.toString(),
              title: t.title,
              slug:  t.slug,
              type:  'tv',
            });
          }
        }

        const results = await search(db, {
          term:       input.term,
          properties: ['title'],
          threshold:  0.5,
        });

        return results.hits.map(h => h.document);
      } catch (error) {
        console.error('Orama search error:', error);
        return [];
      }
    },
  }),
};
