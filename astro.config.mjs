// @ts-check
import { defineConfig } from 'astro/config';
import svelte from '@astrojs/svelte';
import cloudflare from '@astrojs/cloudflare';
import sitemap from '@astrojs/sitemap';
import partytown from '@astrojs/partytown';
import tailwindcss from '@tailwindcss/vite';

// https://astro.build/config
export default defineConfig({
  site: 'https://ma3ak.top',

  // External image domains still allowed for <img> src passthrough
  image: {
    domains: ['static.ma3ak.top']
  },

  output: 'server',

  adapter: cloudflare({
    // Use Cloudflare Images binding for runtime image optimisation.
    // The binding is declared in wrangler.jsonc as "IMAGES".
    // For build-time prerendered pages we fall back to 'compile'.
    imageService: { build: 'compile', runtime: 'cloudflare-binding' },

    // NOTE: platformProxy and experimentalJsonConfig have been REMOVED
    // in @astrojs/cloudflare v13. The dev server now uses the real
    // workerd runtime via the Cloudflare Vite plugin automatically.
  }),

  integrations: [
    svelte(),
    sitemap(),
    partytown({
      config: {
        forward: ['dataLayer.push']
      }
    })
  ],

  vite: {
    plugins: [
      tailwindcss()
    ],
    ssr: {
      // miniflare is no longer needed for local dev in v13 — workerd
      // runs directly via the Cloudflare Vite plugin.
      // Keep only truly incompatible packages here if needed.
      external: []
    }
  }
});
