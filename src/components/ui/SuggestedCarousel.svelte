<script lang="ts">
  import { CDN_URLS } from '../../lib/constants';
  import HorizontalCarousel from './HorizontalCarousel.svelte';
  import ndjsonStream from 'can-ndjson-stream';

  let { type = 'movie', genre, excludeSlug } = $props<{
    type?: string;
    genre?: string;
    excludeSlug?: string;
  }>();

  // 1- Svelte 5 Runes $state - $derived - $props - $effect - $state.raw - $host
  let items = $state.raw<any[]>([]);
  let isLoading = $state(true);
  let isError = $state(false);

  $effect(() => {
    let isCancelled = false;

    async function loadSuggestions() {
      try {
        isLoading = true;
        // 5- Cache + Edge
        const res = await fetch(`${CDN_URLS.STATIC}/${type}/index.1.ndjson`, {
          cf: { cacheTtl: 3600, cacheEverything: true }
        });
        if (!res.ok) throw new Error("Failed to fetch suggestions");
        
        // 3- Live Loader + can-ndjson-stream
        // Eager Loading: Show items as they stream in
        const stream = await ndjsonStream(res.body);
        const reader = stream.getReader();
        
        const newItems = [];
        while (true) {
          const { done, value } = await reader.read();
          if (done || isCancelled) break;
          
          if (value && value.slug !== excludeSlug) {
             newItems.push(value);
          }
          if (newItems.length >= 8) break; // Batch API limit
        }

        if (!isCancelled) {
          items = newItems;
        }
      } catch(e) {
        if (!isCancelled) isError = true;
      } finally {
        if (!isCancelled) isLoading = false;
      }
    }

    loadSuggestions();

    return () => {
      isCancelled = true;
    };
  });
</script>

<!-- Content Layer + Eager Loading UI -->
<div class="mt-16 pt-8 border-t border-zinc-900/50">
  {#if isLoading}
     <HorizontalCarousel title="أعمال مقترحة" items={[]} />
  {:else if isError}
     <div class="bg-red-900/20 text-red-500 p-4 rounded-xl border border-red-900/50 text-center">
       حدث خطأ أثناء تحميل الاقتراحات.
     </div>
  {:else if items && items.length > 0}
     <HorizontalCarousel title="أعمال مقترحة مميزة" {items} />
  {/if}
</div>
