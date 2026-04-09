<script lang="ts">
  import { actions } from 'astro:actions';
  // Svelte 5 Runes
  let query = $state('');
  let results = $state<{id: string, title: string, slug: string, type: string}[]>([]);
  let isSearching = $state(false);

  $effect(() => {
    const timer = setTimeout(async () => {
      if (query.trim().length > 2) {
        isSearching = true;
        try {
          const res = await actions.searchOrama({ term: query });
          if (res.data) {
            results = res.data;
          }
        } catch (e) {
          console.error(e);
        } finally {
          isSearching = false;
        }
      } else {
        results = [];
      }
    }, 400); // 400ms debounce
    
    return () => clearTimeout(timer);
  });
</script>

<div class="relative w-full group">
  <div class="absolute inset-y-0 right-0 max-w-sm flex items-center pr-4 pointer-events-none">
    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="text-zinc-500 group-focus-within:text-primary transition-colors"><circle cx="11" cy="11" r="8"></circle><line x1="21" y1="21" x2="16.65" y2="16.65"></line></svg>
  </div>
  
  <input 
     type="search" 
     bind:value={query} 
     placeholder="ابحث بالعنوان أو اسم الممثل..." 
     class="w-full bg-zinc-950/80 backdrop-blur border-2 border-zinc-800 focus:border-amber-500 focus:bg-zinc-900 focus:outline-none rounded-2xl py-4 pr-12 pl-6 text-xl text-white transition-all shadow-xl placeholder:text-zinc-600 font-medium"
     autocomplete="off"
  />
  
  {#if isSearching}
     <div class="absolute top-1/2 left-6 transform -translate-y-1/2 flex gap-1">
        <span class="w-2 h-2 bg-amber-500 rounded-full animate-bounce [animation-delay:-0.3s]"></span>
        <span class="w-2 h-2 bg-amber-500 rounded-full animate-bounce [animation-delay:-0.15s]"></span>
        <span class="w-2 h-2 bg-amber-500 rounded-full animate-bounce"></span>
     </div>
  {/if}

  {#if results.length > 0}
    <div class="absolute mt-3 bg-zinc-900 border border-zinc-700 w-full rounded-2xl overflow-hidden shadow-2xl z-50 ring-1 ring-black/50 max-h-96 overflow-y-auto">
      <div class="p-2 border-b border-zinc-800 text-xs text-zinc-500 text-right pr-4 bg-zinc-950/30 font-medium tracking-wide">
        نتائج البحث بالاقتراحات الذكية...
      </div>
      {#each results as res}
        <a href={`/${res.type}/${res.slug}`} class="flex items-center gap-4 py-4 px-6 hover:bg-zinc-800 transition-colors border-b border-zinc-800/60 last:border-b-0 text-right group">
          <div class="aspect-[2/3] h-14 bg-zinc-800 rounded skeleton"></div> 
          <div class="flex flex-col">
            <span class="font-bold text-lg text-white group-hover:text-primary transition-colors">{res.title}</span>
            <span class="text-xs bg-zinc-800 px-2 py-0.5 rounded text-zinc-400 border border-zinc-700 w-fit mt-1">
              {res.type === 'movie' ? 'فيلم' : 'مسلسل'}
            </span>
          </div>
        </a>
      {/each}
    </div>
  {/if}
</div>
