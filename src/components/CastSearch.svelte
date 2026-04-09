<script>
  let { actorName, contentType, cdnBase } = $props();

  let items = $state([]);
  let loading = $state(true);
  let error = $state('');

  const isMovie = contentType === 'movie' || contentType === 'movies';
  const pluralType = isMovie ? 'movies' : 'tv';
  const displayType = isMovie ? 'أفلام' : 'مسلسلات';
  const routeType = isMovie ? 'movie' : 'tv';

  // Fetch from server API — scoped to the requested type only
  $effect(() => {
    if (!actorName) return;

    (async () => {
      loading = true;
      items = [];
      error = '';

      try {
        const res = await fetch(`/api/cast/${routeType}?name=${encodeURIComponent(actorName)}`);
        if (!res.ok) throw new Error('API error');
        const data = await res.json();
        // Every item returned by the API already has .type = routeType
        items = data;
      } catch (e) {
        error = 'حدث خطأ أثناء البحث، يرجى المحاولة لاحقاً.';
        console.error('Cast search error:', e);
      } finally {
        loading = false;
      }
    })();
  });

  let resultCount = $derived(items.length);
</script>

<div class="space-y-8">
  <!-- Loading -->
  {#if loading}
    <div class="bg-zinc-900/60 rounded-2xl p-6 border border-zinc-800 space-y-4">
      <div class="flex items-center gap-3 text-sm font-bold text-zinc-400">
        <span class="w-2 h-2 bg-amber-500 rounded-full animate-pulse"></span>
        جاري البحث عن {displayType} {actorName}...
      </div>
      <div class="w-full bg-zinc-800 rounded-full h-1.5 overflow-hidden">
        <div class="bg-gradient-to-l from-amber-500 to-orange-500 h-full rounded-full animate-pulse w-2/3"></div>
      </div>
    </div>
  {/if}

  <!-- Error -->
  {#if error}
    <div class="text-center py-16 bg-red-950/20 rounded-3xl border border-red-900/50">
      <p class="text-red-400 font-bold">{error}</p>
    </div>
  {/if}

  <!-- Results Grid -->
  {#if resultCount > 0}
    <div class="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-6 gap-6">
      {#each items as item (item.id)}
        <a href={`/${routeType}/${item.slug}`} class="group block animate-fade-in">
          <div class="aspect-[2/3] rounded-2xl overflow-hidden relative shadow-2xl border border-zinc-800/50 group-hover:border-amber-500/50 transition-all duration-500">
            <img 
              src={`${cdnBase}/${pluralType}/${item.id.toString().substring(0, 2)}/${item.id}/${item.id}.webp`}
              alt={item.title}
              loading="lazy"
              decoding="async"
              class="absolute inset-0 w-full h-full object-cover group-hover:scale-110 transition-transform duration-700"
            />
            <div class="absolute inset-0 bg-gradient-to-t from-zinc-950 via-zinc-900/20 to-transparent opacity-60"></div>
            <div class="absolute bottom-0 p-4 w-full translate-y-1 group-hover:translate-y-0 transition-transform duration-500">
              <div class="flex items-center justify-between">
                <span class="bg-amber-500/90 text-white text-[10px] font-black px-2 py-0.5 rounded shadow-lg">
                  {item.year}
                </span>
                <span class="text-[10px] font-black text-white opacity-80 tracking-tighter">
                  {isMovie ? 'فيلم' : 'مسلسل'}
                </span>
              </div>
            </div>
          </div>
          <h3 class="mt-3 text-sm font-bold text-zinc-300 group-hover:text-amber-400 truncate transition-colors">{item.title}</h3>
        </a>
      {/each}
    </div>
  {/if}

  <!-- Empty State -->
  {#if !loading && resultCount === 0 && !error}
    <div class="text-center py-32 bg-zinc-900/20 rounded-3xl border border-dashed border-zinc-800">
      <div class="text-6xl mb-6">🎭</div>
      <h2 class="text-2xl font-black text-zinc-400">عذراً، لم نتمكن من العثور على {displayType} لـ {actorName}</h2>
      <p class="mt-2 text-zinc-500 font-medium">قد لا تتوفر البيانات التفصيلية حالياً.</p>
      <div class="mt-8">
        <a href={`/${routeType}`} class="bg-amber-500 hover:bg-amber-600 text-white px-8 py-3 rounded-2xl font-black transition-all">تصفح {displayType}</a>
      </div>
    </div>
  {/if}

  <!-- Final Count -->
  {#if !loading && resultCount > 0}
    <div class="text-center py-4">
      <span class="bg-zinc-900 text-zinc-400 px-6 py-2 rounded-full text-sm font-bold border border-zinc-800">
        عُثر على {resultCount} {resultCount === 1 ? (isMovie ? 'فيلم' : 'مسلسل') : (isMovie ? 'أفلام' : 'مسلسلات')} لـ {actorName}
      </span>
    </div>
  {/if}
</div>

<style>
  @keyframes fade-in {
    from { opacity: 0; transform: translateY(12px); }
    to { opacity: 1; transform: translateY(0); }
  }
  .animate-fade-in {
    animation: fade-in 0.5s ease-out both;
  }
</style>
