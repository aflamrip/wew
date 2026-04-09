<script lang="ts">
  import emblaCarouselSvelte from 'embla-carousel-svelte';
  import Autoplay from 'embla-carousel-autoplay';

  // Svelte 5 Runes for props
  let { items = [], title = "أعمال مقترحة" } = $props<{
    items: any[];
    title?: string;
  }>();

  let options = { loop: true, direction: 'rtl', align: 'start' as const };
  let plugins = [Autoplay({ delay: 3500, stopOnInteraction: true })];

</script>

<div class="carousel-section mb-10 w-full relative">
  {#if title}
    <div class="flex items-center justify-between mb-6 border-b border-zinc-800 pb-2">
      <h3 class="text-2xl font-bold border-r-4 border-primary pr-3">{title}</h3>
      <div class="flex gap-2">
        <!-- Navigation Buttons could go here -->
      </div>
    </div>
  {/if}
  
  <div class="embla overflow-hidden rounded-xl" use:emblaCarouselSvelte={{ options, plugins }}>
    <div class="embla__container flex gap-4">
      {#if items.length > 0}
        {#each items as item}
          <div class="embla__slide flex-[0_0_140px] sm:flex-[0_0_180px] md:flex-[0_0_220px]">
             <!-- We use slots to allow passing custom cards like MovieCard -->
             <slot item={item}>
               <a href={`/${item.type || 'movie'}/${item.slug}`} class="block bg-zinc-900 rounded-lg overflow-hidden shadow-xl border border-zinc-800 h-full transition-transform hover:-translate-y-1 hover:border-blue-500/50 group">
                 <div class="aspect-[2/3] bg-zinc-800 relative flex items-center justify-center">
                    <span class="z-10 text-center p-2 text-sm text-zinc-400 group-hover:text-white transition-colors">{item.title}</span>
                    <div class="absolute inset-0 skeleton opacity-50"></div>
                 </div>
                 <div class="p-3">
                   <h4 class="font-semibold text-sm md:text-base truncate group-hover:text-amber-400 transition-colors">{item.title}</h4>
                   <p class="text-xs text-zinc-400 mt-1">{item.year}</p>
                 </div>
               </a>
             </slot>
          </div>
        {/each}
      {:else}
        <!-- Loading Skeletons for empty array -->
        {#each Array(6) as _, i}
          <div class="embla__slide flex-[0_0_140px] sm:flex-[0_0_180px] md:flex-[0_0_220px]">
            <div class="bg-zinc-900 rounded-lg aspect-[2/3] skeleton border border-zinc-800"></div>
          </div>
        {/each}
      {/if}
    </div>
  </div>
</div>

<style>
  .embla__container {
    backface-visibility: hidden;
    touch-action: pan-y pinch-zoom;
  }
</style>
