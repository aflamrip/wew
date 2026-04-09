<script lang="ts">
  import Plyr from 'plyr';
  import 'plyr/dist/plyr.css';

  let { src, subtitle } = $props<{ src: string, subtitle?: string }>();
  let videoElement = $state<HTMLVideoElement>();
  
  $effect(() => {
    let player: Plyr | undefined;
    
    if (videoElement) {
      player = new Plyr(videoElement, {
        controls: [
          'play-large', 'play', 'progress', 'current-time', 'duration',
          'mute', 'volume', 'captions', 'settings', 'pip', 'fullscreen'
        ],
        settings: ['captions', 'speed', 'loop'],
        captions: { active: true, language: 'ar', update: true }
      });
    }

    return () => {
      if (player) {
        player.destroy();
      }
    };
  });
</script>

<div class="h-full w-full">
  <video bind:this={videoElement} playsinline controls crossorigin="anonymous" class="w-full h-auto">
    <!-- Using cross origin so we can load captions and track events properly if CORS is setup on CDN -->
    <source {src} type="video/mp4" />
    {#if subtitle}
      <track kind="captions" label="عربي" srclang="ar" src={subtitle} default />
    {/if}
  </video>
</div>

<style>
  /* Customize plyr colors to match the theme */
  :global(.plyr--video .plyr__control--overlaid) {
    background: rgba(30, 64, 175, 0.8) !important; /* using primary blue */
  }
  
  :global(.plyr--video .plyr__control.plyr__tab-focus, 
          .plyr--video .plyr__control:hover, 
          .plyr--video .plyr__control[aria-expanded=true]) {
    background: #1e40af !important;
  }

  :global(.plyr--full-ui input[type=range]) {
    color: #1e40af !important;
  }
</style>
