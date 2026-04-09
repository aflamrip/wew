import { getSitemapData, getFullSiteUrl, GENRES_LIST, CDN_URLS, getPrefix, formatDateWithOffset } from '../lib/constants';

/** 
 * SCALABLE DYNAMIC SITEMAP ENGINE (Stable Version)
 * Rules:
 * 1. ALWAYS use numbering (e.g., sitemap-movies1.xml) for permanent indexing.
 * 2. This prevents URL changes when the site grows beyond page 1.
 */
export async function GET(context: any) {
  const { slug } = context.params;
  const currentSite = getFullSiteUrl(context);
  
  // Extract type and page number: e.g., 'sitemap-movies1', 'sitemap-pages1'
  const match = slug.match(/^([a-z-]+)(\d+)$/);
  if (!match) return new Response('Invalid Sitemap Param', { status: 404 });

  const [, rawType, pageNumStr] = match;
  const page = parseInt(pageNumStr, 10);
  const ITEMS_PER_SITEMAP = 50000;

  const typeKey = rawType.replace(/^sitemap-/, '');

  let urls: string[] = [];
  let isVideoMap = typeKey === 'video' || typeKey === 'video-sitemap';
  let xmlContent = "";

  try {
    if (typeKey === 'pages') {
      if (page > 1) return new Response('Range Out', { status: 404 });
      urls = [
        `${currentSite}/`, `${currentSite}/movie`, `${currentSite}/tv`, `${currentSite}/categories`, `${currentSite}/search`,
        ...GENRES_LIST.map(g => `${currentSite}/movie/category/${g.slug}`),
        ...GENRES_LIST.map(g => `${currentSite}/tv/category/${g.slug}`)
      ];
      xmlContent = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  ${urls.map(u => `<url><loc>${u}</loc><changefreq>weekly</changefreq><priority>1.0</priority></url>`).join('\n')}
</urlset>`;
    } 
    else if (typeKey === 'movies' || isVideoMap) {
      const all = await getSitemapData('movie', 45);
      const totalPages = Math.max(1, Math.ceil(all.length / ITEMS_PER_SITEMAP));
      if (page > totalPages) return new Response('Range Out', { status: 404 });
      
      const chunk = all.slice((page - 1) * ITEMS_PER_SITEMAP, page * ITEMS_PER_SITEMAP);
      if (isVideoMap) {
        xmlContent = generateVideoXml(chunk, currentSite);
      } else {
        urls = chunk.map(item => {
          const pubDate = formatDateWithOffset(item.data.published);
          return `  <url>
    <loc>${currentSite}/movie/${item.data.slug}</loc>
    <lastmod>${pubDate}</lastmod>
    <changefreq>monthly</changefreq>
    <priority>0.8</priority>
  </url>`;
        });
      }
    } 
    else if (typeKey === 'series' || typeKey === 'tv') {
      const all = await getSitemapData('tv', 45);
      const totalPages = Math.max(1, Math.ceil(all.length / ITEMS_PER_SITEMAP));
      if (page > totalPages) return new Response('Range Out', { status: 404 });
      
      const chunk = all.slice((page - 1) * ITEMS_PER_SITEMAP, page * ITEMS_PER_SITEMAP);
      urls = chunk.map(item => {
        const pubDate = formatDateWithOffset(item.data.published);
        return `  <url>
    <loc>${currentSite}/tv/${item.data.slug}</loc>
    <lastmod>${pubDate}</lastmod>
    <changefreq>monthly</changefreq>
    <priority>0.8</priority>
  </url>`;
      });
    } 
    else if (typeKey === 'seasons' || typeKey === 'episodes') {
      const all = await getSitemapData('tv', 45);
      const totalPages = Math.max(1, Math.ceil(all.length / ITEMS_PER_SITEMAP));
      if (page > totalPages) return new Response('Range Out', { status: 404 });
      
      const chunk = all.slice((page - 1) * ITEMS_PER_SITEMAP, page * ITEMS_PER_SITEMAP);
      
      if (typeKey === 'seasons') {
        urls = [];
        for (let i = 0; i < chunk.length; i++) {
          const t = chunk[i];
          const pubDate = formatDateWithOffset(t.data.published);
          const maxS = (i < 5) ? 5 : 1; 
          for (let s = 1; s <= maxS; s++) {
            urls.push(`  <url>
    <loc>${currentSite}/tv/${t.data.slug}/s${s}</loc>
    <lastmod>${pubDate}</lastmod>
    <changefreq>weekly</changefreq>
    <priority>0.7</priority>
  </url>`);
          }
        }
      } else {
        urls = [];
        for (let i = 0; i < chunk.length; i++) {
          const t = chunk[i];
          const pubDate = formatDateWithOffset(t.data.published);
          const maxE = (i < 3) ? 40 : 25; 
          for (let e = 1; e <= maxE; e++) {
             urls.push(`  <url>
    <loc>${currentSite}/tv/${t.data.slug}/s1/e${e}</loc>
    <lastmod>${pubDate}</lastmod>
    <changefreq>weekly</changefreq>
    <priority>0.6</priority>
  </url>`);
          }
        }
      }
    }
    else if (typeKey === 'cast') {
      const movies = await getSitemapData('movie', 10);
      const tv = await getSitemapData('tv', 10);
      const all = [...movies, ...tv];
      const castSet = new Set<string>();
      all.forEach(item => {
        if (Array.isArray(item.data.cast)) {
          item.data.cast.forEach((name: string) => castSet.add(name.trim()));
        }
      });
      
      const sortedCast = Array.from(castSet).sort();
      const chunk = sortedCast.slice((page - 1) * ITEMS_PER_SITEMAP, page * ITEMS_PER_SITEMAP);
      urls = chunk.map(name => `  <url>
    <loc>${currentSite}/movie/cast/${encodeURIComponent(name)}</loc>
    <changefreq>monthly</changefreq>
    <priority>0.5</priority>
  </url>`);
    }

    if (!xmlContent) {
      if (urls.length === 0) return new Response('Empty Partition', { status: 404 });
      xmlContent = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${urls.join('\n')}
</urlset>`;
    }

    return new Response(xmlContent, {
      headers: {
        'Content-Type': 'application/xml; charset=utf-8',
        'Cache-Control': 'public, s-maxage=86400, stale-while-revalidate=86400'
      }
    });

  } catch (e) {
    return new Response('Sitemap Error', { status: 500 });
  }
}

function generateVideoXml(items: any[], currentSite: string) {
  const CDN_URLS = { VIDEO: "https://s001.mogcdn.com", STATIC: "https://static.ma3ak.top" };
  const getPrefix = (id: any) => id.toString().substring(0, 2);

  return `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:video="http://www.google.com/schemas/sitemap-video/1.1">
  ${items.map((item: any) => {
    const id = item.id;
    const prefix = getPrefix(id);
    const thumb = `${CDN_URLS.STATIC}/movies/${prefix}/${id}/${id}.webp`;
    const pubDate = formatDateWithOffset(item.data.published);
    const title = item.data.arabic_title || item.data.title;
    const year = item.data.year || '2026';
    const isForeign = item.data.lang === 'en';
    const fullTitle = isForeign ? `فيلم ${title} ${year} مترجم كامل HD` : `مشاهدة فيلم ${title} ${year} كامل HD (معاك سيما)`;
    const description = item.data.overview ? item.data.overview.substring(0, 150) + '...' : `مشاهدة وتحميل فيلم ${title} اون لاين بجودة عالية HD كامل (معاك سيما)`;

    return `<url>
    <loc>${currentSite}/watch/movie/${item.data.slug}</loc>
    <lastmod>${pubDate}</lastmod>
    <video:video>
      <video:thumbnail_loc>${thumb}</video:thumbnail_loc>
      <video:title>${fullTitle}</video:title>
      <video:description>${description}</video:description>
      <video:content_loc>https://s001.mogcdn.com/movies/${prefix}/${id}/${id}.mp4</video:content_loc>
      <video:publication_date>${pubDate}</video:publication_date>
      <video:family_friendly>yes</video:family_friendly>
    </video:video>
  </url>`;
  }).join('')}
</urlset>`;
}
