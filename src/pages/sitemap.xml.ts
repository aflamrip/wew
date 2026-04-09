import { getFullSiteUrl, getSitemapData } from '../lib/constants';

/** 
 * PROFESSIONAL SCALABLE SITEMAP INDEX
 * Uses permanent numbering (e.g., sitemap-movies1.xml) to ensure 
 * URL stability and predictable crawling as the site grows.
 */
export async function GET(context: any) {
  try {
    const currentSite = getFullSiteUrl(context);
    const now = new Date().toISOString();

    // Secure dynamic calculation (Max 45 total subrequests for Worker safety)
    const movies = await getSitemapData('movie', 20);
    const tv = await getSitemapData('tv', 20);
    
    // Google standard: 50,000 URLs per sitemap
    const ITEMS_PER_SITEMAP = 50000;
    
    const moviePages = Math.max(1, Math.ceil(movies.length / ITEMS_PER_SITEMAP));
    const tvPages = Math.max(1, Math.ceil(tv.length / ITEMS_PER_SITEMAP));

    const getLinks = (type: string, totalPages: number) => {
      return Array.from({ length: totalPages }, (_, i) => `${currentSite}/sitemap-${type}${i + 1}.xml`);
    };

    const sitemaps = [
      `${currentSite}/sitemap-pages1.xml`,
      ...getLinks('movies', moviePages),
      ...getLinks('series', tvPages),
      ...getLinks('seasons', tvPages),
      ...getLinks('episodes', tvPages),
      ...getLinks('video', moviePages),
      `${currentSite}/sitemap-cast1.xml`,
    ];

    const sitemapIndex = `<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  ${sitemaps.map(url => `
  <sitemap>
    <loc>${url}</loc>
    <lastmod>${now}</lastmod>
  </sitemap>`).join('')}
</sitemapindex>
`;

    return new Response(sitemapIndex, {
      headers: {
        'Content-Type': 'application/xml; charset=utf-8',
        'Cache-Control': 'public, s-maxage=86400, stale-while-revalidate=86400'
      }
    });
  } catch (e: any) {
    return new Response(`DEBUG ERROR: ${e.message}\n${e.stack}`, { status: 500 });
  }
}
