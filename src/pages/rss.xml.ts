import rss from '@astrojs/rss';
import { getSitemapData, getFullSiteUrl, CDN_URLS, getPrefix } from '../lib/constants';

export async function GET(context: any) {
  const currentSite = getFullSiteUrl(context);
  
  // Get latest 50 items for fresh RSS
  const movies = await getSitemapData('movie', 3);
  const series = await getSitemapData('tv', 3);

  const movieItems = movies.slice(0, 25).map((m: any) => {
    const thumbUrl = `${CDN_URLS.STATIC}/movies/${getPrefix(m.id)}/${m.id}/${m.id}.webp`;
    const castNames = Array.isArray(m.data.cast) ? m.data.cast : [];
    const castLinks = castNames.map((name: string) => 
      `<a href="${currentSite}/movie/cast/${encodeURIComponent(name.trim())}">${name}</a>`
    ).join('، ');
    const directorHtml = m.data.director ? `<p><strong>إخراج:</strong> ${m.data.director}</p>` : '';
    const castHtml = castLinks ? `<p><strong>طاقم العمل:</strong> ${castLinks}</p>` : '';
    
    return {
      title: `فيلم: ${m.data.title}`,
      pubDate: new Date(),
      description: m.data.overview || `شاهد فيلم ${m.data.title} أون لاين على معاك سيما`,
      link: `/movie/${m.data.slug}`,
      content: `<img src="${thumbUrl}" alt="${m.data.title}" /><br/>${directorHtml}${castHtml}<p>${m.data.overview || ''}</p>`,
      customData: `<media:content 
        xmlns:media="http://search.yahoo.com/mrss/"
        url="${thumbUrl}" 
        medium="image" 
        height="320" 
        width="213" />`
    };
  });

  const tvItems = series.slice(0, 25).map((t: any) => {
    const thumbUrl = `${CDN_URLS.STATIC}/tv/${getPrefix(t.id)}/${t.id}/${t.id}.webp`;
    const castNames = Array.isArray(t.data.cast) ? t.data.cast : [];
    const castLinks = castNames.map((name: string) => 
      `<a href="${currentSite}/tv/cast/${encodeURIComponent(name.trim())}">${name}</a>`
    ).join('، ');
    const directorHtml = t.data.director ? `<p><strong>إخراج:</strong> ${t.data.director}</p>` : '';
    const castHtml = castLinks ? `<p><strong>طاقم العمل:</strong> ${castLinks}</p>` : '';
    
    return {
      title: `مسلسل: ${t.data.title}`,
      pubDate: new Date(),
      description: t.data.overview || `شاهد حلقات مسلسل ${t.data.title} على معاك سيما`,
      link: `/tv/${t.data.slug}`,
      content: `<img src="${thumbUrl}" alt="${t.data.title}" /><br/>${directorHtml}${castHtml}<p>${t.data.overview || ''}</p>`,
      customData: `<media:content 
        xmlns:media="http://search.yahoo.com/mrss/"
        url="${thumbUrl}" 
        medium="image" 
        height="320" 
        width="213" />`
    };
  });

  return rss({
    title: 'موقع معاك سيما - أحدث الأفلام والمسلسلات',
    description: 'موقع تفاعلي سريع لمشاهدة الأفلام والمسلسلات المترجمة حصرياً.',
    site: currentSite,
    items: [...movieItems, ...tvItems],
    customData: `<language>ar</language><follow_challenge><feedId>51241</feedId></follow_challenge>`,
  });
}

export const responseConfig = {
  headers: {
    'Content-Type': 'application/rss+xml; charset=utf-8',
    'Cache-Control': 'public, s-maxage=86400'
  }
};
