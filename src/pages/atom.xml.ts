import { getSitemapData, getFullSiteUrl, CDN_URLS, getPrefix, formatDateWithOffset } from '../lib/constants';

export async function GET(context: any) {
  const currentSite = getFullSiteUrl(context);
  const now = formatDateWithOffset();
  
  const movies = await getSitemapData('movie', 2);
  const series = await getSitemapData('tv', 2);
  const allItems = [...movies.slice(0, 15), ...series.slice(0, 15)];

  const entries = allItems.map((item: any) => {
    const isMovie = item.data.type === 'movie';
    const typePath = isMovie ? 'movies' : 'tv';
    const routeType = isMovie ? 'movie' : 'tv';
    const thumbUrl = `${CDN_URLS.STATIC}/${typePath}/${getPrefix(item.id)}/${item.id}/${item.id}.webp`;
    const url = `${currentSite}/${routeType}/${item.data.slug}`;
    
    const castNames = Array.isArray(item.data.cast) ? item.data.cast : [];
    const castLinks = castNames.map((name: string) => 
      `<a href="${currentSite}/${routeType}/cast/${encodeURIComponent(name.trim())}">${name}</a>`
    ).join('، ');
    const directorHtml = item.data.director ? `<p><b>إخراج:</b> ${item.data.director}</p>` : '';
    const castHtml = castLinks ? `<p><b>طاقم العمل:</b> ${castLinks}</p>` : '';
    
    return `<entry>
      <title>${item.data.title}</title>
      <link href="${url}" />
      <id>${url}</id>
      <updated>${now}</updated>
      <summary>${item.data.overview || `مشاهدة ${item.data.title} على معاك سيما.`}</summary>
	  <author><name>معاك سيما</name></author>
      <content type="html"><![CDATA[<img src="${thumbUrl}" alt="${item.data.title}" /><br/>${directorHtml}${castHtml}<p>${item.data.overview || ''}</p>]]></content>
    </entry>`;
  }).join('');

  const atom = `<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>معاك سيما - أحدث الإضافات</title>
  <subtitle>مشاهدة الأفلام والمسلسلات أون لاين</subtitle>
  <link href="${currentSite}/atom.xml" rel="self" />
  <link href="${currentSite}/" />
  <id>${currentSite}/</id>
  <updated>${now}</updated>
  <author><name>معاك سيما</name></author>
  ${entries}
</feed>
`;

  return new Response(atom, {
    headers: {
      'Content-Type': 'application/atom+xml; charset=utf-8',
      'Cache-Control': 'public, s-maxage=86400, stale-while-revalidate=86400'
    }
  });
}
