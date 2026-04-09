import { CDN_URLS, getPrefix, formatDateWithOffset } from '../constants';

export interface SeoMetadata {
  title: string;
  description: string;
  schema: any;
  ogType: string;
}

export interface SeoItemInput {
  item: any;
  type: 'movie' | 'tv';
  season?: string;
  episode?: string;
  siteUrl: string;
  currentUrl: string;
  isWatchPage: boolean;
}

const getArabicOrdinal = (numStr?: string) => {
  const num = parseInt(numStr || '1', 10);
  const ordinals: Record<number, string> = {
    1: 'الأولى', 2: 'الثانية', 3: 'الثالثة', 4: 'الرابعة', 5: 'الخامسة',
    6: 'السادسة', 7: 'السابعة', 8: 'الثامنة', 9: 'التاسعة', 10: 'العاشرة',
    11: 'الحادية عشر', 12: 'الثانية عشر', 13: 'الثالثة عشر', 14: 'الرابعة عشر',
    15: 'الخامسة عشر', 16: 'السادسة عشر', 17: 'السابعة عشر', 18: 'الثامنة عشر',
    19: 'التاسعة عشر', 20: 'العشرون', 21: 'الحادية والعشرون', 22: 'الثانية والعشرون',
    23: 'الثالثة والعشرون', 24: 'الرابعة والعشرون', 25: 'الخامسة والعشرون',
    26: 'السادسة والعشرون', 27: 'السابعة والعشرون', 28: 'الثامنة والعشرون',
    29: 'التاسعة والعشرون', 30: 'الثلاثون'
  };
  return ordinals[num] || numStr;
};

const cleanTitle = (str: string) => {
  return str ? str.replace(/\s*\(?\[?[0-9]{4}\)?\]?$/g, '').trim() : '';
};

const smartTruncate = (str: string, max: number) => {
  if (!str) return '';
  return str.length > max ? str.substring(0, max - 3) + '...' : str;
};

/** Poster URL — always uses image CDN (static.ma3ak.top) */
const getPosterUrl = (item: any, type: 'movie' | 'tv') => {
  const prefix = getPrefix(item.id);
  const path = type === 'movie' ? 'movies' : 'tv';
  return `${CDN_URLS.STATIC}/${path}/${prefix}/${item.id}/${item.id}.webp`;
};

const getFormattedDates = (item: any) => {
  let ts = Number(item.data?.publish_date_timestamp || Date.now() / 1000);
  if (ts > 9999999999) ts = ts / 1000;
  return {
    pubDate: formatDateWithOffset(ts * 1000),
    modDate: formatDateWithOffset()
  };
};

export const wrapMetadata = (meta: { title: string; description: string; schema: any; ogType: string }) => {
  const brand = ' - معاك سيما';
  let finalTitle = meta.title;
  if (!finalTitle.includes("معاك سيما")) {
    if ((finalTitle + brand).length > 65) {
      finalTitle = smartTruncate(finalTitle, 65 - brand.length) + brand;
    } else {
      finalTitle += brand;
    }
  }
  
  return {
    ...meta,
    title: smartTruncate(finalTitle, 65),
    description: smartTruncate(meta.description, 155)
  };
};

// --- 1. Arabic Movies Template ---
export function arabicMovieTemplate(input: SeoItemInput): SeoMetadata {
  const { item, currentUrl, isWatchPage, siteUrl } = input;
  const title = cleanTitle(item.data?.['title-ar'] || item.data?.arabic_title || item.data?.title || item.title);
  const year = item.data?.year || '2026';
  const overview = item.data?.overview || item.overview || '';
  const slug = item.data?.slug || item.slug || '';
  const { pubDate, modDate } = getFormattedDates(item);
  const poster = getPosterUrl(item, 'movie');

  let seoTitle = isWatchPage ? `مشاهدة فيلم ${title} ${year} كامل HD` : `فيلم ${title} ${year} كامل HD`;
  let description = isWatchPage ? `مشاهدة وتحميل فيلم ${title} ${year} اون لاين بجودة عالية HD كامل ${overview}` : `مشاهدة فيلم ${title} اون لاين - ${overview}`;

  const director = item.data?.director;
  const cast = Array.isArray(item.data?.cast) ? item.data.cast : [];
  const safeSiteUrl = siteUrl || 'https://ma3ak.top';
  const watchUrl = `${safeSiteUrl}/watch/movie/${slug}`;

  const schema: any = {
    "@context": "https://schema.org",
    "@type": ["WebPage", "Movie"],
    "name": seoTitle,
    "url": currentUrl,
    "image": poster,
    "description": description,
    "inLanguage": "ar",
    "datePublished": pubDate,
    "dateModified": modDate,
    "genre": item.data?.genres || [],
    ...(director && { "director": { "@type": "Person", "name": director } }),
    ...(cast.length > 0 && { "actor": cast.map((name: string) => ({ "@type": "Person", "name": name, "url": `${safeSiteUrl}/movie/cast/${encodeURIComponent(name.trim())}` })) }),
    "aggregateRating": { "@type": "AggregateRating", "ratingValue": "8.5", "reviewCount": "1200" },
    "video": {
      "@type": "VideoObject",
      "name": seoTitle,
      "description": description,
      "thumbnailUrl": poster,
      "uploadDate": pubDate,
      "embedUrl": watchUrl
    }
  };

  return wrapMetadata({ title: seoTitle, description, schema, ogType: 'video.movie' });
}

// --- 2. Foreign Movies Template ---
export function foreignMovieTemplate(input: SeoItemInput): SeoMetadata {
  const { item, currentUrl, isWatchPage, siteUrl } = input;
  const engTitle = cleanTitle(item.data?.title || item.title);
  const arTitle = cleanTitle(item.data?.['title-ar'] || item.data?.arabic_title || '');
  const year = item.data?.year || '2026';
  const overview = item.data?.overview || item.overview || '';
  const slug = item.data?.slug || item.slug || '';
  const { pubDate, modDate } = getFormattedDates(item);
  const poster = getPosterUrl(item, 'movie');

  let seoTitle = isWatchPage ? `مشاهدة فيلم ${arTitle || engTitle} ${year} مترجم` : `فيلم ${arTitle || engTitle} ${year} مترجم`;
  let description = isWatchPage ? `مشاهدة وتحميل فيلم ${arTitle || engTitle} (${engTitle}) ${year} مترجم اون لاين بجودة عالية HD كامل ${overview}` : `مشاهدة فيلم ${arTitle || engTitle} مترجم - ${engTitle} بجودة عالية HD اون لاين ${overview}`;

  const director = item.data?.director;
  const cast = Array.isArray(item.data?.cast) ? item.data.cast : [];
  const safeSiteUrl = siteUrl || 'https://ma3ak.top';
  const watchUrl = `${safeSiteUrl}/watch/movie/${slug}`;

  const schema: any = {
    "@context": "https://schema.org",
    "@type": ["WebPage", "Movie"],
    "name": seoTitle,
    "url": currentUrl,
    "image": poster,
    "description": description,
    "inLanguage": "en",
    "datePublished": pubDate,
    "dateModified": modDate,
    "genre": item.data?.genres || [],
    ...(director && { "director": { "@type": "Person", "name": director } }),
    ...(cast.length > 0 && { "actor": cast.map((name: string) => ({ "@type": "Person", "name": name, "url": `${safeSiteUrl}/movie/cast/${encodeURIComponent(name.trim())}` })) }),
    "aggregateRating": { "@type": "AggregateRating", "ratingValue": "8.5", "reviewCount": "1200" },
    "video": {
      "@type": "VideoObject",
      "name": seoTitle,
      "description": description,
      "thumbnailUrl": poster,
      "uploadDate": pubDate,
      "embedUrl": watchUrl
    }
  };

  return wrapMetadata({ title: seoTitle, description, schema, ogType: 'video.movie' });
}

// --- 3. Arabic Series Template ---
export function arabicSeriesTemplate(input: SeoItemInput): SeoMetadata {
  const { item, currentUrl, isWatchPage, season, episode, siteUrl } = input;
  const title = cleanTitle(item.data?.['title-ar'] || item.data?.arabic_title || item.data?.title || item.title);
  const year = item.data?.year || '2026';
  const overview = item.data?.overview || item.overview || '';
  const slug = item.data?.slug || item.slug || '';
  const { pubDate, modDate } = getFormattedDates(item);
  const poster = getPosterUrl(item, 'tv');

  let seoTitle = '';
  let description = '';
  let schema: any = {};
  const safeSiteUrl = siteUrl || 'https://ma3ak.top';

  const director = item.data?.director;
  const cast = Array.isArray(item.data?.cast) ? item.data.cast : [];

  if (isWatchPage && episode) {
    const sNum = parseInt(season || '1', 10);
    const sStr = sNum > 1 ? ` الموسم ${sNum}` : '';
    seoTitle = `مشاهدة مسلسل ${title}${sStr} الحلقة ${episode} ${getArabicOrdinal(episode)}`;
    description = `مشاهدة وتحميل مسلسل ${title} الحلقة ${episode} اون لاين ${overview}`;
    
    schema = {
      "@context": "https://schema.org",
      "@type": ["WebPage", "TVEpisode"],
      "name": seoTitle,
      "url": currentUrl,
      "episodeNumber": parseInt(episode),
      "image": poster,
      ...(director && { "director": { "@type": "Person", "name": director } }),
      ...(cast.length > 0 && { "actor": cast.map((name: string) => ({ "@type": "Person", "name": name, "url": `${safeSiteUrl}/tv/cast/${encodeURIComponent(name.trim())}` })) }),
      "video": {
        "@type": "VideoObject",
        "name": seoTitle,
        "description": description,
        "thumbnailUrl": poster,
        "uploadDate": pubDate,
        "embedUrl": currentUrl
      }
    };
  } else {
    seoTitle = `مسلسل ${title} ${year} كامل`;
    description = `مشاهدة وتحميل جميع حلقات مسلسل ${title} الموسم الاول مسلسلات عربية ${year} على موقع معاك سيما`;
    
    schema = {
      "@context": "https://schema.org",
      "@type": ["WebPage", "TVSeries"],
      "name": seoTitle,
      "url": currentUrl,
      "description": description,
      "image": poster,
      "startDate": pubDate,
      "genre": item.data?.genres || [],
      ...(director && { "director": { "@type": "Person", "name": director } }),
      ...(cast.length > 0 && { "actor": cast.map((name: string) => ({ "@type": "Person", "name": name, "url": `${safeSiteUrl}/tv/cast/${encodeURIComponent(name.trim())}` })) })
    };
  }

  return wrapMetadata({ title: seoTitle, description, schema, ogType: 'video.tv_show' });
}

// --- 4. Foreign Series Template ---
export function foreignSeriesTemplate(input: SeoItemInput): SeoMetadata {
  const { item, currentUrl, isWatchPage, season, episode, siteUrl } = input;
  const engTitle = cleanTitle(item.data?.title || item.title);
  const arTitle = cleanTitle(item.data?.['title-ar'] || item.data?.arabic_title || '');
  const year = item.data?.year || '2026';
  const overview = item.data?.overview || item.overview || '';
  const slug = item.data?.slug || item.slug || '';
  const { pubDate, modDate } = getFormattedDates(item);
  const poster = getPosterUrl(item, 'tv');

  let seoTitle = '';
  let description = '';
  let schema: any = {};
  const safeSiteUrl = siteUrl || 'https://ma3ak.top';

  const director = item.data?.director;
  const cast = Array.isArray(item.data?.cast) ? item.data.cast : [];

  if (isWatchPage && episode) {
    const sNum = parseInt(season || '1', 10);
    const sStr = sNum > 1 ? ` الموسم ${sNum}` : ' الموسم الاول';
    seoTitle = `مشاهدة مسلسل ${arTitle || engTitle}${sStr} الحلقة ${episode} مترجمة`;
    description = `مشاهدة وتحميل الحلقة ${episode} من مسلسل ${arTitle || engTitle} مترجم بجودة HD اون لاين ${overview}`;
    
    schema = {
      "@context": "https://schema.org",
      "@type": ["WebPage", "TVEpisode"],
      "name": seoTitle,
      "url": currentUrl,
      "episodeNumber": parseInt(episode),
      "image": poster,
      ...(director && { "director": { "@type": "Person", "name": director } }),
      ...(cast.length > 0 && { "actor": cast.map((name: string) => ({ "@type": "Person", "name": name, "url": `${safeSiteUrl}/tv/cast/${encodeURIComponent(name.trim())}` })) }),
      "video": {
        "@type": "VideoObject",
        "name": seoTitle,
        "description": description,
        "thumbnailUrl": poster,
        "uploadDate": pubDate,
        "embedUrl": currentUrl
      }
    };
  } else {
    seoTitle = `مسلسل ${arTitle || engTitle} ${year} مترجم`;
    description = `مشاهدة وتحميل جميع حلقات مسلسل ${arTitle || engTitle} (${engTitle}) مترجم كامل بجودة HD اون لاين ${overview}`;
    
    schema = {
      "@context": "https://schema.org",
      "@type": ["WebPage", "TVSeries"],
      "name": seoTitle,
      "url": currentUrl,
      "description": description,
      "image": poster,
      "startDate": pubDate,
      "genre": item.data?.genres || [],
      ...(director && { "director": { "@type": "Person", "name": director } }),
      ...(cast.length > 0 && { "actor": cast.map((name: string) => ({ "@type": "Person", "name": name, "url": `${safeSiteUrl}/tv/cast/${encodeURIComponent(name.trim())}` })) })
    };
  }

  return wrapMetadata({ title: seoTitle, description, schema, ogType: 'video.tv_show' });
}
