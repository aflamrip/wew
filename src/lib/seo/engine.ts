import { 
  arabicMovieTemplate, 
  foreignMovieTemplate, 
  arabicSeriesTemplate, 
  foreignSeriesTemplate,
  wrapMetadata,
  type SeoItemInput,
  type SeoMetadata
} from './templates';

/**
 * Main SEO Engine that selects the appropriate template based on item metadata
 * ensuring no overlap between Arabic/Foreign Movies/Series.
 */
export function getSeoMetadata(input: SeoItemInput): SeoMetadata {
  const { item, type } = input;
  
  if (!item) {
    return wrapMetadata({
      title: 'معاك سيما - مشاهدة الأفلام والمسلسلات اون لاين',
      description: 'موقع معاك سيما لمشاهدة أحدث الافلام والمسلسلات العربية والأجنبية بجودة عالية أون لاين.',
      schema: {
        "@context": "https://schema.org",
        "@type": "WebSite",
        "name": "معاك سيما",
        "url": input.siteUrl
      },
      ogType: 'website'
    });
  }

  // Detect Language
  const isArabic = 
    item.data?.lang === 'ar' || 
    item.lang === 'ar' || 
    item.data?.original_language === 'ar';
    
  const isMovie = type === 'movie';

  // 1. Arabic Movies
  if (isMovie && isArabic) {
    return arabicMovieTemplate(input);
  }
  
  // 2. Foreign Movies
  if (isMovie && !isArabic) {
    return foreignMovieTemplate(input);
  }
  
  // 3. Arabic Series
  if (!isMovie && isArabic) {
    return arabicSeriesTemplate(input);
  }
  
  // 4. Foreign Series
  if (!isMovie && !isArabic) {
    return foreignSeriesTemplate(input);
  }

  // Fallback
  return wrapMetadata({
    title: item.data?.title || item.title || 'معاك سيما',
    description: item.data?.overview || 'شاهد اون لاين على معاك سيما',
    schema: null,
    ogType: isMovie ? 'video.movie' : 'video.tv_show'
  });
}
