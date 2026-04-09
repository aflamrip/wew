/**
 * src/lib/constants.ts
 *
 * All CDN access now goes through the cf-bindings layer which uses:
 *   1. KV cache  (edge-local, fast)
 *   2. R2 bucket (zero-egress, reliable)
 *   3. HTTP fetch (local dev fallback only)
 */

import ndjsonStream from 'can-ndjson-stream';
import {
  fetchIndexPage,
  fetchDetailNdjson,
  fetchCdnNdjson,
  fetchSeasonNdjson,
  seasonExists,
} from './cf-bindings';

// ---------------------------------------------------------------------------
// Public CDN URLs (used for client-side <img> src, video src, etc.)
// These are NOT used for SSR data fetching anymore.
// ---------------------------------------------------------------------------
export const CDN_URLS = {
  STATIC: 'https://static.ma3ak.top',
  VIDEO:  'https://s001.mogcdn.com',
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Compute CDN prefix from ID: first 2 digits. e.g. 1010857 → "10" */
export function getPrefix(id: string | number): string {
  return id.toString().substring(0, 2);
}

// ---------------------------------------------------------------------------
// Internal ndjson parser (shared logic)
// ---------------------------------------------------------------------------
function parseNdjsonText(text: string): any[] {
  return text
    .split('\n')
    .filter(l => l.trim().length > 0)
    .map(l => { try { return JSON.parse(l); } catch { return null; } })
    .filter(Boolean);
}

function normaliseItem(rawD: any): any {
  if (!rawD) return null;
  const raw = rawD.data && typeof rawD.data === 'object' ? { ...rawD, ...rawD.data } : rawD;
  const data: any = { ...raw };

  const findValue = (keys: string[]) => {
    for (const k of keys) { if (raw[k]) return raw[k]; }
    return null;
  };

  data.arabic_title   = findValue(['title_ar', 'title-ar', 'arabic_title']);
  data['title-ar']    = data.arabic_title;
  data.original_title = raw.original_title || raw.name || '';
  data.title          = raw.title || raw.name || '';
  data.published      = raw.published || raw.date || raw.release_date || null;
  data.lang           = raw.lang || 'ar';

  const yearRaw = findValue(['year', 'release_date', 'date', 'published']) || '2026';
  data.year    = yearRaw.toString().substring(0, 4);
  data.overview = findValue(['overview', 'plot', 'summary', 'description', 'story', 'info', 'qessa', 'content']) || '';
  data.genres  = Array.isArray(raw.genres) ? raw.genres : (raw.genre ? [raw.genre] : []);
  data.slug    = raw.slug || (data.title ? data.title.toLowerCase().replace(/\s+/g, '-') : '');
  data.director = raw.director || null;
  data.cast    = Array.isArray(raw.cast) ? raw.cast : [];

  return data;
}

// ---------------------------------------------------------------------------
// SSR helper: fetch paginated index (R2 → KV → HTTP fallback)
// ---------------------------------------------------------------------------
export async function getDynamicContent(type: 'movie' | 'tv', pageCount = 3) {
  try {
    const pluralType = type.startsWith('movie') ? 'movies' : 'tv';
    const allItems: any[] = [];

    for (let i = 0; i < pageCount; i++) {
      try {
        const text = await fetchIndexPage(pluralType as 'movies' | 'tv', i + 1);
        if (!text) continue;

        for (const rawD of parseNdjsonText(text)) {
          const data = normaliseItem(rawD);
          if (data) {
            allItems.push({
              id:   (rawD.id || '').toString(),
              data,
            });
          }
        }
      } catch { /* skip page on error */ }
    }

    return allItems;
  } catch (e) {
    console.error('Error fetching dynamic indices:', e);
    return [];
  }
}

// ---------------------------------------------------------------------------
// SSR helper: find a specific item by slug (R2 → KV → HTTP fallback)
// ---------------------------------------------------------------------------
export async function getDynamicItemBySlug(
  type: 'movie' | 'tv',
  slug: string,
  pageCount = 10
) {
  try {
    const pluralType = type.startsWith('movie') ? 'movies' : 'tv';
    let foundInIndex: any = null;

    for (let i = 0; i < pageCount; i++) {
      if (foundInIndex) break;
      try {
        const text = await fetchIndexPage(pluralType as 'movies' | 'tv', i + 1);
        if (!text) continue;

        for (const value of parseNdjsonText(text)) {
          if (value && value.slug === slug) {
            foundInIndex = value;
            break;
          }
        }
      } catch { /* continue */ }
    }

    if (foundInIndex) {
      const id     = foundInIndex.id.toString();
      const prefix = getPrefix(id);

      // Try detail ndjson via R2/KV
      try {
        const detailText = await fetchDetailNdjson(pluralType as 'movies' | 'tv', prefix, id);
        if (detailText) {
          const lines = parseNdjsonText(detailText);
          const d = lines[0];
          if (d) {
            const raw = d.data && typeof d.data === 'object' ? { ...d, ...d.data } : d;
            const data: any = { ...raw };
            const findValue = (keys: string[]) => {
              for (const k of keys) { if (raw[k]) return raw[k]; }
              return null;
            };

            data.arabic_title   = findValue(['title_ar', 'title-ar', 'arabic_title']);
            data.original_title = raw.title || raw.name || raw.original_title || foundInIndex.title || '';
            data.title          = data.original_title;
            data.lang           = raw.lang || foundInIndex.lang || 'ar';

            const yearRaw = findValue(['year', 'release_date', 'date', 'published']) || '2026';
            data.year     = yearRaw.toString().substring(0, 4);
            data.overview = findValue(['overview', 'plot', 'summary', 'description', 'story', 'info', 'qessa', 'content'])
                            || foundInIndex.overview || '';
            data.genres   = Array.isArray(raw.genres) ? raw.genres : (raw.genre ? [raw.genre] : []);
            data.slug     = raw.slug || slug;

            return { id, data };
          }
        }
      } catch { /* fall through to cdn.ndjson */ }

      // Fallback: cdn.ndjson via R2/KV
      try {
        const cdnText = await fetchCdnNdjson(pluralType as 'movies' | 'tv', prefix, id);
        if (cdnText) {
          const lines = parseNdjsonText(cdnText);
          const d = lines[0];
          if (d) {
            const raw = d.data && typeof d.data === 'object' ? { ...d, ...d.data } : d;
            return { id, data: { ...raw, slug } };
          }
        }
      } catch { /* fall through to index data */ }

      // Ultimate fallback: return index data
      return { id, data: { ...foundInIndex, slug } };
    }

    return null;
  } catch (e) {
    console.error(`Error finding slug ${slug}:`, e);
    return null;
  }
}

// ---------------------------------------------------------------------------
// Re-export seasonExists and fetchSeasonNdjson for EpisodeIsland / PlayerIsland
// ---------------------------------------------------------------------------
export { seasonExists, fetchSeasonNdjson };

// ---------------------------------------------------------------------------
// Sitemap helper
// ---------------------------------------------------------------------------
export async function getSitemapData(type: 'movie' | 'tv', pageCount = 45) {
  return getDynamicContent(type, pageCount);
}

export function getFullSiteUrl(context: any): string {
  if (context.url && context.url.origin) {
    return context.url.origin.replace(/\/$/, '');
  }
  const site = context.site ? context.site.toString().replace(/\/$/, '') : 'https://ma3ak.top';
  return site;
}

/** Formats date to YYYY-MM-DDTHH:mm:ss+03:00 */
export function formatDateWithOffset(dateInput?: Date | string | number): string {
  const date = dateInput ? new Date(dateInput) : new Date();
  const offsetMs = 3 * 60 * 60 * 1000;
  const localDate = new Date(date.getTime() + offsetMs);
  const iso = localDate.toISOString();
  return iso.substring(0, 19) + '+03:00';
}

/** 40 SEO Promo Messages */
export const SEO_PROMO_MESSAGES = [
  'يقدم موقع معاك سيما مكتبة متنوعة من الأفلام والمسلسلات العربية والأجنبية.',
  'استمتع بتجربة مشاهدة مريحة مع افلام معاك سيما بجودات متعددة.',
  'يوفر معاك سيما أحدث الأفلام والمسلسلات مع تحديث مستمر للمحتوى.',
  'يمكنك متابعة مسلسلات معاك سيما بسهولة ومن أي جهاز.',
  'يُعد معاك سيما من المواقع المميزة لعشاق الأفلام والمسلسلات.',
  'مشاهدة الأفلام والمسلسلات على موقع معاك سيما تتم بدون تعقيد.',
  'يضم معاك سيما مجموعة كبيرة من الأعمال الجديدة والقديمة.',
  'يتم تحديث افلام معاك سيما بشكل دوري لتقديم الأفضل دائمًا.',
  'يوفر موقع معاك سيما تجربة مشاهدة سلسة وسريعة.',
  'استكشف مكتبة مسلسلات معاك سيما المتنوعة في جميع التصنيفات.',
  'يتم إضافة حلقات المسلسلات الجديدة على معاك سيما أولًا بأول.',
  'يحرص موقع معاك سيما على تقديم محتوى يناسب جميع الأذواق.',
  'معاك سيما يوفر مشاهدة مباشرة للأفلام والمسلسلات أونلاين.',
  'يتميز معاك سيما بسهولة الاستخدام وسرعة التصفح.',
  'يمكنك الاعتماد على معاك سيما لمتابعة أحدث الأعمال الفنية.',
  'يوفر افلام معاك سيما خيارات مشاهدة بجودات مختلفة.',
  'مسلسلات معاك سيما متاحة للمتابعة في أي وقت.',
  'يقدم موقع معاك سيما محتوى متجدد لمحبي السينما والدراما.',
  'يضم معاك سيما أعمالًا عربية وأجنبية مترجمة.',
  'يمكنك مشاهدة الأفلام والمسلسلات عبر موقع ma3ak بسهولة.',
  'يركز موقع معاك سيما على راحة المستخدم أثناء المشاهدة.',
  'يتم تنظيم المحتوى على معاك سيما لتسهيل الوصول إليه.',
  'معاك سيما من المواقع التي تجمع بين البساطة والتنوع.',
  'يوفر معاك سيما مكتبة غنية من الأفلام والمسلسلات.',
  'مشاهدة افلام معاك سيما تتم عبر سيرفرات سريعة.',
  'يتيح موقع معاك سيما متابعة المسلسلات دون تقطيع.',
  'يتم تحديث محتوى معاك سيما باستمرار لإرضاء الزوار.',
  'يجمع معاك سيما بين أحدث الأعمال والكلاسيكيات.',
  'يمكنك اكتشاف أفلام ومسلسلات جديدة عبر معاك سيما.',
  'يهدف موقع معاك سيما لتقديم تجربة مشاهدة مميزة.',
  'افلام معاك سيما مناسبة لمحبي الأكشن والدراما والكوميديا.',
  'مسلسلات معاك سيما تشمل أعمالًا متنوعة من مختلف الدول.',
  'يوفر معاك سيما واجهة بسيطة وسهلة الاستخدام.',
  'يمكنك الاستمتاع بالمحتوى عبر معاك سيما في أي وقت.',
  'يهتم معاك سيما بتقديم تجربة مشاهدة مستقرة.',
  'معاك سيما خيار مناسب لمتابعة الأفلام والمسلسلات.',
  'يتم إضافة محتوى جديد إلى معاك سيما بشكل منتظم.',
  'يضم موقع معاك سيما مكتبة متجددة لمحبي المشاهدة أونلاين.',
  'يمكنك متابعة أحدث الإصدارات عبر افلام معاك سيما.',
  'معاك سيما منصة مشاهدة تجمع بين التنوع وسهولة الوصول.',
];

export const GENRES_LIST = [
  { name: 'أكشن',       slug: 'action',      icon: '🔥' },
  { name: 'دراما',      slug: 'drama',       icon: '🎭' },
  { name: 'كوميدي',    slug: 'comedy',      icon: '😂' },
  { name: 'رعب',        slug: 'horror',      icon: '🎃' },
  { name: 'خيال علمي', slug: 'sci-fi',      icon: '🚀' },
  { name: 'رومانسية',  slug: 'romance',     icon: '❤️' },
  { name: 'أنمي',       slug: 'anime',       icon: '🉐' },
  { name: 'جريمة',      slug: 'crime',       icon: '🔍' },
  { name: 'غموض',       slug: 'mystery',     icon: '🕵️' },
  { name: 'وثائقي',    slug: 'documentary', icon: '🌎' },
  { name: 'عائلي',      slug: 'family',      icon: '🏠' },
  { name: 'مغامرة',    slug: 'adventure',   icon: '🗺️' },
  { name: 'إثارة',      slug: 'thriller',    icon: '⚡' },
];

export function getSeoPromo(id: string | number): string {
  const date = new Date();
  const dayOfYear = Math.floor(
    (date.getTime() - new Date(date.getFullYear(), 0, 0).getTime()) / 86400000
  );
  const hour = date.getHours();
  const idStr = id.toString();
  let hash = 0;
  for (let i = 0; i < idStr.length; i++) {
    hash = ((hash << 5) - hash) + idStr.charCodeAt(i);
    hash |= 0;
  }
  const index = Math.abs(dayOfYear + hour + hash) % SEO_PROMO_MESSAGES.length;
  return SEO_PROMO_MESSAGES[index];
}
