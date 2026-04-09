
const CDN_STATIC = "https://static.ma3ak.top";

async function testFetch() {
  console.log("--- بدء اختبار جلب البيانات من الـ CDN ---");
  try {
    // إضافة t=.. لمنع الـ caching أثناء الاختبار
    const url = `${CDN_STATIC}/movie/index.1.ndjson?t=${Date.now()}`;
    console.log(`جارِ فحص الرابط: ${url}`);
    
    const res = await fetch(url);
    if (!res.ok) {
      console.error(`خطأ: السيرفر رد بـ ${res.status}`);
      return;
    }
    
    const text = await res.text();
    const lines = text.split('\n').filter(l => l.trim());
    console.log(`تم العثور على: ${lines.length} أفلام في ملف الفهرس الأول.`);
    
    if (lines.length > 0) {
      const firstMovie = JSON.parse(lines[0]);
      console.log(`آخر فيلم مضاف في الفهرس هو: ${firstMovie.title} (ID: ${firstMovie.id})`);
    }

  } catch (e) {
    console.error("فشل الاتصال بـ CDN:", e);
  }
  console.log("--- انتهى الاختبار ---");
}

testFetch();
