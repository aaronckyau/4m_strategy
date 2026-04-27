const CACHE_NAME = '4m-datalab-v2.3-chart-fix';
const STATIC_ASSETS = [
  '/',
  '/static/css/base.css',
  '/static/css/terminal.css',
  '/static/js/utils.js',
  '/static/js/nav.js',
  '/static/js/terminal.js',
  '/static/images/favicon.svg',
];

// 安裝：預快取靜態資源
self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then((cache) => cache.addAll(STATIC_ASSETS))
      .catch((err) => console.warn('SW: precache failed', err))
  );
  self.skipWaiting();
});

// 啟動：清除舊版快取
self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((k) => k !== CACHE_NAME).map((k) => caches.delete(k)))
    )
  );
  self.clients.claim();
});

// 攔截請求：Network First（API / 頁面），Cache First（靜態資源）
self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url);

  // 只處理同源 GET 請求
  if (event.request.method !== 'GET') return;

  // 靜態資源：Cache First
  if (url.pathname.startsWith('/static/')) {
    event.respondWith(
      caches.match(event.request).then((cached) => cached || fetch(event.request))
    );
    return;
  }

  // API 請求不快取
  if (url.pathname.startsWith('/api/') || url.pathname.startsWith('/analyze/')) {
    return;
  }

  // 頁面：Network First
  event.respondWith(
    fetch(event.request)
      .then((response) => {
        if (response.status === 200) {
          var clone = response.clone();
          caches.open(CACHE_NAME).then((cache) => cache.put(event.request, clone));
        }
        return response;
      })
      .catch(() => caches.match(event.request))
  );
});
