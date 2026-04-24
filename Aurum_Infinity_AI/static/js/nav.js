/**
 * nav.js - 桌面摺疊 + 手機底部「更多」選單 + Active 高亮
 * 桌面：#app-nav.is-collapsed = 摺疊（只顯示 K + 圖示）
 * 手機：#mobile-more-sheet = 底部彈出選單
 */
document.addEventListener('DOMContentLoaded', function () {
    var nav         = document.getElementById('app-nav');
    var overlay     = document.getElementById('nav-overlay');
    var collapseBtn = document.getElementById('nav-collapse-btn');
    var logoK       = nav ? nav.querySelector('.logo-k') : null;
    var appMain     = document.getElementById('app-main');

    function resetMainScroll() {
        if (appMain) {
            appMain.scrollTop = 0;
        }
        window.scrollTo({ top: 0, left: 0, behavior: 'auto' });
    }

    function shouldResetForLink(link) {
        if (!link || !link.href) return false;
        if (link.hasAttribute('download')) return false;
        if ((link.getAttribute('target') || '').toLowerCase() === '_blank') return false;

        var rawHref = link.getAttribute('href') || '';
        if (!rawHref || rawHref.charAt(0) === '#') return false;
        if (/^\s*javascript:/i.test(rawHref)) return false;

        try {
            var url = new URL(link.href, window.location.href);
            return url.origin === window.location.origin;
        } catch (error) {
            return false;
        }
    }

    if ('scrollRestoration' in window.history) {
        window.history.scrollRestoration = 'manual';
    }

    /* ---- 手機「更多」底部彈出選單 ---- */
    var moreBtn     = document.getElementById('mobile-more-btn');
    var moreSheet   = document.getElementById('mobile-more-sheet');
    var moreOverlay = document.getElementById('mobile-more-overlay');
    var themeMeta   = document.querySelector('meta[name="theme-color"]');

    function openMore() {
        if (moreSheet)   moreSheet.classList.add('is-open');
        if (moreOverlay) moreOverlay.classList.add('is-visible');
        if (moreBtn)     moreBtn.setAttribute('aria-expanded', 'true');
        if (moreBtn)     moreBtn.classList.add('active');
        if (moreSheet)   moreSheet.setAttribute('aria-hidden', 'false');
        if (moreOverlay) moreOverlay.setAttribute('aria-hidden', 'false');
        document.body.classList.add('has-mobile-sheet-open');
    }
    function closeMore() {
        if (moreSheet)   moreSheet.classList.remove('is-open');
        if (moreOverlay) moreOverlay.classList.remove('is-visible');
        if (moreBtn)     moreBtn.setAttribute('aria-expanded', 'false');
        if (moreBtn)     moreBtn.classList.remove('active');
        if (moreSheet)   moreSheet.setAttribute('aria-hidden', 'true');
        if (moreOverlay) moreOverlay.setAttribute('aria-hidden', 'true');
        document.body.classList.remove('has-mobile-sheet-open');
    }

    if (moreBtn) {
        moreBtn.addEventListener('click', function () {
            moreSheet && moreSheet.classList.contains('is-open') ? closeMore() : openMore();
        });
    }
    if (moreOverlay) {
        moreOverlay.addEventListener('click', closeMore);
    }
    document.addEventListener('keydown', function (e) {
        if (e.key === 'Escape') closeMore();
    });

    /* ---- 桌面摺疊 ---- */
    var COLLAPSED_KEY = 'nav-collapsed';
    var isDesktop = function () { return window.innerWidth >= 768; };

    function collapseNav() {
        if (nav) nav.classList.add('is-collapsed');
        localStorage.setItem(COLLAPSED_KEY, '1');
    }
    function expandNav() {
        if (nav) nav.classList.remove('is-collapsed');
        localStorage.setItem(COLLAPSED_KEY, '0');
    }

    if (collapseBtn) {
        collapseBtn.addEventListener('click', function () {
            if (isDesktop()) collapseNav();
        });
    }
    if (logoK) {
        logoK.addEventListener('click', function () {
            if (isDesktop() && nav && nav.classList.contains('is-collapsed')) {
                expandNav();
            }
        });
    }

    if (isDesktop() && localStorage.getItem(COLLAPSED_KEY) === '1') {
        if (nav) nav.classList.add('is-collapsed');
    }

    /* ---- resize 時關閉手機選單 ---- */
    window.addEventListener('resize', function () {
        if (window.innerWidth >= 768) closeMore();
    });

    document.addEventListener('click', function (event) {
        if (event.defaultPrevented || event.button !== 0) return;
        if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;

        var link = event.target.closest('a');
        if (!shouldResetForLink(link)) return;

        resetMainScroll();
    }, true);

    /* ---- 深色模式 ---- */
    var THEME_KEY = 'aurum-theme';
    var htmlEl = document.documentElement;

    function applyTheme(theme) {
        htmlEl.setAttribute('data-theme', theme);
        localStorage.setItem(THEME_KEY, theme);
        // 更新圖示
        var isDark = theme === 'dark';
        ['', 'm-'].forEach(function (prefix) {
            var lightIcon = document.getElementById(prefix + 'theme-icon-light');
            var darkIcon  = document.getElementById(prefix + 'theme-icon-dark');
            if (lightIcon) lightIcon.classList.toggle('hidden', isDark);
            if (darkIcon)  darkIcon.classList.toggle('hidden', !isDark);
        });
        // 更新文字（讀取 I18N 或用預設值）
        var i18n = window.I18N || {};
        var label = isDark ? (i18n.nav_theme_light || '淺色模式') : (i18n.nav_theme_dark || '深色模式');
        var tl = document.querySelector('.theme-label');
        if (tl) tl.textContent = label;
        var ml = document.getElementById('m-theme-label');
        if (ml) ml.textContent = label;
        if (themeMeta) {
            themeMeta.setAttribute('content', isDark ? '#131210' : '#f6f3ef');
        }

        // 更新 TradingView 圖表顏色
        if (window._chart) {
            window._chart.applyOptions({
                layout: {
                    background: { type: 'solid', color: isDark ? '#242428' : '#ffffff' },
                    textColor: isDark ? '#777' : '#999',
                },
                grid: {
                    vertLines: { color: isDark ? '#333' : '#f0f0f0' },
                    horzLines: { color: isDark ? '#333' : '#f0f0f0' },
                },
                rightPriceScale: { borderColor: isDark ? '#444' : '#e5e5e5' },
                timeScale:       { borderColor: isDark ? '#444' : '#e5e5e5' },
            });
        }
    }

    function toggleTheme() {
        var current = htmlEl.getAttribute('data-theme') || 'corporate';
        applyTheme(current === 'dark' ? 'corporate' : 'dark');
    }
    window._toggleTheme = toggleTheme;

    // 初始化
    var savedTheme = localStorage.getItem(THEME_KEY);
    if (savedTheme) applyTheme(savedTheme);

    var themeBtn = document.getElementById('theme-toggle-btn');
    if (themeBtn) themeBtn.addEventListener('click', toggleTheme);

    /* ---- 語言切換攔截 ---- */
    window._switchLang = function (lang) {
        if (typeof window.switchLanguage === 'function') {
            window.switchLanguage(lang);
            return false; // 阻止 <a> 的預設跳轉
        }
        return true; // 非分析頁面，走正常連結
    };

    /* ---- Active 高亮 ---- */
    var currentPage = document.body.dataset.page;
    if (currentPage) {
        document.querySelectorAll('.nav-item[data-page]').forEach(function (item) {
            item.classList.toggle('active', item.dataset.page === currentPage);
        });
        document.querySelectorAll('.mobile-nav-item[data-page]').forEach(function (item) {
            item.classList.toggle('active', item.dataset.page === currentPage);
        });
        document.querySelectorAll('.more-sheet-item[data-page]').forEach(function (item) {
            item.classList.toggle('active', item.dataset.page === currentPage);
        });
    }

    resetMainScroll();
    window.addEventListener('pageshow', resetMainScroll);
    window.addEventListener('beforeunload', resetMainScroll);
});
