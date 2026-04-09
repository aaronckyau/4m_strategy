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

    /* ---- 手機「更多」底部彈出選單 ---- */
    var moreBtn     = document.getElementById('mobile-more-btn');
    var moreSheet   = document.getElementById('mobile-more-sheet');
    var moreOverlay = document.getElementById('mobile-more-overlay');

    function openMore() {
        if (moreSheet)   moreSheet.classList.add('is-open');
        if (moreOverlay) moreOverlay.classList.add('is-visible');
        if (moreBtn)     moreBtn.setAttribute('aria-expanded', 'true');
    }
    function closeMore() {
        if (moreSheet)   moreSheet.classList.remove('is-open');
        if (moreOverlay) moreOverlay.classList.remove('is-visible');
        if (moreBtn)     moreBtn.setAttribute('aria-expanded', 'false');
    }

    if (moreBtn) {
        moreBtn.addEventListener('click', function () {
            moreSheet && moreSheet.classList.contains('is-open') ? closeMore() : openMore();
        });
    }
    if (moreOverlay) {
        moreOverlay.addEventListener('click', closeMore);
    }

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
    }
});
