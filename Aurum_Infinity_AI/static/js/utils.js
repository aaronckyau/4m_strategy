/**
 * utils.js - 共用工具函式
 * 供 terminal.js、ipo.js 等前端模組共用
 */

/**
 * HTML 跳脫（防 XSS）
 * @param {*} s - 要跳脫的值
 * @returns {string} 跳脫後的安全字串
 */
function escHtml(s) {
    if (s == null) return '';
    var d = document.createElement('div');
    d.textContent = String(s);
    return d.innerHTML;
}

/**
 * HTML 屬性值跳脫（含單引號）
 * @param {*} s - 要跳脫的值
 * @returns {string} 跳脫後的安全屬性值
 */
function escAttr(s) {
    return escHtml(s).replace(/'/g, '&#39;');
}

/**
 * 基本 HTML 淨化 — 移除 script 標籤和危險事件屬性
 * 適用於信任來源（自家 API）的 HTML 內容，提供額外防護層
 * @param {string} html - 要淨化的 HTML
 * @returns {string} 淨化後的 HTML
 */
function sanitizeHtml(html) {
    if (!html) return '';
    return html
        .replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi, '')
        .replace(/<iframe\b[^>]*>.*?<\/iframe>/gi, '')
        .replace(/\bon\w+\s*=\s*["'][^"']*["']/gi, '')
        .replace(/\bon\w+\s*=\s*[^\s>]*/gi, '')
        .replace(/javascript\s*:/gi, 'blocked:');
}
