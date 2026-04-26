document.addEventListener('DOMContentLoaded', function () {
    var cards     = document.querySelectorAll('.ins-signal-card');
    var tabs      = document.querySelectorAll('.ins-stab');
    var emptyEl   = document.getElementById('ins-search-empty');
    var searchInput = document.getElementById('ins-search-input');
    var clearBtn  = document.getElementById('ins-search-clear');

    var _activeTab = 'buy';   // default tab
    var _searchQ   = '';

    // ── Apply combined filter (tab + search) ──
    function applyFilter() {
        var q = _searchQ.trim().toLowerCase();
        var visible = 0;
        cards.forEach(function (card) {
            var sideMatch   = (_activeTab === 'all' || card.dataset.side === _activeTab);
            var searchMatch = !q || card.dataset.ticker === q;
            var show = sideMatch && searchMatch;
            card.style.display = show ? '' : 'none';
            if (show) visible++;
        });
        if (emptyEl) emptyEl.classList.toggle('hidden', visible > 0 || !q);
    }

    // ── Card expand/collapse ──
    document.querySelectorAll('.ins-card-header').forEach(function (btn) {
        btn.addEventListener('click', function () {
            var expanded = btn.getAttribute('aria-expanded') === 'true';
            btn.setAttribute('aria-expanded', expanded ? 'false' : 'true');
            var detail = btn.closest('.ins-signal-card').querySelector('.ins-card-detail');
            if (detail) detail.hidden = expanded;
        });
    });

    // ── Buy/Sell/All tab filter ──
    tabs.forEach(function (tab) {
        tab.addEventListener('click', function () {
            tabs.forEach(function (t) { t.classList.remove('is-active'); });
            tab.classList.add('is-active');
            _activeTab = tab.dataset.filter;
            applyFilter();
        });
    });

    // ── Search input ──
    if (searchInput) {
        searchInput.addEventListener('input', function () {
            _searchQ = searchInput.value;
            if (clearBtn) clearBtn.hidden = !_searchQ;
            applyFilter();
        });
        searchInput.addEventListener('keydown', function (e) {
            if (e.key === 'Escape') {
                searchInput.value = '';
                _searchQ = '';
                if (clearBtn) clearBtn.hidden = true;
                applyFilter();
                searchInput.blur();
            }
        });
    }

    // ── Clear button ──
    if (clearBtn) {
        clearBtn.addEventListener('click', function () {
            if (searchInput) { searchInput.value = ''; searchInput.focus(); }
            _searchQ = '';
            clearBtn.hidden = true;
            applyFilter();
        });
    }
});
