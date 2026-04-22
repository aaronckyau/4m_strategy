document.addEventListener('DOMContentLoaded', function () {
    document.querySelectorAll('[data-insider-row]').forEach(function (row) {
        var button = row.querySelector('.insider-signal-main');
        var detail = row.querySelector('.insider-signal-detail');
        if (!button || !detail) return;

        button.addEventListener('click', function () {
            var isExpanded = button.getAttribute('aria-expanded') === 'true';
            button.setAttribute('aria-expanded', isExpanded ? 'false' : 'true');
            detail.hidden = isExpanded;
        });
    });
});
