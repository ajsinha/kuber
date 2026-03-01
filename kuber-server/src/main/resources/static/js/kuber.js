/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Kuber Cache - Custom JavaScript v2.6.2
 */

(function() {
    'use strict';

    // ============================================
    // Utility Functions
    // ============================================
    
    const Kuber = {
        formatNumber: function(num) {
            return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
        },
        formatBytes: function(bytes, decimals = 2) {
            if (bytes === 0) return '0 Bytes';
            const k = 1024;
            const dm = decimals < 0 ? 0 : decimals;
            const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
            const i = Math.floor(Math.log(bytes) / Math.log(k));
            return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
        },
        formatDuration: function(seconds) {
            if (seconds < 60) return seconds + 's';
            if (seconds < 3600) return Math.floor(seconds / 60) + 'm ' + (seconds % 60) + 's';
            const hours = Math.floor(seconds / 3600);
            const mins = Math.floor((seconds % 3600) / 60);
            return hours + 'h ' + mins + 'm';
        },
        showLoading: function() {
            const overlay = document.createElement('div');
            overlay.className = 'loading-overlay';
            overlay.id = 'loadingOverlay';
            overlay.innerHTML = '<div class="spinner"></div>';
            document.body.appendChild(overlay);
        },
        hideLoading: function() {
            const overlay = document.getElementById('loadingOverlay');
            if (overlay) overlay.remove();
        },
        showToast: function(message, type = 'info') {
            const toast = document.createElement('div');
            toast.className = `alert alert-${type} position-fixed top-0 end-0 m-3 fade-in`;
            toast.style.zIndex = '9999';
            toast.innerHTML = `<button type="button" class="btn-close float-end" onclick="this.parentElement.remove()"></button>${message}`;
            document.body.appendChild(toast);
            setTimeout(() => toast.remove(), 5000);
        },
        copyToClipboard: function(text) {
            navigator.clipboard.writeText(text).then(() => {
                this.showToast('Copied to clipboard!', 'success');
            }).catch(() => {
                this.showToast('Failed to copy', 'danger');
            });
        },
        confirm: function(message) {
            return new Promise((resolve) => {
                const modal = document.createElement('div');
                modal.className = 'modal fade';
                modal.innerHTML = `
                    <div class="modal-dialog modal-dialog-centered">
                        <div class="modal-content">
                            <div class="modal-header">
                                <h5 class="modal-title">Confirm</h5>
                                <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                            </div>
                            <div class="modal-body">${message}</div>
                            <div class="modal-footer">
                                <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancel</button>
                                <button type="button" class="btn btn-danger" id="confirmBtn">Confirm</button>
                            </div>
                        </div>
                    </div>`;
                document.body.appendChild(modal);
                const bsModal = new bootstrap.Modal(modal);
                bsModal.show();
                modal.querySelector('#confirmBtn').addEventListener('click', () => { bsModal.hide(); resolve(true); });
                modal.addEventListener('hidden.bs.modal', () => { modal.remove(); resolve(false); });
            });
        }
    };
    
    // ============================================
    // JSON Formatter
    // ============================================
    
    const JsonFormatter = {
        format: function(json) {
            if (typeof json === 'string') { try { json = JSON.parse(json); } catch (e) { return json; } }
            return JSON.stringify(json, null, 2);
        },
        highlight: function(json) {
            if (typeof json === 'string') { try { json = JSON.parse(json); } catch (e) { return this.escapeHtml(json); } }
            return this.syntaxHighlight(JSON.stringify(json, null, 2));
        },
        syntaxHighlight: function(str) {
            str = this.escapeHtml(str);
            return str.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, 
                function(match) {
                    let cls = 'json-number';
                    if (/^"/.test(match)) { cls = /:$/.test(match) ? 'json-key' : 'json-string'; }
                    else if (/true|false/.test(match)) { cls = 'json-boolean'; }
                    else if (/null/.test(match)) { cls = 'json-null'; }
                    return '<span class="' + cls + '">' + match + '</span>';
                });
        },
        escapeHtml: function(str) {
            const div = document.createElement('div');
            div.appendChild(document.createTextNode(str));
            return div.innerHTML;
        }
    };
    
    // ============================================
    // API Client
    // ============================================
    
    const ApiClient = {
        baseUrl: '/api',
        async request(method, endpoint, data = null) {
            const options = { method, headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' } };
            if (data) options.body = JSON.stringify(data);
            const response = await fetch(this.baseUrl + endpoint, options);
            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
            return response.json();
        },
        getInfo: function() { return this.request('GET', '/info'); },
        getRegions: function() { return this.request('GET', '/regions'); },
        getRegion: function(name) { return this.request('GET', '/regions/' + encodeURIComponent(name)); },
        createRegion: function(name, description) { return this.request('POST', '/regions', { name, description }); },
        deleteRegion: function(name) { return this.request('DELETE', '/regions/' + encodeURIComponent(name)); },
        getKeys: function(region, pattern = '*') { return this.request('GET', `/cache/${encodeURIComponent(region)}/keys?pattern=${encodeURIComponent(pattern)}`); },
        getValue: function(region, key) { return this.request('GET', `/cache/${encodeURIComponent(region)}/${encodeURIComponent(key)}`); },
        setValue: function(region, key, value, ttl = -1) { return this.request('PUT', `/cache/${encodeURIComponent(region)}/${encodeURIComponent(key)}`, { value, ttl }); },
        deleteValue: function(region, key) { return this.request('DELETE', `/cache/${encodeURIComponent(region)}/${encodeURIComponent(key)}`); },
        searchJson: function(region, query) { return this.request('POST', `/cache/${encodeURIComponent(region)}/search`, { query }); }
    };
    
    // ============================================
    // KuberTable — Reusable sortable, searchable,
    // paginated table component (no DataTables)
    // ============================================
    //
    // Usage:
    //   Wrap any <table> inside a container with:
    //     data-kuber-table="tableName"
    //     data-page-sizes="10,25,50,All"       (optional, default "10,25,50,All")
    //     data-default-page-size="25"           (optional, default 10)
    //
    //   On <th> headers, add:
    //     data-sort="string"  |  data-sort="number"  |  data-sort="date"
    //   Headers without data-sort are not sortable.
    //
    //   On data <tr> rows, add class "data-row".
    //   Optional: a <tr class="empty-row"> shown when 0 results.
    //
    // The component auto-generates:
    //   • Search input
    //   • Page-size dropdown
    //   • Info text ("Showing 1-10 of 42")
    //   • Pagination links with ellipsis
    //   • Click-to-sort headers with icons
    //

    class KuberTable {
        constructor(container) {
            this.container = container;
            this.name = container.dataset.kuberTable;
            this.table = container.querySelector('table');
            if (!this.table) return;

            this.pageSizes = (container.dataset.pageSizes || '10,25,50,All').split(',').map(s => s.trim());
            this.pageSize = parseInt(container.dataset.defaultPageSize) || parseInt(this.pageSizes[0]) || 10;
            this.currentPage = 1;
            this.sortCol = -1;
            this.sortDir = 'asc';
            this.searchTerm = '';

            this._allRows = Array.from(this.table.querySelectorAll('tbody tr.data-row'));
            this._emptyRow = this.table.querySelector('tbody tr.empty-row');

            this._buildControls();
            this._bindSortHeaders();
            this._refresh();
        }

        // ---- Build search + page-size + info + pagination controls ----
        _buildControls() {
            // Top bar: search + page-size + info
            const topBar = document.createElement('div');
            topBar.className = 'row align-items-center g-2 mb-2 kuber-table-controls';
            topBar.innerHTML = `
                <div class="col-auto">
                    <div class="d-flex align-items-center">
                        <label class="me-2 text-muted small">Show:</label>
                        <select class="form-select form-select-sm kt-page-size" style="width:auto">
                            ${this.pageSizes.map(s => {
                                const val = s.toLowerCase() === 'all' ? 'all' : s;
                                const sel = (parseInt(s) === this.pageSize || (s.toLowerCase() === 'all' && this.pageSize >= 99999)) ? ' selected' : '';
                                return `<option value="${val}"${sel}>${s}</option>`;
                            }).join('')}
                        </select>
                        <span class="ms-2 text-muted small">entries</span>
                    </div>
                </div>
                <div class="col">
                    <div class="input-group input-group-sm" style="max-width:260px">
                        <span class="input-group-text"><i class="fas fa-search"></i></span>
                        <input type="text" class="form-control kt-search" placeholder="Search...">
                        <button class="btn btn-outline-secondary kt-search-clear" type="button" title="Clear"><i class="fas fa-times"></i></button>
                    </div>
                </div>
                <div class="col-auto">
                    <span class="text-muted small kt-info"></span>
                </div>`;

            // Bottom bar: pagination
            const bottomBar = document.createElement('div');
            bottomBar.className = 'row align-items-center mt-2 kuber-table-footer';
            bottomBar.innerHTML = `
                <div class="col-auto">
                    <span class="text-muted small kt-info-bottom"></span>
                </div>
                <div class="col">
                    <nav class="kt-pagination-nav d-flex justify-content-end">
                        <ul class="pagination pagination-sm mb-0 kt-pagination"></ul>
                    </nav>
                </div>`;

            // Insert before & after table
            this.table.parentNode.insertBefore(topBar, this.table);
            this.table.parentNode.appendChild(bottomBar);

            // Cache refs
            this._searchInput = topBar.querySelector('.kt-search');
            this._pageSizeSelect = topBar.querySelector('.kt-page-size');
            this._infoTop = topBar.querySelector('.kt-info');
            this._infoBottom = bottomBar.querySelector('.kt-info-bottom');
            this._pagination = bottomBar.querySelector('.kt-pagination');
            this._searchClear = topBar.querySelector('.kt-search-clear');

            // Events
            this._searchInput.addEventListener('input', () => { this.searchTerm = this._searchInput.value; this.currentPage = 1; this._refresh(); });
            this._searchClear.addEventListener('click', () => { this._searchInput.value = ''; this.searchTerm = ''; this.currentPage = 1; this._refresh(); });
            this._pageSizeSelect.addEventListener('change', () => {
                const v = this._pageSizeSelect.value;
                this.pageSize = v === 'all' ? 999999 : parseInt(v);
                this.currentPage = 1;
                this._refresh();
            });
        }

        // ---- Make headers clickable for sorting ----
        _bindSortHeaders() {
            const headers = this.table.querySelectorAll('thead th[data-sort]');
            headers.forEach((th, _) => {
                const colIdx = Array.from(th.parentNode.children).indexOf(th);
                th.style.cursor = 'pointer';
                th.style.userSelect = 'none';
                
                // Add sort icon
                let icon = th.querySelector('.kt-sort-icon');
                if (!icon) {
                    icon = document.createElement('i');
                    icon.className = 'fas fa-sort ms-1 text-muted kt-sort-icon';
                    icon.style.fontSize = '0.75em';
                    th.appendChild(icon);
                }

                th.addEventListener('click', (e) => {
                    if (e.target.closest('a, button, input')) return; // don't sort on link/button clicks
                    if (this.sortCol === colIdx) {
                        this.sortDir = this.sortDir === 'asc' ? 'desc' : 'asc';
                    } else {
                        this.sortCol = colIdx;
                        this.sortDir = 'asc';
                    }
                    this._updateSortIcons();
                    this.currentPage = 1;
                    this._refresh();
                });
            });
        }

        _updateSortIcons() {
            this.table.querySelectorAll('thead th[data-sort]').forEach(th => {
                const icon = th.querySelector('.kt-sort-icon');
                const colIdx = Array.from(th.parentNode.children).indexOf(th);
                if (!icon) return;
                if (colIdx === this.sortCol) {
                    icon.className = 'fas fa-sort-' + (this.sortDir === 'asc' ? 'up' : 'down') + ' ms-1 text-primary kt-sort-icon';
                    icon.style.fontSize = '0.75em';
                } else {
                    icon.className = 'fas fa-sort ms-1 text-muted kt-sort-icon';
                    icon.style.fontSize = '0.75em';
                }
            });
        }

        // ---- Filter + Sort + Paginate ----
        _refresh() {
            let rows = [...this._allRows];

            // Search
            if (this.searchTerm) {
                const term = this.searchTerm.toLowerCase();
                rows = rows.filter(r => r.textContent.toLowerCase().includes(term));
            }

            // Sort
            if (this.sortCol >= 0) {
                const sortType = this._getSortType(this.sortCol);
                rows.sort((a, b) => {
                    const aCell = a.children[this.sortCol];
                    const bCell = b.children[this.sortCol];
                    if (!aCell || !bCell) return 0;
                    let aVal = (aCell.dataset.sortValue || aCell.textContent).trim();
                    let bVal = (bCell.dataset.sortValue || bCell.textContent).trim();
                    let result = 0;
                    if (sortType === 'number') {
                        result = (parseFloat(aVal.replace(/[^0-9.\-]/g, '')) || 0) - (parseFloat(bVal.replace(/[^0-9.\-]/g, '')) || 0);
                    } else if (sortType === 'date') {
                        result = (new Date(aVal) || 0) - (new Date(bVal) || 0);
                    } else {
                        result = aVal.localeCompare(bVal, undefined, { numeric: true, sensitivity: 'base' });
                    }
                    return this.sortDir === 'desc' ? -result : result;
                });
            }

            // Pagination
            const total = rows.length;
            const totalPages = Math.max(1, Math.ceil(total / this.pageSize));
            if (this.currentPage > totalPages) this.currentPage = totalPages;
            const start = (this.currentPage - 1) * this.pageSize;
            const end = Math.min(start + this.pageSize, total);

            // Hide all, show page
            this._allRows.forEach(r => r.style.display = 'none');
            if (this._emptyRow) this._emptyRow.style.display = 'none';

            rows.forEach((r, i) => {
                r.style.display = (i >= start && i < end) ? '' : 'none';
            });

            if (total === 0 && this._emptyRow) {
                this._emptyRow.style.display = '';
            }

            // Info
            const info = total === 0 ? 'No matching entries'
                : `Showing ${start + 1}\u2013${end} of ${total}`;
            if (this._infoTop) this._infoTop.textContent = info;
            if (this._infoBottom) this._infoBottom.textContent = info;

            // Pagination
            this._renderPagination(totalPages);
        }

        _getSortType(colIdx) {
            const th = this.table.querySelectorAll('thead th')[colIdx];
            return th ? (th.dataset.sort || 'string') : 'string';
        }

        _renderPagination(totalPages) {
            const ul = this._pagination;
            ul.innerHTML = '';
            if (totalPages <= 1) return;

            const mkLi = (label, page, disabled, active) => {
                const li = document.createElement('li');
                li.className = 'page-item' + (disabled ? ' disabled' : '') + (active ? ' active' : '');
                const a = document.createElement('a');
                a.className = 'page-link';
                a.href = '#';
                a.innerHTML = label;
                if (!disabled && !active) {
                    a.addEventListener('click', (e) => { e.preventDefault(); this.currentPage = page; this._refresh(); });
                }
                li.appendChild(a);
                return li;
            };

            // Prev
            ul.appendChild(mkLi('<i class="fas fa-chevron-left"></i>', this.currentPage - 1, this.currentPage === 1, false));

            // Pages with ellipsis
            const range = 2;
            for (let i = 1; i <= totalPages; i++) {
                if (i === 1 || i === totalPages || (i >= this.currentPage - range && i <= this.currentPage + range)) {
                    ul.appendChild(mkLi(i, i, false, i === this.currentPage));
                } else if (i === this.currentPage - range - 1 || i === this.currentPage + range + 1) {
                    ul.appendChild(mkLi('&hellip;', i, true, false));
                }
            }

            // Next
            ul.appendChild(mkLi('<i class="fas fa-chevron-right"></i>', this.currentPage + 1, this.currentPage === totalPages, false));
        }

        // Public: reload rows (e.g. after AJAX adds/removes)
        reload() {
            this._allRows = Array.from(this.table.querySelectorAll('tbody tr.data-row'));
            this._emptyRow = this.table.querySelector('tbody tr.empty-row');
            this._refresh();
        }
    }

    // Registry
    const _kuberTables = {};

    // ============================================
    // Auto-refresh Stats (Dashboard)
    // ============================================
    
    let statsInterval = null;
    
    function startStatsRefresh(intervalMs = 5000) {
        if (statsInterval) clearInterval(statsInterval);
        statsInterval = setInterval(async () => {
            try {
                const info = await ApiClient.getInfo();
                updateDashboardStats(info);
            } catch (error) {
                console.error('Failed to refresh stats:', error);
            }
        }, intervalMs);
    }
    
    function stopStatsRefresh() {
        if (statsInterval) { clearInterval(statsInterval); statsInterval = null; }
    }
    
    function updateDashboardStats(info) {
        const totalEntries = document.querySelector('[data-stat="totalEntries"]');
        if (totalEntries) totalEntries.textContent = Kuber.formatNumber(info.totalEntries || 0);
        const connections = document.querySelector('[data-stat="connections"]');
        if (connections) connections.textContent = info.activeConnections || 0;
    }
    
    // ============================================
    // Initialize
    // ============================================
    
    document.addEventListener('DOMContentLoaded', function() {
        // Tooltips
        const tooltipTriggerList = document.querySelectorAll('[data-bs-toggle="tooltip"]');
        [...tooltipTriggerList].map(el => new bootstrap.Tooltip(el));
        
        // Popovers
        const popoverTriggerList = document.querySelectorAll('[data-bs-toggle="popover"]');
        [...popoverTriggerList].map(el => new bootstrap.Popover(el));
        
        // Auto-dismiss alerts
        document.querySelectorAll('.alert-dismissible').forEach(alert => {
            const delay = parseInt(alert.dataset.dismissDelay) || 5000;
            setTimeout(() => {
                const bsAlert = bootstrap.Alert.getOrCreateInstance(alert);
                bsAlert.close();
            }, delay);
        });
        
        // Format JSON in pre elements
        document.querySelectorAll('pre code.json').forEach(block => {
            try {
                const json = JSON.parse(block.textContent);
                block.innerHTML = JsonFormatter.highlight(json);
            } catch (e) { }
        });
        
        // Auto-init KuberTable
        document.querySelectorAll('[data-kuber-table]').forEach(container => {
            const name = container.dataset.kuberTable;
            _kuberTables[name] = new KuberTable(container);
        });
        
        // Start stats refresh on dashboard
        if (document.querySelector('[data-page="dashboard"]')) {
            startStatsRefresh();
        }
        
        console.log('Kuber Cache UI v2.6.2 initialized');
    });
    
    window.addEventListener('beforeunload', function() { stopStatsRefresh(); });
    
    // Export to global scope
    window.Kuber = Kuber;
    window.JsonFormatter = JsonFormatter;
    window.ApiClient = ApiClient;
    window.KuberTable = KuberTable;
    window.KuberTables = _kuberTables;
    
    // ============================================
    // KuberTheme — Light/Dark Theme Toggle
    // ============================================
    
    const KuberTheme = {
        _key: 'kuber-theme',
        
        get: function() {
            return document.documentElement.getAttribute('data-bs-theme') || 'light';
        },
        
        set: function(theme) {
            document.documentElement.setAttribute('data-bs-theme', theme);
            localStorage.setItem(this._key, theme);
            this._updateIcons(theme);
            this._updateCharts(theme);
        },
        
        toggle: function() {
            this.set(this.get() === 'dark' ? 'light' : 'dark');
        },
        
        _updateIcons: function(theme) {
            const moon = document.getElementById('themeIconMoon');
            const sun = document.getElementById('themeIconSun');
            if (!moon || !sun) return;
            if (theme === 'dark') {
                moon.classList.add('d-none');
                sun.classList.remove('d-none');
            } else {
                moon.classList.remove('d-none');
                sun.classList.add('d-none');
            }
        },
        
        _updateCharts: function(theme) {
            // Update Chart.js instances if present (monitoring page)
            if (typeof Chart !== 'undefined' && Chart.instances) {
                const gridColor = theme === 'dark' ? 'rgba(255,255,255,0.08)' : 'rgba(0,0,0,0.1)';
                const tickColor = theme === 'dark' ? '#8b949e' : '#666';
                Object.values(Chart.instances).forEach(function(chart) {
                    if (chart.options.scales) {
                        Object.values(chart.options.scales).forEach(function(scale) {
                            if (scale.grid) scale.grid.color = gridColor;
                            if (scale.ticks) scale.ticks.color = tickColor;
                        });
                    }
                    if (chart.options.plugins && chart.options.plugins.legend && chart.options.plugins.legend.labels) {
                        chart.options.plugins.legend.labels.color = tickColor;
                    }
                    chart.update('none');
                });
            }
        },
        
        init: function() {
            // Icons already correct from head script; just sync icons
            this._updateIcons(this.get());
        }
    };
    
    window.KuberTheme = KuberTheme;
    
    // Init theme icons after DOM ready
    document.addEventListener('DOMContentLoaded', function() {
        KuberTheme.init();
    });
    
})();
