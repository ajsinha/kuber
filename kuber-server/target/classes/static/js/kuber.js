/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Kuber Cache - Custom JavaScript
 */

(function() {
    'use strict';

    // ============================================
    // Utility Functions
    // ============================================
    
    const Kuber = {
        // Format numbers with commas
        formatNumber: function(num) {
            return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
        },
        
        // Format bytes to human readable
        formatBytes: function(bytes, decimals = 2) {
            if (bytes === 0) return '0 Bytes';
            const k = 1024;
            const dm = decimals < 0 ? 0 : decimals;
            const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
            const i = Math.floor(Math.log(bytes) / Math.log(k));
            return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
        },
        
        // Format duration
        formatDuration: function(seconds) {
            if (seconds < 60) return seconds + 's';
            if (seconds < 3600) return Math.floor(seconds / 60) + 'm ' + (seconds % 60) + 's';
            const hours = Math.floor(seconds / 3600);
            const mins = Math.floor((seconds % 3600) / 60);
            return hours + 'h ' + mins + 'm';
        },
        
        // Show loading overlay
        showLoading: function() {
            const overlay = document.createElement('div');
            overlay.className = 'loading-overlay';
            overlay.id = 'loadingOverlay';
            overlay.innerHTML = '<div class="spinner"></div>';
            document.body.appendChild(overlay);
        },
        
        // Hide loading overlay
        hideLoading: function() {
            const overlay = document.getElementById('loadingOverlay');
            if (overlay) {
                overlay.remove();
            }
        },
        
        // Show toast notification
        showToast: function(message, type = 'info') {
            const toast = document.createElement('div');
            toast.className = `alert alert-${type} position-fixed top-0 end-0 m-3 fade-in`;
            toast.style.zIndex = '9999';
            toast.innerHTML = `
                <button type="button" class="btn-close float-end" onclick="this.parentElement.remove()"></button>
                ${message}
            `;
            document.body.appendChild(toast);
            
            setTimeout(() => {
                toast.remove();
            }, 5000);
        },
        
        // Copy to clipboard
        copyToClipboard: function(text) {
            navigator.clipboard.writeText(text).then(() => {
                this.showToast('Copied to clipboard!', 'success');
            }).catch(err => {
                this.showToast('Failed to copy', 'danger');
            });
        },
        
        // Confirm dialog
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
                    </div>
                `;
                document.body.appendChild(modal);
                
                const bsModal = new bootstrap.Modal(modal);
                bsModal.show();
                
                modal.querySelector('#confirmBtn').addEventListener('click', () => {
                    bsModal.hide();
                    resolve(true);
                });
                
                modal.addEventListener('hidden.bs.modal', () => {
                    modal.remove();
                    resolve(false);
                });
            });
        }
    };
    
    // ============================================
    // JSON Formatter
    // ============================================
    
    const JsonFormatter = {
        format: function(json) {
            if (typeof json === 'string') {
                try {
                    json = JSON.parse(json);
                } catch (e) {
                    return json;
                }
            }
            return JSON.stringify(json, null, 2);
        },
        
        highlight: function(json) {
            if (typeof json === 'string') {
                try {
                    json = JSON.parse(json);
                } catch (e) {
                    return this.escapeHtml(json);
                }
            }
            
            return this.syntaxHighlight(JSON.stringify(json, null, 2));
        },
        
        syntaxHighlight: function(str) {
            str = this.escapeHtml(str);
            return str.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, 
                function(match) {
                    let cls = 'json-number';
                    if (/^"/.test(match)) {
                        if (/:$/.test(match)) {
                            cls = 'json-key';
                        } else {
                            cls = 'json-string';
                        }
                    } else if (/true|false/.test(match)) {
                        cls = 'json-boolean';
                    } else if (/null/.test(match)) {
                        cls = 'json-null';
                    }
                    return '<span class="' + cls + '">' + match + '</span>';
                }
            );
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
            const options = {
                method: method,
                headers: {
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                }
            };
            
            if (data) {
                options.body = JSON.stringify(data);
            }
            
            const response = await fetch(this.baseUrl + endpoint, options);
            
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            return response.json();
        },
        
        // Server info
        getInfo: function() {
            return this.request('GET', '/info');
        },
        
        // Regions
        getRegions: function() {
            return this.request('GET', '/regions');
        },
        
        getRegion: function(name) {
            return this.request('GET', '/regions/' + encodeURIComponent(name));
        },
        
        createRegion: function(name, description) {
            return this.request('POST', '/regions', { name, description });
        },
        
        deleteRegion: function(name) {
            return this.request('DELETE', '/regions/' + encodeURIComponent(name));
        },
        
        // Cache operations
        getKeys: function(region, pattern = '*') {
            return this.request('GET', `/cache/${encodeURIComponent(region)}/keys?pattern=${encodeURIComponent(pattern)}`);
        },
        
        getValue: function(region, key) {
            return this.request('GET', `/cache/${encodeURIComponent(region)}/${encodeURIComponent(key)}`);
        },
        
        setValue: function(region, key, value, ttl = -1) {
            return this.request('PUT', `/cache/${encodeURIComponent(region)}/${encodeURIComponent(key)}`, { value, ttl });
        },
        
        deleteValue: function(region, key) {
            return this.request('DELETE', `/cache/${encodeURIComponent(region)}/${encodeURIComponent(key)}`);
        },
        
        // JSON operations
        searchJson: function(region, query) {
            return this.request('POST', `/cache/${encodeURIComponent(region)}/search`, { query });
        }
    };
    
    // ============================================
    // Auto-refresh Stats (Dashboard)
    // ============================================
    
    let statsInterval = null;
    
    function startStatsRefresh(intervalMs = 5000) {
        if (statsInterval) {
            clearInterval(statsInterval);
        }
        
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
        if (statsInterval) {
            clearInterval(statsInterval);
            statsInterval = null;
        }
    }
    
    function updateDashboardStats(info) {
        // Update stats cards if elements exist
        const totalEntries = document.querySelector('[data-stat="totalEntries"]');
        if (totalEntries) {
            totalEntries.textContent = Kuber.formatNumber(info.totalEntries || 0);
        }
        
        const connections = document.querySelector('[data-stat="connections"]');
        if (connections) {
            connections.textContent = info.activeConnections || 0;
        }
    }
    
    // ============================================
    // Initialize
    // ============================================
    
    document.addEventListener('DOMContentLoaded', function() {
        // Initialize tooltips
        const tooltipTriggerList = document.querySelectorAll('[data-bs-toggle="tooltip"]');
        [...tooltipTriggerList].map(el => new bootstrap.Tooltip(el));
        
        // Initialize popovers
        const popoverTriggerList = document.querySelectorAll('[data-bs-toggle="popover"]');
        [...popoverTriggerList].map(el => new bootstrap.Popover(el));
        
        // Auto-dismiss alerts
        document.querySelectorAll('.alert-dismissible').forEach(alert => {
            setTimeout(() => {
                const bsAlert = bootstrap.Alert.getOrCreateInstance(alert);
                bsAlert.close();
            }, 5000);
        });
        
        // Format JSON in pre elements
        document.querySelectorAll('pre code.json').forEach(block => {
            try {
                const json = JSON.parse(block.textContent);
                block.innerHTML = JsonFormatter.highlight(json);
            } catch (e) {
                // Not valid JSON, leave as is
            }
        });
        
        // Start stats refresh on dashboard
        if (document.querySelector('[data-page="dashboard"]')) {
            startStatsRefresh();
        }
        
        console.log('Kuber Cache UI initialized');
    });
    
    // Cleanup on page unload
    window.addEventListener('beforeunload', function() {
        stopStatsRefresh();
    });
    
    // Export to global scope
    window.Kuber = Kuber;
    window.JsonFormatter = JsonFormatter;
    window.ApiClient = ApiClient;
    
})();
