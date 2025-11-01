const API_BASE = 'http://localhost:5000';

let currentPage = 1;

function getFilterValue(id) {
    const el = document.getElementById(id);
    return el ? el.value.trim() : '';
}

function buildQueryParams(page = 1) {
    const params = new URLSearchParams();
    if (getFilterValue('userIdFilter')) params.append('user_id', getFilterValue('userIdFilter'));
    if (getFilterValue('deviceIdFilter')) params.append('device_id', getFilterValue('deviceIdFilter'));
    if (getFilterValue('traceIdFilter')) params.append('traceId', getFilterValue('traceIdFilter'));
    if (getFilterValue('functionFilter')) params.append('functionName', getFilterValue('functionFilter'));
    if (getFilterValue('routeFilter')) params.append('route', getFilterValue('routeFilter'));
    if (getFilterValue('levelFilter')) params.append('level', getFilterValue('levelFilter'));
    if (getFilterValue('methodFilter')) params.append('method', getFilterValue('methodFilter'));
    if (getFilterValue('sourceFilter')) params.append('source', getFilterValue('sourceFilter'));
    if (getFilterValue('statusFilter')) params.append('status', getFilterValue('statusFilter'));
    if (getFilterValue('searchFilter')) params.append('search', getFilterValue('searchFilter'));
    if (getFilterValue('startDateFilter')) params.append('startDate', getFilterValue('startDateFilter'));
    if (getFilterValue('endDateFilter')) params.append('endDate', getFilterValue('endDateFilter'));
    params.append('page', page);
    params.append('limit', getFilterValue('limitFilter') || 100);
    return params.toString();
}

async function fetchLogs(page = 1) {
    const params = buildQueryParams(page);
    const res = await fetch(`${API_BASE}/api/logs?${params}`);
    if (!res.ok) throw new Error('Failed to fetch logs');
    const json = await res.json();
    if (!json.success) throw new Error(json.message || 'Failed to fetch logs');
    return json.data;
}

async function fetchStats() {
    const res = await fetch(`${API_BASE}/api/logs/stats`);
    if (!res.ok) return {};
    const json = await res.json();
    return json.data || {};
}

function showLoading() {
    document.getElementById('logsContent').innerHTML = `
        <div class="loading">
            <div class="spinner"></div>
            <div class="loading-text">Loading logs...</div>
        </div>
    `;
    document.getElementById('logsCount').textContent = 'Loading...';
}

function showError(msg) {
    document.getElementById('logsContent').innerHTML = `
        <div class="error-message">${msg}</div>
    `;
    document.getElementById('logsCount').textContent = 'Error';
}

function renderStats(stats) {
    const statsGrid = document.getElementById('statsGrid');
    if (!statsGrid) return;
    statsGrid.innerHTML = `
        <div class="stat-card">
            <div class="stat-number">${stats.totalLogs || 0}</div>
            <div class="stat-label">Total Logs</div>
        </div>
        <div class="stat-card">
            <div class="stat-number level-error">${stats.errorCount || 0}</div>
            <div class="stat-label">Errors</div>
        </div>
        <div class="stat-card">
            <div class="stat-number level-warn">${stats.warnCount || 0}</div>
            <div class="stat-label">Warnings</div>
        </div>
        <div class="stat-card">
            <div class="stat-number level-info">${stats.infoCount || 0}</div>
            <div class="stat-label">Info</div>
        </div>
        <div class="stat-card">
            <div class="stat-number">${stats.uniqueUsers || 0}</div>
            <div class="stat-label">Unique Users</div>
        </div>
    `;
}

// Replace the renderLogs function in static.js with this:
function renderLogs(data) {
    const logsContent = document.getElementById('logsContent');
    const logsCount = document.getElementById('logsCount');
    if (!data.logs || data.logs.length === 0) {
        logsContent.innerHTML = `<div class="no-logs"><h3>No logs found</h3></div>`;
        logsCount.textContent = '0 logs';
        renderPagination(data.pagination || {});
        return;
    }
    logsCount.textContent = `${data.pagination.totalCount} logs`;

    logsContent.innerHTML = data.logs.map(log => `
        <div class="log-entry ${log.level}">
            <div class="log-header">
                <div class="log-badges">
                    <span class="log-badge level ${log.level}">${log.level}</span>
                    ${log.meta && log.meta.method ? `<span class="log-badge method">${log.meta.method}</span>` : ''}
                    ${log.meta && log.meta.source ? `<span class="log-badge">${log.meta.source}</span>` : ''}
                </div>
                <span class="log-timestamp">${log.timestamp ? new Date(log.timestamp).toLocaleString() : ''}</span>
            </div>
            <div class="log-message">${log.message || ''}</div>
            <div class="log-details">
                ${log.meta && log.meta.user_id ? `<div class="log-detail"><span class="log-detail-label">User ID:</span> <span class="log-detail-value">${log.meta.user_id}</span></div>` : ''}
                ${log.meta && log.meta.device_id ? `<div class="log-detail"><span class="log-detail-label">Device ID:</span> <span class="log-detail-value">${log.meta.device_id}</span></div>` : ''}
                ${log.meta && log.meta.traceId ? `<div class="log-detail"><span class="log-detail-label">Trace ID:</span> <span class="log-detail-value">${log.meta.traceId}</span></div>` : ''}
                ${log.meta && log.meta.route ? `<div class="log-detail"><span class="log-detail-label">Route/Endpoint:</span> <span class="log-detail-value">${log.meta.route}</span></div>` : ''}
                ${log.meta && (log.meta.function || log.meta.functionName) ? `<div class="log-detail"><span class="log-detail-label">Function:</span> <span class="log-detail-value">${log.meta.function || log.meta.functionName}</span></div>` : ''}
                ${log.meta && log.meta.status ? `<div class="log-detail"><span class="log-detail-label">Status:</span> <span class="log-detail-value">${log.meta.status}</span></div>` : ''}
                ${log.meta && log.meta.duration ? `<div class="log-detail"><span class="log-detail-label">Duration:</span> <span class="log-detail-value">${log.meta.duration}ms</span></div>` : ''}
            </div>
            ${log.meta && Object.keys(log.meta).length > 0 ? `<div class="log-meta">${JSON.stringify(log.meta, null, 2)}</div>` : ''}
        </div>
    `).join('');
    renderPagination(data.pagination || {});
}

function renderPagination(pagination) {
    const paginationDiv = document.getElementById('pagination');
    if (!paginationDiv || !pagination.totalPages) return;
    let html = '';
    if (pagination.totalPages > 1) {
        if (pagination.hasPrev) {
            html += `<button onclick="loadLogs(${pagination.currentPage - 1})">&laquo; Prev</button>`;
        }
        for (let i = 1; i <= pagination.totalPages; i++) {
            html += `<button onclick="loadLogs(${i})" ${i === pagination.currentPage ? 'class="active"' : ''}>${i}</button>`;
        }
        if (pagination.hasNext) {
            html += `<button onclick="loadLogs(${pagination.currentPage + 1})">Next &raquo;</button>`;
        }
    }
    paginationDiv.innerHTML = html;
}

function applyFilters() {
    currentPage = 1;
    loadLogs(currentPage);
}

function clearFilters() {
    [
        'userIdFilter', 'deviceIdFilter', 'traceIdFilter', 'functionFilter', 'routeFilter',
        'levelFilter', 'methodFilter', 'sourceFilter', 'statusFilter', 'searchFilter',
        'startDateFilter', 'endDateFilter'
    ].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.value = '';
    });
    loadLogs(1);
}

async function exportLogs() {
    const params = buildQueryParams(currentPage);
    try {
        const response = await fetch(`${API_BASE}/api/logs/export?${params}`);
        if (!response.ok) throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = 'logs_export.json';
        document.body.appendChild(a);
        a.click();
        a.remove();
        window.URL.revokeObjectURL(url);
    } catch (error) {
        showError('Failed to export logs: ' + error.message);
    }
}

async function loadLogs(page = 1) {
    currentPage = page;
    showLoading();
    try {
        const data = await fetchLogs(page);
        renderLogs(data);
    } catch (error) {
        showError(error.message || 'Failed to load logs');
    }
}

// Initial load
document.addEventListener('DOMContentLoaded', async () => {
    showLoading();
    const stats = await fetchStats();
    renderStats(stats);
    loadLogs(1);

    // Allow pressing Enter in any filter input to apply filters
    document.querySelectorAll('.filters-grid input, .filters-grid select').forEach(el => {
        el.addEventListener('keydown', function(e) {
            if (e.key === 'Enter') {
                e.preventDefault();
                applyFilters();
            }
        });
    });
});