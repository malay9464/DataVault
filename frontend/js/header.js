const token = localStorage.getItem('access_token');

if (!token) {
    window.location.href = '/static/login.html';
}

async function authFetch(url, options = {}) {
    const res = await fetch(url, {
        ...options,
        headers: {
            ...(options.headers || {}),
            'Authorization': `Bearer ${token}`
        }
    });

    if (res.status === 401) {
        localStorage.removeItem('access_token');
        window.location.href = '/static/login.html';
        return null;
    }

    return res;
}

const uploadId = new URLSearchParams(window.location.search).get('upload_id');
let headerInfo = null;
let caseType = null;
let columnWidths = {};

function getDefaultWidth(columnName) {
    const col = columnName.toLowerCase();
    if (col.includes('email') || col.includes('mail')) return 220;
    if (col.includes('phone') || col.includes('mobile') || col.includes('contact')) return 160;
    if (col.includes('name')) return 180;
    if (col.includes('address') || col.includes('description')) return 250;
    if (col.includes('id') || col.includes('code')) return 120;
    if (col.includes('city') || col.includes('state') || col.includes('zip')) return 130;
    if (col.includes('date') || col.includes('time')) return 150;
    if (col.includes('age') || col.includes('level') || col.includes('gender')) return 100;
    return 150;
}

// ── Update renamed column counter in action bar ──
function updateRenamedCount() {
    const inputs = document.querySelectorAll('input[id^="col_"]');
    let count = 0;
    inputs.forEach(input => {
        if (input.value.trim() !== '') count++;
    });

    const badge = document.getElementById('renamedCount');
    const statusText = document.getElementById('actionStatusText');

    if (count > 0) {
        badge.textContent = `${count} column${count > 1 ? 's' : ''} renamed`;
        badge.style.display = 'inline';
        statusText.textContent = 'Confirm to finalize and ingest the file.';
    } else {
        badge.style.display = 'none';
        statusText.textContent = 'Review columns above, then confirm to continue.';
    }
}

async function loadHeaders() {
    try {
        const response = await authFetch(`/upload/${uploadId}/headers`);

        if (!response || !response.ok) {
            throw new Error('Failed to load headers');
        }

        const data = await response.json();
        headerInfo = data.header_info;
        caseType = headerInfo.case_type;

        // Update page subtitle with filename
        const subtitle = document.getElementById('pageSubtitle');
        if (subtitle && data.filename) {
            subtitle.innerHTML = `File: <span class="filename">${data.filename}</span>`;
        }

        renderHeaderUI();
    } catch (error) {
        showAlert('error', 'Failed to load header information: ' + error.message);
    }
}

function renderHeaderUI() {
    const alertContainer = document.getElementById('alertContainer');
    const headerContent = document.getElementById('headerContent');
    const firstRowOptionContainer = document.getElementById('firstRowOptionContainer');

    alertContainer.innerHTML = '';
    headerContent.innerHTML = '';
    firstRowOptionContainer.innerHTML = '';

    // Show case-specific alert
    if (caseType === 'missing') {
        alertContainer.innerHTML = `
            <div class="alert alert-warning">
                <span class="alert-icon">⚠</span>
                <div class="alert-body">
                    <strong>Missing Headers Detected</strong>
                    <p>Some columns have no headers or generic names like <code>unnamed_1</code>.</p>
                    <em>You don't have to name all columns — only the ones you need. Unnamed columns will be preserved as-is.</em>
                </div>
            </div>
        `;
    } else if (caseType === 'suspicious') {
        alertContainer.innerHTML = `
            <div class="alert alert-warning">
                <span class="alert-icon">⚠</span>
                <div class="alert-body">
                    <strong>Suspicious Headers Detected</strong>
                    <p>The first row appears to contain data (emails, phones, or numbers) instead of column names.</p>
                    <em>If the first row is actual data, check the box below before confirming.</em>
                </div>
            </div>
        `;
    }

    const unnamedIndices = headerInfo.metadata?.unnamed_indices || [];
    const columnCount = headerInfo.samples.length;

    // Build table
    let tableHTML = `
        <div class="instruction-banner">
            <span class="instruction-banner-icon">📋</span>
            <div>
                <div class="instruction-banner-title">How to use this page</div>
                <div class="instruction-banner-text">
                    Review detected column names and sample data below.
                    Type a new name in any input cell to rename that column.
                    Leave inputs blank to keep the detected name.
                    When ready, click <strong>Confirm &amp; Ingest</strong>.
                </div>
            </div>
        </div>

        <div class="header-table-wrapper">
        <table id="headerTable">
        <colgroup>
            <col style="width:52px">
            ${headerInfo.samples.map((col, idx) => {
                const w = columnWidths[idx] || getDefaultWidth(col.column_name);
                return `<col style="width:${w}px">`;
            }).join('')}
        </colgroup>
    `;

    // Row 1: Column indices
    tableHTML += '<thead><tr class="index-row">';
    tableHTML += '<th style="width:52px; min-width:52px;">#</th>';
    headerInfo.samples.forEach((col, idx) => {
        tableHTML += `
            <th data-column="${col.column_name}" data-index="${idx}">
                Col ${idx + 1}
                <div class="resize-handle"></div>
            </th>
        `;
    });
    tableHTML += '</tr></thead><tbody>';

    // Row 2: Detected names
    tableHTML += '<tr class="detected-row">';
    tableHTML += '<td>Detected</td>';
    headerInfo.samples.forEach((col, idx) => {
        const isUnnamed = unnamedIndices.includes(idx);
        tableHTML += `<td class="${isUnnamed ? 'unnamed' : ''}">${col.column_name}</td>`;
    });
    tableHTML += '</tr>';

    // Row 3: Input boxes
    tableHTML += '<tr class="input-row">';
    tableHTML += '<td>✏</td>';
    headerInfo.samples.forEach((col, idx) => {
        const isUnnamed = unnamedIndices.includes(idx);
        tableHTML += `
            <td id="input_cell_${idx}">
                <input
                    type="text"
                    id="col_${idx}"
                    placeholder="${isUnnamed ? 'Name this column…' : 'Rename (optional)'}"
                    autocomplete="off"
                    spellcheck="false"
                >
            </td>
        `;
    });
    tableHTML += '</tr>';

    // Section divider
    tableHTML += `
        <tr class="preview-section-header">
            <td colspan="${columnCount + 1}">Sample Data Preview — first ${Math.min(10, Math.max(...headerInfo.samples.map(c => c.samples.length)))} rows</td>
        </tr>
    `;

    // Data rows
    const maxRows = Math.max(...headerInfo.samples.map(col => col.samples.length));
    for (let rowIdx = 0; rowIdx < Math.min(maxRows, 10); rowIdx++) {
        tableHTML += '<tr class="data-row">';
        tableHTML += `<td>${rowIdx + 1}</td>`;
        headerInfo.samples.forEach(col => {
            const value = col.samples[rowIdx] ?? '';
            const display = value === '' ? '—' : value;
            tableHTML += `<td title="${value}">${display}</td>`;
        });
        tableHTML += '</tr>';
    }

    tableHTML += '</tbody></table>';
    tableHTML += '<div class="resize-line" id="resizeLine"></div>';
    tableHTML += '</div>';

    headerContent.innerHTML = tableHTML;

    // "First row is data" for suspicious case
    if (caseType === 'suspicious') {
        firstRowOptionContainer.innerHTML = `
            <div class="first-row-option">
                <div class="checkbox-container">
                    <input type="checkbox" id="firstRowIsData">
                    <label for="firstRowIsData">Treat first row as data, not headers</label>
                </div>
                <span class="first-row-option-hint">The current first row will be moved into the dataset</span>
            </div>
        `;
    }

    attachInputListeners();
    attachResizeHandlers();
}

function attachInputListeners() {
    document.querySelectorAll('input[id^="col_"]').forEach(input => {
        input.addEventListener('input', function () {
            const cell = this.closest('td');
            if (this.value.trim() !== '') {
                cell.classList.add('modified');
            } else {
                cell.classList.remove('modified');
            }
            updateRenamedCount();
        });
    });
}

function attachResizeHandlers() {
    const table = document.getElementById('headerTable');
    if (!table) return;

    const handles = table.querySelectorAll('.resize-handle');
    const resizeLine = document.getElementById('resizeLine');
    const wrapper = document.querySelector('.header-table-wrapper');

    let isResizing = false;
    let currentIndex = null;
    let startX = 0;
    let startWidth = 0;

    handles.forEach(handle => {
        handle.addEventListener('mousedown', (e) => {
            e.preventDefault();
            e.stopPropagation();

            isResizing = true;
            const th = handle.closest('th');
            currentIndex = parseInt(th.dataset.index);
            startX = e.clientX;
            startWidth = th.offsetWidth;

            const wrapperRect = wrapper.getBoundingClientRect();
            resizeLine.style.left = (e.clientX - wrapperRect.left + wrapper.scrollLeft) + 'px';
            resizeLine.classList.add('active');

            document.body.style.cursor = 'col-resize';
            document.body.style.userSelect = 'none';
        });
    });

    document.addEventListener('mousemove', (e) => {
        if (!isResizing) return;

        const diff = e.clientX - startX;
        const newWidth = Math.max(100, startWidth + diff);

        const col = table.querySelectorAll('col')[currentIndex + 1];
        if (col) col.style.width = newWidth + 'px';

        columnWidths[currentIndex] = newWidth;

        const wrapperRect = wrapper.getBoundingClientRect();
        resizeLine.style.left = (e.clientX - wrapperRect.left + wrapper.scrollLeft) + 'px';
    });

    document.addEventListener('mouseup', () => {
        if (!isResizing) return;
        isResizing = false;
        resizeLine.classList.remove('active');
        document.body.style.cursor = 'default';
        document.body.style.userSelect = 'auto';
    });
}

async function submitResolution() {
    const submitBtn = document.getElementById('submitBtn');
    const cancelBtn = document.getElementById('cancelBtn');
    const spinner = document.getElementById('submitSpinner');
    const text = document.getElementById('submitText');

    if (submitBtn.disabled) return;

    submitBtn.disabled = true;
    cancelBtn.disabled = true;
    spinner.style.display = 'inline-block';
    text.textContent = 'Processing…';

    document.querySelectorAll('input[id^="col_"]').forEach(i => i.disabled = true);

    // Update status text
    const statusText = document.getElementById('actionStatusText');
    if (statusText) statusText.textContent = 'Ingesting file, please wait…';

    try {
        const userMapping = {};
        headerInfo.samples.forEach((col, idx) => {
            const input = document.getElementById(`col_${idx}`);
            if (input && input.value.trim()) {
                userMapping[idx] = input.value.trim();
            }
        });

        const firstRowIsData = document.getElementById('firstRowIsData')?.checked || false;

        const response = await authFetch(`/upload/${uploadId}/resolve-headers`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                user_mapping: userMapping,
                first_row_is_data: firstRowIsData
            })
        });

        if (!response || !response.ok) {
            const err = await response.json();
            throw new Error(err.detail || 'Failed to resolve headers');
        }

        showAlert('success', 'Headers resolved successfully. Redirecting to dashboard…');

        setTimeout(() => {
            window.location.href = '/';
        }, 1500);

    } catch (error) {
        submitBtn.disabled = false;
        cancelBtn.disabled = false;
        spinner.style.display = 'none';
        text.textContent = 'Confirm & Ingest';
        document.querySelectorAll('input[id^="col_"]').forEach(i => i.disabled = false);
        if (statusText) statusText.textContent = 'Review columns above, then confirm to continue.';
        showAlert('error', error.message);
    }
}

function cancelResolution() {
    if (confirm('Cancel this upload? The file will not be saved.')) {
        window.location.href = '/';
    }
}

function showAlert(type, message) {
    const alertContainer = document.getElementById('alertContainer');

    const iconMap = { error: '⚠', success: '✓', info: 'ℹ' };
    const classMap = { error: 'alert-warning', success: 'alert-success', info: 'alert-info' };

    alertContainer.innerHTML = `
        <div class="alert ${classMap[type] || 'alert-info'}">
            <span class="alert-icon">${iconMap[type] || 'ℹ'}</span>
            <div class="alert-body">
                <p>${message}</p>
            </div>
        </div>
    `;

    window.scrollTo({ top: 0, behavior: 'smooth' });
}

// ── Init ──
if (!uploadId) {
    showAlert('error', 'No upload ID provided. Please return to the dashboard and try again.');
} else {
    loadHeaders();
}