/* ===============================
   AUTHENTICATION
   =============================== */
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

/* ===============================
   GLOBAL STATE
   =============================== */
const uploadId = new URLSearchParams(window.location.search).get('upload_id');
let headerInfo = null;
let caseType = null;
let columnWidths = {}; // Store custom column widths

/* ===============================
   SMART DEFAULT WIDTHS
   =============================== */
function getDefaultWidth(columnName) {
    const col = columnName.toLowerCase();
    
    if (col.includes('email') || col.includes('mail')) return 220;
    if (col.includes('phone') || col.includes('mobile') || col.includes('contact')) return 140;
    if (col.includes('name')) return 180;
    if (col.includes('address') || col.includes('resume') || col.includes('description')) return 250;
    if (col.includes('id') || col.includes('code')) return 120;
    if (col.includes('city') || col.includes('state') || col.includes('zip')) return 130;
    if (col.includes('date') || col.includes('time')) return 140;
    if (col.includes('age') || col.includes('level')) return 100;
    if (col.includes('gender') || col.includes('active')) return 100;
    
    return 150; // Default
}

/* ===============================
   LOAD HEADER DATA
   =============================== */
async function loadHeaders() {
    try {
        const response = await authFetch(`/upload/${uploadId}/headers`);

        if (!response || !response.ok) {
            throw new Error('Failed to load headers');
        }

        const data = await response.json();
        headerInfo = data.header_info;
        caseType = headerInfo.case_type;

        renderHeaderUI();
    } catch (error) {
        showAlert('error', 'Failed to load header information: ' + error.message);
    }
}

function renderHeaderUI() {
    const alertContainer = document.getElementById('alertContainer');
    const headerContent = document.getElementById('headerContent');
    const firstRowOptionContainer = document.getElementById('firstRowOptionContainer');

    // Clear containers
    alertContainer.innerHTML = '';
    headerContent.innerHTML = '';
    firstRowOptionContainer.innerHTML = '';

    // Show appropriate alert
    if (caseType === 'missing') {
        alertContainer.innerHTML = `
            <div class="alert alert-warning">
                <strong>⚠️ Missing Headers Detected</strong>
                <p>Some columns have no headers or generic names like "unnamed_1".</p>
                <em>💡 You don't have to name all columns - only the ones you need. Unnamed columns will be preserved as-is.</em>
            </div>
        `;
    } else if (caseType === 'suspicious') {
        alertContainer.innerHTML = `
            <div class="alert alert-warning">
                <strong>⚠️ Suspicious Headers Detected</strong>
                <p>The first row appears to contain data (emails, phones, or numbers) instead of header names.</p>
                <em>💡 If the first row is actual data, check the box below. You can still rename columns.</em>
            </div>
        `;
    }

    // Instruction banner
    const instructionBanner = `
        <div class="instruction-banner">
            <div class="instruction-banner-icon">📝</div>
            <div class="instruction-banner-content">
                <div class="instruction-banner-title">How to use this page:</div>
                <div class="instruction-banner-text">
                    Review the detected column names and sample data below. 
                    Type new names in the white input boxes for columns you want to rename. 
                    Leave inputs blank to keep the detected names. Then click "Submit & Continue Upload".
                </div>
            </div>
        </div>
    `;

    // Build table HTML
    let tableHTML = instructionBanner + `
    <div class="header-table-wrapper">
    <table id="headerTable">
    <colgroup>
        <col style="width:50px">
        ${headerInfo.samples.map((col, idx) => {
            const w = columnWidths[idx] || getDefaultWidth(col.column_name);
            return `<col style="width:${w}px">`;
        }).join('')}
    </colgroup>
    `;

    // Column count
    const columnCount = headerInfo.samples.length;
    const unnamedIndices = headerInfo.metadata.unnamed_indices || [];

    // ROW 1: Column indices (# 1, 2, 3...)
    tableHTML += '<thead><tr class="index-row">';
    tableHTML += '<th style="width: 50px; min-width: 50px;">#</th>';
    
    headerInfo.samples.forEach((col, idx) => {
        tableHTML += `
            <th data-column="${col.column_name}" data-index="${idx}">
                Col ${idx + 1}
                <div class="resize-handle"></div>
            </th>
        `;
    });

    tableHTML += '</tr></thead>';

    tableHTML += '<tbody>';

    // ROW 2: Detected names
    tableHTML += '<tr class="detected-row">';
    tableHTML += '<td>Detected</td>';
    
    headerInfo.samples.forEach((col, idx) => {
        const isUnnamed = unnamedIndices.includes(idx);
        const cellClass = isUnnamed ? 'unnamed' : '';
        tableHTML += `<td class="${cellClass}">${col.column_name}</td>`;
    });
    tableHTML += '</tr>';

    // ROW 3: Input boxes
    tableHTML += '<tr class="input-row">';
    tableHTML += '<td>📝</td>';
    
    headerInfo.samples.forEach((col, idx) => {
        const isUnnamed = unnamedIndices.includes(idx);
        const placeholder = isUnnamed 
            ? 'Enter name (optional)' 
            : 'New name (optional)';
        
        tableHTML += `
            <td id="input_cell_${idx}">
                <input 
                    type="text" 
                    id="col_${idx}" 
                    placeholder="${placeholder}"
                    autocomplete="off"
                >
            </td>
        `;
    });
    tableHTML += '</tr>';

    // SECTION HEADER: Sample Data Preview
    tableHTML += `
        <tr class="preview-section-header">
            <td colspan="${columnCount + 1}">
                📊 Sample Data Preview (first 10 rows)
            </td>
        </tr>
    `;

    // DATA ROWS: Show up to 10 sample rows
    const maxRows = Math.max(...headerInfo.samples.map(col => col.samples.length));
    
    for (let rowIdx = 0; rowIdx < Math.min(maxRows, 10); rowIdx++) {
        tableHTML += '<tr class="data-row">';
        tableHTML += `<td>${rowIdx + 1}</td>`;
        
        headerInfo.samples.forEach((col) => {
            const value = col.samples[rowIdx] || '';
            const displayValue = value === '' ? '-' : value;
            tableHTML += `<td title="${value}">${displayValue}</td>`;
        });
        
        tableHTML += '</tr>';
    }

    tableHTML += '</tbody></table>';
    
    // Resize line (hidden by default)
    tableHTML += '<div class="resize-line" id="resizeLine"></div>';
    
    tableHTML += '</div>';

    headerContent.innerHTML = tableHTML;

    // Add "first row is data" option for suspicious case
    if (caseType === 'suspicious') {
        firstRowOptionContainer.innerHTML = `
            <div class="first-row-option">
                <div class="checkbox-container">
                    <input type="checkbox" id="firstRowIsData">
                    <label for="firstRowIsData">
                        ✓ Treat first row as data (not headers)
                    </label>
                </div>
            </div>
        `;
    }

    // Attach event listeners
    attachInputListeners();
    attachResizeHandlers();
}

/* ===============================
   INPUT TRACKING (modified state)
   =============================== */
function attachInputListeners() {
    const inputs = document.querySelectorAll('input[id^="col_"]');
    
    inputs.forEach(input => {
        input.addEventListener('input', function() {
            const cell = this.closest('td');
            
            if (this.value.trim() !== '') {
                cell.classList.add('modified');
            } else {
                cell.classList.remove('modified');
            }
        });
    });
}

/* ===============================
   COLUMN RESIZE FUNCTIONALITY
   (Reused from preview.js)
   =============================== */
function attachResizeHandlers() {
    const table = document.getElementById('headerTable');
    const handles = table.querySelectorAll('.resize-handle');
    const resizeLine = document.getElementById('resizeLine');
    const wrapper = document.querySelector('.header-table-wrapper');

    if (!handles.length) return;

    let isResizing = false;
    let currentTh = null;
    let currentIndex = null;
    let startX = 0;
    let startWidth = 0;

    handles.forEach(handle => {
        handle.addEventListener('mousedown', (e) => {
            e.preventDefault();
            e.stopPropagation();

            isResizing = true;
            currentTh = handle.closest('th');
            currentIndex = parseInt(currentTh.dataset.index);
            startX = e.clientX;
            startWidth = currentTh.offsetWidth;

            const wrapperRect = wrapper.getBoundingClientRect();
            resizeLine.style.left =
                (currentTh.offsetLeft + currentTh.offsetWidth) + 'px';
            resizeLine.classList.add('active');

            document.body.style.cursor = 'col-resize';
            document.body.style.userSelect = 'none';
        });
    });

    document.addEventListener('mousemove', (e) => {
        if (!isResizing) return;

        const diff = e.clientX - startX;
        const newWidth = Math.max(120, startWidth + diff);

        // Update header column
        const col = table.querySelectorAll('col')[currentIndex + 1];
        col.style.width = newWidth + 'px';

        // Update all cells in this column
        const columnIndex = currentIndex + 1; // +1 because first column is row number

        // Store width
        columnWidths[currentIndex] = newWidth;

        // Update resize line position
        const wrapperRect = wrapper.getBoundingClientRect();
        resizeLine.style.left =
            (currentTh.offsetLeft + newWidth) + 'px';
    });

    document.addEventListener('mouseup', () => {
        if (!isResizing) return;

        isResizing = false;
        resizeLine.classList.remove('active');
        document.body.style.cursor = 'default';
        document.body.style.userSelect = 'auto';
    });
}

/* ===============================
   SUBMIT RESOLUTION
   =============================== */
async function submitResolution() {
    const submitBtn = document.getElementById('submitBtn');
    const cancelBtn = document.getElementById('cancelBtn');
    const spinner = document.getElementById('submitSpinner');
    const text = document.getElementById('submitText');

    // Prevent double submit
    if (submitBtn.disabled) return;

    // Lock UI
    submitBtn.disabled = true;
    cancelBtn.disabled = true;
    spinner.style.display = 'inline-block';
    text.textContent = 'Processing…';

    // Disable all inputs
    const inputs = document.querySelectorAll('input[id^="col_"]');
    inputs.forEach(input => input.disabled = true);

    try {
        // Collect user mappings
        const userMapping = {};
        headerInfo.samples.forEach((col, idx) => {
            const input = document.getElementById(`col_${idx}`);
            if (input && input.value.trim()) {
                userMapping[idx] = input.value.trim();
            }
        });

        // Get first row checkbox state
        const firstRowIsData = document.getElementById('firstRowIsData')?.checked || false;

        // Submit to backend
        const response = await authFetch(`/upload/${uploadId}/resolve-headers`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                user_mapping: userMapping,
                first_row_is_data: firstRowIsData
            })
        });

        if (!response || !response.ok) {
            const err = await response.json();
            throw new Error(err.detail || 'Failed to resolve headers');
        }

        // Success!
        showAlert('success', '✓ Headers resolved successfully! Redirecting...');

        // Redirect after short delay
        setTimeout(() => {
            window.location.href = '/';
        }, 1500);

    } catch (error) {
        // Unlock UI on error
        submitBtn.disabled = false;
        cancelBtn.disabled = false;
        spinner.style.display = 'none';
        text.textContent = 'Submit & Continue Upload';
        inputs.forEach(input => input.disabled = false);

        showAlert('error', error.message);
    }
}

/* ===============================
   CANCEL RESOLUTION
   =============================== */
function cancelResolution() {
    if (confirm('Are you sure you want to cancel? The upload will be deleted.')) {
        // TODO: Call delete endpoint if available
        window.location.href = '/';
    }
}

/* ===============================
   ALERT DISPLAY
   =============================== */
function showAlert(type, message) {
    const alertContainer = document.getElementById('alertContainer');
    
    let alertClass = 'alert-info';
    let icon = 'ℹ️';
    
    if (type === 'error') {
        alertClass = 'alert-warning';
        icon = '⚠️';
    } else if (type === 'success') {
        alertClass = 'alert-success';
        icon = '✓';
    }
    
    alertContainer.innerHTML = `
        <div class="alert ${alertClass}">
            ${icon} ${message}
        </div>
    `;

    // Scroll to top to show alert
    window.scrollTo({ top: 0, behavior: 'smooth' });
}

/* ===============================
   INITIALIZATION
   =============================== */
if (!uploadId) {
    showAlert('error', 'No upload ID provided');
} else {
    loadHeaders();
}