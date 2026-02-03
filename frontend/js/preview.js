/* ---------- AUTH ---------- */
const token = localStorage.getItem("access_token");

if (!token) {
    window.location.href = "/static/login.html";
}

async function authFetch(url, options = {}) {
    const res = await fetch(url, {
        ...options,
        headers: {
            ...(options.headers || {}),
            "Authorization": "Bearer " + token
        }
    });

    if (res.status === 401) {
        localStorage.removeItem("access_token");
        window.location.href = "/static/login.html";
        return;
    }

    return res;
}

/* ---------- PARAMS ---------- */
const params = new URLSearchParams(window.location.search);
const uploadId = params.get("upload_id");

let page = 1;
let pageSize = 50;
let totalRecords = 0;
let columnWidths = {};
let searchQuery = '';

/* ---------- COLUMN ORDERING ---------- */
function orderColumns(columns) {
    const priority = ["name", "email", "phone"];
    const priorityColumns = priority.filter(col => columns.includes(col));
    const remainingColumns = columns.filter(col => !priority.includes(col));
    return [...priorityColumns, ...remainingColumns];
}

/* ---------- LOAD FILE METADATA ---------- */
async function loadFileMetadata() {
    try {
        const res = await authFetch(`/upload-metadata?upload_id=${uploadId}`);
        if (!res.ok) return;
        
        const metadata = await res.json();
        const fileNameEl = document.getElementById("fileName");
        const fileContextEl = document.getElementById("fileContext");

        if (fileNameEl) {
            fileNameEl.textContent = metadata.filename;
        }

        if (fileContextEl) {
            fileContextEl.style.display = "block";
        }
    } catch (err) {
        console.error("Error loading metadata:", err);
    }
}

/* ---------- PAGE SIZE ---------- */
document.getElementById("pageSize").onchange = e => {
    pageSize = parseInt(e.target.value);
    page = 1;
    
    if (searchQuery) {
        loadSearchResults();
    } else {
        loadData();
    }
};

/* ---------- LOADING STATE ---------- */
function showLoading() {
    document.getElementById("loadingState").style.display = "flex";
    document.getElementById("tableWrapper").style.display = "none";
    document.getElementById("paginationWrapper").style.display = "none";
}

function hideLoading() {
    document.getElementById("loadingState").style.display = "none";
    document.getElementById("tableWrapper").style.display = "block";
    document.getElementById("paginationWrapper").style.display = "block";
}

/* ---------- SEARCH FUNCTIONALITY ---------- */
async function performSearch() {
    const input = document.getElementById("searchInput").value.trim();
    
    if (!input) {
        alert("Please enter a search term");
        return;
    }
    
    searchQuery = input;
    page = 1;
    
    // Show reset button
    document.getElementById("resetBtn").style.display = "inline-block";
    
    // Load search results from server
    await loadSearchResults();
}

function resetSearch() {
    searchQuery = '';
    sortColumn = null;
    sortDirection = 'asc';
    document.getElementById("searchInput").value = '';
    document.getElementById("resetBtn").style.display = "none";
    document.getElementById("resultsInfo").style.display = "none";
    page = 1;
    
    // Reload normal data
    loadData();
}

async function loadSearchResults() {
    showLoading();
    
    try {
        const res = await authFetch(
            `/search?upload_id=${uploadId}&query=${encodeURIComponent(searchQuery)}&page=${page}&page_size=${pageSize}`
        );

        if (!res.ok) {
            alert("Search failed");
            return;
        }

        const data = await res.json();
        const searchTotal = data.total_records;
        
        // Calculate total pages for search results
        const totalPages = Math.ceil(searchTotal / pageSize);
        
        // Show results info
        const resultsInfo = document.getElementById("resultsInfo");
        const resultsText = document.getElementById("resultsText");
        
        resultsInfo.style.display = "block";
        const start = (page - 1) * pageSize + 1;
        const end = Math.min(page * pageSize, searchTotal);
        resultsText.innerHTML = `
            <strong>Search Results:</strong> Found ${searchTotal.toLocaleString()} records matching "${searchQuery}"
            ${searchTotal > 0 ? `(showing ${start}-${end})` : ''}
        `;
        
        // Render table and pagination
        renderTable({ columns: data.columns, rows: data.rows });
        renderPagination(totalPages);
        
    } catch (err) {
        console.error("Search error:", err);
        alert("Search failed. Please try again.");
    } finally {
        hideLoading();
    }
}

let sortColumn = null;
let sortDirection = 'asc';

function sortByColumn(columnName) {
    
    // Toggle sort direction if clicking same column
    if (sortColumn === columnName) {
        sortDirection = sortDirection === 'asc' ? 'desc' : 'asc';
    } else {
        sortColumn = columnName;
        sortDirection = 'asc';
    }
    
    page = 1;
    loadData();
}

/* ---------- LOAD DATA ---------- */
async function loadData() {
    showLoading();
    
    try {
        let url = `/preview?upload_id=${uploadId}&page=${page}&page_size=${pageSize}`;
        
        // Add sorting parameters if active
        if (sortColumn) {
            url += `&sort_column=${encodeURIComponent(sortColumn)}&sort_direction=${sortDirection}`;
        }
        
        const res = await authFetch(url);

        if (!res.ok) {
            alert("Unauthorized or data not found");
            return;
        }

        const data = await res.json();
        totalRecords = data.total_records;
        
        // Calculate total pages
        const totalPages = Math.ceil(totalRecords / pageSize);
        
        // Render table and pagination
        renderTable({ columns: data.columns, rows: data.rows });
        renderPagination(totalPages);
        
        // Hide results info when not searching
        document.getElementById("resultsInfo").style.display = "none";
        
    } catch (err) {
        console.error("Error loading data:", err);
        alert("Failed to load data");
    } finally {
        hideLoading();
    }
}

/* ---------- SMART DEFAULT WIDTHS ---------- */
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
    
    return 150;
}

/* ---------- RENDER TABLE ---------- */
function renderTable(data) {
    const table = document.getElementById("dataTable");
    table.innerHTML = "";

    if (!data.rows || data.rows.length === 0) {
        table.innerHTML = "<tr><td colspan='100' style='text-align: center; padding: 40px; color: #9ca3af;'>No data found</td></tr>";
        return;
    }

    const orderedColumns = orderColumns(data.columns);

    // Build header with resize handles and sort icons
    let html = "<thead><tr>";
    orderedColumns.forEach((col, index) => {
        const width = columnWidths[col] || getDefaultWidth(col);
        const isSorted = sortColumn === col;
        const sortIcon = isSorted ? (sortDirection === 'asc' ? '↑' : '↓') : '⇅';
        const isLast = index === orderedColumns.length - 1;
        
        html += `
            <th style="
                    width: ${width}px;
                    min-width: ${width}px;
                    ${isLast ? '' : `max-width: ${width}px;`}"
                data-column="${col}" 
                data-index="${index}"
                class="${isSorted ? 'sorted' : ''}">
                <div class="th-content">
                    <span class="th-label"
                        ${searchQuery ? '' : `onclick="sortByColumn('${col}')"`}>
                        ${col}
                    </span>
                    ${searchQuery ? '' : `<span class="sort-icon">${sortIcon}</span>`}
                    <div class="resize-handle"></div>
                </div>
            </th>
        `;
    });
    html += "</tr></thead>";

    // Build body
    html += "<tbody>";
    data.rows.forEach(r => {
        const rowData = {};
        data.columns.forEach((col, idx) => {
            rowData[col] = r.values[idx];
        });
        
        html += "<tr>";
        orderedColumns.forEach(col => {
            const value = rowData[col] ?? "";
            const displayValue = value || "-";
            const isLong = String(value).length > 50;
            const isLast = col === orderedColumns[orderedColumns.length - 1];
            const w = columnWidths[col] || getDefaultWidth(col);
            
            html += `<td  class="${isLong ? 'has-long-text' : ''}"
                style="${isLast ? '' : `width:${w}px; min-width:${w}px; max-width:${w}px;`}" title="${value}">${displayValue}</td>`;
        });
        html += "</tr>";
    });
    html += "</tbody>";

    table.innerHTML = html;

    // Attach resize functionality
    attachResizeHandlers();
}

/* ---------- COLUMN RESIZE FUNCTIONALITY ---------- */
function attachResizeHandlers() {
    const table = document.getElementById('dataTable');
    const handles = table.querySelectorAll('.resize-handle');
    const resizeLine = document.getElementById('resizeLine');
    const wrapper = document.getElementById('tableWrapper');

    let isResizing = false;
    let currentTh = null;
    let startX = 0;
    let startWidth = 0;
    let wrapperRect = null;

    handles.forEach(handle => {
        handle.addEventListener('mousedown', (e) => {
            e.preventDefault();
            e.stopPropagation();

            isResizing = true;
            currentTh = handle.closest('th');
            startX = e.clientX;
            startWidth = currentTh.offsetWidth;
            wrapperRect = wrapper.getBoundingClientRect();

            const thRect = currentTh.getBoundingClientRect();
            resizeLine.style.left =
                (thRect.right - wrapperRect.left + wrapper.scrollLeft) + 'px';

            resizeLine.classList.add('active');
            document.body.style.cursor = 'col-resize';
            document.body.style.userSelect = 'none';
        });
    });

    document.addEventListener('mousemove', (e) => {
        if (!isResizing) return;

        const diff = e.clientX - startX;
        const newWidth = Math.max(120, startWidth + diff);

        currentTh.style.width = newWidth + 'px';
        currentTh.style.minWidth = newWidth + 'px';
        currentTh.style.maxWidth = newWidth + 'px';

        const columnIndex = currentTh.cellIndex + 1;
        table.querySelectorAll(`tbody tr td:nth-child(${columnIndex})`)
            .forEach(td => {
                td.style.width = newWidth + 'px';
                td.style.minWidth = newWidth + 'px';
                td.style.maxWidth = newWidth + 'px';
            });

        // Store width for this column
        const colName = currentTh.getAttribute('data-column');
        columnWidths[colName] = newWidth;

        const thRect = currentTh.getBoundingClientRect();
        resizeLine.style.left =
            (thRect.right - wrapperRect.left + wrapper.scrollLeft) + 'px';
    });

    document.addEventListener('mouseup', () => {
        if (!isResizing) return;

        isResizing = false;
        resizeLine.classList.remove('active');
        document.body.style.cursor = 'default';
        document.body.style.userSelect = 'auto';
    });
}

/* ---------- PAGINATION ---------- */
function renderPagination(totalPages) {
    const pagination = document.getElementById("pagination");
    pagination.innerHTML = "";

    if (totalPages <= 1) return;

    const MAX_VISIBLE = 5;

    // Previous button
    const prev = document.createElement("button");
    prev.innerText = "← Previous";
    prev.disabled = page === 1;
    prev.onclick = () => {
        if (page === 1) return;
        page--;
        if (searchQuery) {
            loadSearchResults();
        } else {
            loadData();
        }
    };
    pagination.appendChild(prev);

    // Calculate visible page range
    let start = Math.max(1, page - Math.floor(MAX_VISIBLE / 2));
    let end = start + MAX_VISIBLE - 1;

    if (end > totalPages) {
        end = totalPages;
        start = Math.max(1, end - MAX_VISIBLE + 1);
    }

    // First page + ellipsis
    if (start > 1) {
        addPageButton(1);
        if (start > 2) {
            const ellipsis = document.createElement("span");
            ellipsis.innerText = "...";
            pagination.appendChild(ellipsis);
        }
    }

    // Visible page numbers
    for (let i = start; i <= end; i++) {
        addPageButton(i);
    }

    // Ellipsis + last page
    if (end < totalPages) {
        if (end < totalPages - 1) {
            const ellipsis = document.createElement("span");
            ellipsis.innerText = "...";
            pagination.appendChild(ellipsis);
        }
        addPageButton(totalPages);
    }

    // Next button
    const next = document.createElement("button");
    next.innerText = "Next →";
    next.disabled = page === totalPages;
    next.onclick = () => {
        if (page === totalPages) return;
        page++;
        if (searchQuery) {
            loadSearchResults();
        } else {
            loadData();
        }
    };
    pagination.appendChild(next);
}

function addPageButton(n) {
    const pagination = document.getElementById("pagination");
    const b = document.createElement("button");
    b.innerText = n;
    if (n === page) b.classList.add("active");
    b.onclick = () => {
        if (page === n) return;
        page = n;
        if (searchQuery) {
            loadSearchResults();
        } else {
            loadData();
        }
    };
    pagination.appendChild(b);
}

/* ---------- EXPORT ---------- */
function exportData(format) {
    const btn = document.getElementById("exportBtn");
    const text = document.getElementById("exportText");
    const spinner = document.getElementById("exportSpinner");

    if (btn) btn.disabled = true;
    if (spinner) spinner.style.display = "inline-block";
    if (text) text.innerText = "Exporting...";

    authFetch(`/export?upload_id=${uploadId}&format=${format}`)
        .then(res => {
            if (!res || !res.ok) {
                throw new Error("Export failed");
            }
            return res.blob();
        })
        .then(blob => {
            const url = URL.createObjectURL(blob);
            const a = document.createElement("a");
            a.href = url;
            a.download =
                format === "excel"
                    ? `cleaned_${uploadId}.xlsx`
                    : `cleaned_${uploadId}.csv`;
            document.body.appendChild(a);
            a.click();
            a.remove();
            URL.revokeObjectURL(url);
        })
        .catch(err => {
            console.error(err);
            alert("Export failed or session expired");
        })
        .finally(() => {
            if (btn) btn.disabled = false;
            if (spinner) spinner.style.display = "none";
            if (text) text.innerText = "Export";
        });
}

/* ---------- RELATED RECORDS ---------- */
function openRelatedRecords() {
    if (!uploadId) {
        alert("No upload ID found");
        return;
    }
    window.location.href = `/related.html?upload_id=${uploadId}`;
}

/* ---------- INIT ---------- */
async function init() {
    if (!uploadId) {
        alert("No upload ID found in URL");
        window.location.href = "/static/upload.html";
        return;
    }
    
    try {
        await loadFileMetadata();
        await loadData();
    } catch (err) {
        console.error("Initialization error:", err);
        alert("Failed to load data. Please try again.");
    }
}

init();