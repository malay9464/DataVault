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

const params = new URLSearchParams(window.location.search);
const uploadId = params.get("upload_id");

if (!uploadId) {
    alert("No upload_id provided");
    window.location.href = "/";
}

let currentPage = 1;
let pageSize = 20;
let currentView = 'grouped';
let isSearchMode = false;
let currentFilter = 'all';
let currentSort = 'size-desc';

function orderColumns(columns) {
    const priority = ["name", "email", "phone"];
    const priorityColumns = priority.filter(col => columns.includes(col));
    const remainingColumns = columns.filter(col => !priority.includes(col));
    return [...priorityColumns, ...remainingColumns];
}

async function loadFileMetadata() {
    try {
        const res = await authFetch(`/upload-metadata?upload_id=${uploadId}`);
        if (!res.ok) {
            console.error("Failed to load file metadata");
            return;
        }
        const metadata = await res.json();
        document.getElementById("fileName").textContent = metadata.filename;
        document.getElementById("fileContext").style.display = "block";
    } catch (err) {
        console.error("Error loading file metadata:", err);
    }
}

function isPhoneLike(col) {
    if (!col) return false;
    const s = col.replace(/[_\s\-]/g, ' ').toLowerCase();
    return /\b(phone|mobile|mob|telephone|tel|contact|contactno|mobile_no|phone_no|mobilephone)\b/.test(s);
}

function showLoading() {
    document.getElementById("resultsContainer").innerHTML = `
        <div class="loading-state">
            <div class="loading-spinner"></div>
            <p>Loading related records...</p>
        </div>
    `;
}

// ---------------- LOAD STATISTICS ----------------
async function loadStats() {
    try {
        const res = await authFetch(`/related-grouped-stats?upload_id=${uploadId}`);
        if (!res.ok) throw new Error("Failed to load grouped stats");
        const stats = await res.json();
        document.getElementById("statDuplicateEmails").innerText = stats.email_records;
        document.getElementById("statEmailRecords").innerText = `${stats.email_groups} groups`;
        document.getElementById("statDuplicatePhones").innerText = stats.phone_records;
        document.getElementById("statPhoneRecords").innerText = `${stats.phone_groups} groups`;
        document.getElementById("statBoth").innerText = stats.both_records;
        document.getElementById("statBothRecords").innerText = `${stats.both_groups} groups`;
    } catch (err) {
        console.error("Stats load failed:", err);
    }
}

async function loadGroupedView() {
    showLoading();

    try {
        const res = await authFetch(
            `/related-grouped?upload_id=${uploadId}&page=${currentPage}&page_size=${pageSize}&match_type=${currentFilter}&sort=${currentSort}`
        );

        if (!res.ok) throw new Error("Failed to load grouped data");

        const data = await res.json();

        renderGroupedView(data);
        renderPagination(data.total_groups, data.page, data.page_size);

    } catch (err) {
        console.error(err);
        alert("Failed to load grouped records");
    }
}

function handleSortChange() {
    if (isSearchMode) return;
    currentSort = document.getElementById("sortBy").value;
    currentPage = 1;
    loadGroupedView();
}

// ---------------- FILTER FUNCTION ----------------
function filterByType(filterType) {
    if (isSearchMode) return;
    currentFilter = filterType;
    currentPage = 1;

    document.getElementById("btnFilterAll").classList.toggle("active", filterType === 'all');
    document.getElementById("btnFilterEmail").classList.toggle("active", filterType === 'email');
    document.getElementById("btnFilterPhone").classList.toggle("active", filterType === 'phone');
    document.getElementById("btnFilterBoth").classList.toggle("active", filterType === 'both');

    loadGroupedView();
}

function getPrimaryIdentifier(group) {
    if (!group.match_key) return "Unknown";
    const parts = group.match_key.split("|").map(p => p.trim());
    if (group.match_type === "phone") {
        return parts.find(p => /\d/.test(p)) || parts[0];
    }
    return parts.find(p => p.includes("@")) || parts[0];
}

function getMatchTypeIcon(type) {
    if (type === 'email') return '📧';
    if (type === 'phone') return '📱';
    if (type === 'merged') return '🔗';
    return '📊';
}

function getSeverityClass(count) {
    if (count >= 10) return 'high';
    if (count >= 5) return 'medium';
    return 'low';
}

function getPreviewData(records) {
    if (!records || records.length === 0) return null;
    const firstRecord = records[0].data;
    const preview = {};
    if (firstRecord.name) preview.name = firstRecord.name;
    if (firstRecord.email) preview.email = firstRecord.email;
    if (firstRecord.phone) preview.phone = firstRecord.phone;
    const standardFields = ['name', 'email', 'phone'];
    const otherFields = Object.keys(firstRecord).filter(k => !standardFields.includes(k));
    for (let field of otherFields) {
        if (firstRecord[field] && firstRecord[field] !== '-' && firstRecord[field] !== null) {
            preview.extra = { field, value: firstRecord[field] };
            break;
        }
    }
    return preview;
}

function renderGroupedView(data) {
    const container = document.getElementById('resultsContainer');

    if (!data.groups || data.groups.length === 0) {
        container.innerHTML = '<div class="no-results">No related records found for this filter</div>';
        return;
    }

    let html = '';

    data.groups.forEach((group, index) => {
        const groupId = `group-${data.page}-${index}`;
        const recordCount = group.records.length;
        const matchIcon = getMatchTypeIcon(group.match_type);
        const preview = getPreviewData(group.records);

        html += `
            <div class="group-container ${group.match_type}">
                <div class="group-header" onclick="toggleGroup('${groupId}')">
                    <div class="group-title">
                        <span class="expand-icon collapsed" id="expand-${groupId}">▼</span>
                        <span class="group-identifier">${getPrimaryIdentifier(group)}</span>
                        <span class="match-type-badge ${group.match_type}">
                            ${matchIcon} ${group.match_type}
                        </span>
                        <span class="group-badge">${recordCount}</span>
                    </div>
                    ${preview ? `
                        <div class="group-preview">
                            ${preview.name ? `
                                <div class="preview-field">
                                    <span class="preview-label">Name:</span>
                                    <span class="preview-value">${preview.name}</span>
                                </div>
                            ` : ''}
                            ${preview.email ? `
                                <div class="preview-field">
                                    <span class="preview-label">Email:</span>
                                    <span class="preview-value">${preview.email}</span>
                                </div>
                            ` : ''}
                            ${preview.phone ? `
                                <div class="preview-field">
                                    <span class="preview-label">Phone:</span>
                                    <span class="preview-value">${preview.phone}</span>
                                </div>
                            ` : ''}
                            ${preview.extra ? `
                                <div class="preview-field">
                                    <span class="preview-label">${preview.extra.field}:</span>
                                    <span class="preview-value">${preview.extra.value}</span>
                                </div>
                            ` : ''}
                        </div>
                    ` : ''}
                </div>
                <div class="group-records collapsed" id="records-${groupId}">
                    ${renderGroupTable(group.records)}
                </div>
            </div>
        `;
    });

    container.innerHTML = html;
    container.querySelectorAll('.group-table-scroll')
        .forEach(scroll => attachGroupResizeHandlers(scroll));
}

function toggleGroup(groupId) {
    const recordsDiv = document.getElementById(`records-${groupId}`);
    const expandIcon = document.getElementById(`expand-${groupId}`);
    recordsDiv.classList.toggle('collapsed');
    expandIcon.classList.toggle('collapsed');
}

function renderGroupTable(records) {
    if (records.length === 0) return '<p style="padding: 20px; text-align: center; color: #9ca3af;">No records</p>';

    const allColumns = records[0].data ? Object.keys(records[0].data) : [];
    const columns = orderColumns(allColumns);

    let html = `
        <div class="group-table-scroll">
            <div class="resize-line"></div>
            <table>
    `;

    html += '<thead><tr>';
    columns.forEach((col, index) => {
        html += `
            <th data-index="${index}">
                ${col}
                <div class="resize-handle"></div>
            </th>
        `;
    });
    html += '</tr></thead>';

    html += '<tbody>';
    records.forEach(record => {
        html += '<tr>';
        columns.forEach(col => {
            let value = record.data[col];
            if (value === null || value === undefined || value === "") {
                value = "-";
            }
            html += `<td>${value}</td>`;
        });
        html += '</tr>';
    });
    html += '</tbody>';

    html += `</table></div>`;
    return html;
}

async function searchRelated() {
    const searchValue = document.getElementById("searchValue").value.trim();

    if (!searchValue) {
        alert("Please enter an email or phone number");
        return;
    }

    isSearchMode = true;
    currentPage = 1;
    showLoading();

    try {
        const res = await authFetch(
            `/related-search?upload_id=${uploadId}&value=${encodeURIComponent(searchValue)}&page=1&page_size=100`
        );

        if (!res.ok) throw new Error("Search failed");

        const data = await res.json();
        const container = document.getElementById("resultsContainer");

        if (data.records.length === 0) {
            container.innerHTML = '<div class="no-results">No records found for this search</div>';
            document.getElementById("relatedPagination").innerHTML = '';
            return;
        }

        let html = `
            <div style="padding: 16px; background: linear-gradient(135deg, #fef3c7 0%, #fde68a 100%); border-radius: 8px; margin-bottom: 20px; border-left: 4px solid #f59e0b;">
                <div style="display: flex; align-items: center; gap: 10px;">
                    <span style="font-size: 24px;">🔍</span>
                    <div>
                        <div style="font-weight: 700; color: #92400e; font-size: 15px;">Search Results</div>
                        <div style="font-size: 13px; color: #78350f; margin-top: 2px;">
                            Found <strong>${data.total_records}</strong> records matching "<strong>${searchValue}</strong>"
                        </div>
                    </div>
                </div>
            </div>
        `;

        html += '<div class="group-records" style="max-height: none;"><div class="group-table-scroll"><table>';

        const allColumns = data.records[0].data ? Object.keys(data.records[0].data) : [];
        const columns = orderColumns(allColumns);

        html += '<thead><tr>';
        columns.forEach(col => {
            html += `<th>${col}</th>`;
        });
        html += '</tr></thead>';

        html += '<tbody>';
        data.records.forEach(record => {
            html += '<tr>';
            columns.forEach(col => {
                let value = record.data[col];
                if (value === null || value === undefined || value === "") {
                    value = "-";
                }
                html += `<td>${value}</td>`;
            });
            html += '</tr>';
            html += '</tbody>';
        });

        html += '</table></div></div>';

        container.innerHTML = html;
        document.getElementById("relatedPagination").innerHTML = '';

    } catch (err) {
        console.error(err);
        alert("Search failed");
    }
}

// ---------------- RESET SEARCH ----------------
function resetSearch() {
    document.getElementById("searchValue").value = "";
    isSearchMode = false;
    currentPage = 1;
    loadGroupedView();
}

// ---------------- PAGINATION ----------------
function renderPagination(total, page, size) {
    const pagination = document.getElementById("relatedPagination");
    pagination.innerHTML = "";

    const totalPages = Math.ceil(total / size);
    if (totalPages <= 1) return;

    const prevBtn = document.createElement("button");
    prevBtn.innerText = "← Previous";
    prevBtn.disabled = page === 1;
    prevBtn.onclick = () => {
        currentPage--;
        loadGroupedView();
    };
    pagination.appendChild(prevBtn);

    const MAX_VISIBLE = 7;
    let start = Math.max(1, page - Math.floor(MAX_VISIBLE / 2));
    let end = Math.min(totalPages, start + MAX_VISIBLE - 1);

    if (end === totalPages) {
        start = Math.max(1, end - MAX_VISIBLE + 1);
    }

    if (start > 1) {
        addPageButton(1, page);
        if (start > 2) {
            const ellipsis = document.createElement("span");
            ellipsis.innerText = "...";
            pagination.appendChild(ellipsis);
        }
    }

    for (let i = start; i <= end; i++) {
        addPageButton(i, page);
    }

    if (end < totalPages) {
        if (end < totalPages - 1) {
            const ellipsis = document.createElement("span");
            ellipsis.innerText = "...";
            pagination.appendChild(ellipsis);
        }
        addPageButton(totalPages, page);
    }

    const nextBtn = document.createElement("button");
    nextBtn.innerText = "Next →";
    nextBtn.disabled = page === totalPages;
    nextBtn.onclick = () => {
        currentPage++;
        loadGroupedView();
    };
    pagination.appendChild(nextBtn);
}

function addPageButton(pageNum, currentPageNum) {
    const btn = document.createElement("button");
    btn.innerText = pageNum;
    btn.className = pageNum === currentPageNum ? "active" : "";
    btn.onclick = () => {
        currentPage = pageNum;
        loadGroupedView();
    };
    document.getElementById("relatedPagination").appendChild(btn);
}

function attachGroupResizeHandlers(container) {
    const table = container.querySelector('table');
    const handles = table.querySelectorAll('.resize-handle');
    const resizeLine = container.querySelector('.resize-line');

    let startX = 0;
    let startWidth = 0;
    let currentTh = null;
    let colIndex = -1;

    const MIN_WIDTH = 140;

    handles.forEach(handle => {
        handle.addEventListener('mousedown', e => {
            e.preventDefault();
            e.stopPropagation();

            currentTh = handle.closest('th');
            colIndex = parseInt(currentTh.dataset.index, 10);

            startX = e.clientX;
            startWidth = currentTh.offsetWidth;

            resizeLine.classList.add('active');

            const thRect = currentTh.getBoundingClientRect();
            const containerRect = container.getBoundingClientRect();
            resizeLine.style.left =
                (thRect.right - containerRect.left + container.scrollLeft) + 'px';
        });
    });

    document.addEventListener('mousemove', e => {
        if (!currentTh) return;

        const delta = e.clientX - startX;
        const newWidth = Math.max(MIN_WIDTH, startWidth + delta);

        currentTh.style.width = newWidth + 'px';
        currentTh.style.minWidth = newWidth + 'px';
        currentTh.style.maxWidth = newWidth + 'px';

        table
            .querySelectorAll(`tbody tr td:nth-child(${colIndex + 1})`)
            .forEach(td => {
                td.style.width = newWidth + 'px';
                td.style.minWidth = newWidth + 'px';
                td.style.maxWidth = newWidth + 'px';
            });

        const containerRect = container.getBoundingClientRect();
        const thRect = currentTh.getBoundingClientRect();
        resizeLine.style.left =
            (thRect.right - containerRect.left + container.scrollLeft) + 'px';
    });

    document.addEventListener('mouseup', () => {
        if (!currentTh) return;
        currentTh = null;
        colIndex = -1;
        resizeLine.classList.remove('active');
    });
}

// ---------------- HELPERS ----------------
function goBack() {
    window.location.href = `/preview.html?upload_id=${uploadId}`;
}

async function init() {
    await loadFileMetadata();
    await loadStats();
    await loadGroupedView();
}

init();