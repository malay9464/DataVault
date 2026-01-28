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
let pageSize = 100;
let currentView = 'grouped';
let isSearchMode = false;
let currentFilter = 'all';
let allLoadedGroups = [];
let hasLoadedAllGroups = false;

/* ---------- ✅ SHARED COLUMN ORDERING FUNCTION ---------- */
function orderColumns(columns) {
    const priority = ["name", "email", "phone"];
    
    const priorityColumns = priority.filter(col => columns.includes(col));
    const remainingColumns = columns.filter(col => !priority.includes(col));
    
    return [...priorityColumns, ...remainingColumns];
}

/* ---------- ✅ LOAD FILE METADATA ---------- */
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

/* ---------- ✅ LOADING STATE HELPERS ---------- */
function showLoading() {
    document.getElementById("resultsContainer").innerHTML = `
        <div style="text-align: center; padding: 60px; color: #9ca3af; font-size: 16px;">
            ⏳ Loading data...
        </div>
    `;
}

function hideLoading() {
    // Loading indicator is replaced by actual content in render functions
}

// ---------------- LOAD STATISTICS ----------------
async function loadStats() {
    try {
        const res = await authFetch(
            `/related-grouped-stats?upload_id=${uploadId}`
        );

        if (!res.ok) throw new Error("Failed to load grouped stats");

        const stats = await res.json();

        document.getElementById("statDuplicateEmails").innerText = stats.email_groups;
        document.getElementById("statEmailRecords").innerText = stats.email_records;
        document.getElementById("statDuplicatePhones").innerText = stats.phone_groups;
        document.getElementById("statPhoneRecords").innerText = stats.phone_records;
        document.getElementById("statBoth").innerText = stats.both_groups;
        document.getElementById("statBothRecords").innerText = stats.both_records;

    } catch (err) {
        console.error("Stats load failed:", err);
    }
}

// ---------------- LOAD GROUPED VIEW ----------------
async function loadGroupedView() {
    showLoading();
    
    try {
        const res = await authFetch(
            `/related-grouped?upload_id=${uploadId}&page=1&page_size=20&match_type=${currentFilter}`
        );
        
        if (!res.ok) throw new Error("Failed to load grouped data");
        
        const data = await res.json();
        
        renderGroupedView(data);
        renderPagination(data.total_groups, data.page, data.page_size);
        hideLoading();
        
    } catch (err) {
        console.error(err);
        alert("Failed to load grouped records");
    }
}

async function loadGroupedViewWithPage() {
    showLoading();
    
    try {
        const res = await authFetch(
            `/related-grouped?upload_id=${uploadId}&page=${currentPage}&page_size=20&match_type=${currentFilter}`
        );
        
        if (!res.ok) throw new Error("Failed to load grouped data");
        
        const data = await res.json();
        renderGroupedView(data);
        renderPagination(data.total_groups, data.page, data.page_size);
        hideLoading();
        
    } catch (err) {
        console.error(err);
        alert("Failed to load grouped records");
    }
}

// ---------------- FILTER FUNCTION ----------------
function filterByType(filterType) {
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

// ---------------- ✅ RENDER GROUPED VIEW WITH COLUMN ORDERING ----------------
function renderGroupedView(data) {
    const container = document.getElementById('resultsContainer');
    
    if (!data.groups || data.groups.length === 0) {
        container.innerHTML = '<div class="no-results">No related records found</div>';
        return;
    }
    
    let html = '';
    
    data.groups.forEach((group, index) => {
        const groupId = `group-${data.page}-${index}`;
        const recordCount = group.records.length;
        
        html += `
            <div class="group-container">
                <div class="group-header ${group.match_type}" onclick="toggleGroup('${groupId}')">
                    <div class="group-title">
                        <span class="expand-icon collapsed" id="expand-${groupId}">▼</span>
                        <span>${getPrimaryIdentifier(group)}</span>
                        <span class="group-badge">${recordCount} records</span>
                    </div>
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

// ---------------- TOGGLE GROUP ----------------
function toggleGroup(groupId) {
    const recordsDiv = document.getElementById(`records-${groupId}`);
    const expandIcon = document.getElementById(`expand-${groupId}`);
    
    recordsDiv.classList.toggle('collapsed');
    expandIcon.classList.toggle('collapsed');
}

// ---------------- ✅ RENDER GROUP TABLE WITH COLUMN ORDERING ----------------
function renderGroupTable(records) {
    if (records.length === 0) return '<p>No records</p>';
    
    const allColumns = records[0].data ? Object.keys(records[0].data) : [];
    
    // ✅ Apply column ordering
    let columns = orderColumns(allColumns);

    // Hide duplicate email/phone columns
    if (allColumns.map(c => c.toLowerCase()).includes('email')) {
        columns = columns.filter(c => c === 'email' || !c.toLowerCase().includes('email'));
    }
    if (allColumns.map(c => c.toLowerCase()).includes('phone')) {
        columns = columns.filter(c => c.toLowerCase() === 'phone' || !isPhoneLike(c));
    }
    
    let html = `
        <div class="group-records">
            <div class="group-table-scroll">
                <div class="resize-line"></div>
                <table>
    `;

    // Header
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

    // Rows
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
    
    html += `
                </table>
            </div>
        </div>
    `;
    return html;
}

// ---------------- ✅ SEARCH WITH LOADING STATE ----------------
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
            return;
        }
        
        let html = `
            <div style="padding: 15px; background: #fef3c7; border-radius: 6px; margin-bottom: 20px;">
                <strong>Search Results:</strong> Found ${data.total_records} records matching "${searchValue}"
            </div>
        `;
        
        html += '<div class="group-records" style="max-height: none;"><table>';
        
        const allColumns = data.records[0].data ? Object.keys(data.records[0].data) : [];
        
        // ✅ Apply column ordering
        let columns = orderColumns(allColumns);

        if (allColumns.map(c => c.toLowerCase()).includes('email')) {
            columns = columns.filter(c => c === 'email' || !c.toLowerCase().includes('email'));
        }
        if (allColumns.map(c => c.toLowerCase()).includes('phone')) {
            columns = columns.filter(c => c.toLowerCase() === 'phone' || !isPhoneLike(c));
        }
        
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
        });
        html += '</tbody>';
        
        html += '</table></div>';
        
        container.innerHTML = html;
        document.getElementById("relatedPagination").innerHTML = '';
        hideLoading();
        
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
        loadGroupedViewWithPage();
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
            ellipsis.style.padding = "0 10px";
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
            ellipsis.style.padding = "0 10px";
            pagination.appendChild(ellipsis);
        }
        addPageButton(totalPages, page);
    }
    
    const nextBtn = document.createElement("button");
    nextBtn.innerText = "Next →";
    nextBtn.disabled = page === totalPages;
    nextBtn.onclick = () => {
        currentPage++;
        loadGroupedViewWithPage();
    };
    pagination.appendChild(nextBtn);
}

function addPageButton(pageNum, currentPageNum) {
    const btn = document.createElement("button");
    btn.innerText = pageNum;
    btn.className = pageNum === currentPageNum ? "active" : "";
    btn.onclick = () => {
        currentPage = pageNum;
        loadGroupedViewWithPage();
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

    handles.forEach(handle => {
        handle.addEventListener('mousedown', e => {
            e.preventDefault();
            e.stopPropagation();

            currentTh = handle.closest('th');
            startX = e.clientX;
            startWidth = currentTh.offsetWidth;

            resizeLine.classList.add('active');
        });
    });

    document.addEventListener('mousemove', e => {
        if (!currentTh) return;

        const diff = e.clientX - startX;
        const newWidth = Math.max(140, startWidth + diff);

        // 🔒 lock column width
        currentTh.style.width = newWidth + 'px';
        currentTh.style.minWidth = newWidth + 'px';
        currentTh.style.maxWidth = newWidth + 'px';

        const index = parseInt(currentTh.dataset.index, 10);

        // 🔒 lock body cells
        table.querySelectorAll(`tbody tr td:nth-child(${index + 1})`)
            .forEach(td => {
                td.style.width = newWidth + 'px';
                td.style.minWidth = newWidth + 'px';
                td.style.maxWidth = newWidth + 'px';
            });

        // 🔥 CRITICAL: prevent other columns from shrinking
        table.style.minWidth = table.scrollWidth + 'px';

        const scrollLeft = container.scrollLeft;

        resizeLine.style.left =
            (currentTh.offsetLeft + currentTh.offsetWidth - scrollLeft) + 'px';
    });

    document.addEventListener('mouseup', () => {
        if (!currentTh) return;

        currentTh = null;
        resizeLine.classList.remove('active');
    });
}


// ---------------- HELPERS ----------------
function goBack() {
    window.location.href = `/preview.html?upload_id=${uploadId}`;
}

// ---------------- ✅ INIT - Load metadata, stats, and data ----------------
async function init() {
    await loadFileMetadata(); // ✅ Load filename
    await loadStats();        // Load statistics
    await loadGroupedView();  // Load grouped data
}

init();