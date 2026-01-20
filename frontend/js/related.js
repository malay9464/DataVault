const token = localStorage.getItem("access_token");

if (!token) {
    window.location.href = "/static/login.html";
}

function authFetch(url, options = {}) {
    return fetch(url, {
        ...options,
        headers: {
            ...(options.headers || {}),
            "Authorization": `Bearer ${token}`
        }
    });
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

// ---------------- LOAD STATISTICS ----------------
async function loadStats() {
    try {
        const res = await authFetch(`/related-stats?upload_id=${uploadId}`);
        if (!res.ok) throw new Error("Failed to load stats");
        
        const stats = await res.json();
        
        document.getElementById("statDuplicateEmails").innerText = stats.duplicate_emails;
        document.getElementById("statEmailRecords").innerText = stats.total_email_records;
        document.getElementById("statDuplicatePhones").innerText = stats.duplicate_phones;
        document.getElementById("statPhoneRecords").innerText = stats.total_phone_records;
        
        const totalGroups = stats.duplicate_emails + stats.duplicate_phones;
        document.getElementById("statTotalGroups").innerText = totalGroups;
    } catch (err) {
        console.error("Failed to load stats:", err);
    }
}

// ---------------- LOAD GROUPED VIEW ----------------
async function loadGroupedView() {
    showLoading();
    
    try {
        const res = await authFetch(
            `/related-grouped?upload_id=${uploadId}&page=${currentPage}&page_size=${pageSize}`
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

// ---------------- RENDER GROUPED VIEW ----------------
function renderGroupedView(data) {
    const container = document.getElementById('resultsContainer');
    
    if (!data.groups || data.groups.length === 0) {
        container.innerHTML = '<div class="no-results">No related records found</div>';
        return;
    }
    
    let html = '';
    
    data.groups.forEach((group, index) => {
        const groupId = `group-${index}`;
        const recordCount = group.records.length;
        const isEmail = group.match_type === 'email';
        const icon = isEmail ? '📧' : '📱';
        
        html += `
            <div class="group-container">
                <div class="group-header ${group.match_type}" onclick="toggleGroup('${groupId}')">
                    <div class="group-title">
                        <span class="expand-icon" id="expand-${groupId}">▼</span>
                        <span>${icon} ${group.match_key}</span>
                        <span class="group-badge">${recordCount} records</span>
                    </div>
                </div>
                <div class="group-records" id="records-${groupId}">
                    ${renderGroupTable(group.records)}
                </div>
            </div>
        `;
    });
    
    container.innerHTML = html;
}

// ---------------- TOGGLE GROUP ----------------
function toggleGroup(groupId) {
    const recordsDiv = document.getElementById(`records-${groupId}`);
    const expandIcon = document.getElementById(`expand-${groupId}`);
    
    recordsDiv.classList.toggle('collapsed');
    expandIcon.classList.toggle('collapsed');
}

// ---------------- RENDER GROUP TABLE ----------------
function renderGroupTable(records) {
    if (records.length === 0) return '<p>No records</p>';
    
    const preferredOrder = [
        "name",
        "email",
        "phone",
        "city",
        "address",
        "address1",
        "address2",
        "position",
        "zip",
        "status"
    ];

    const allColumns = records[0].data ? Object.keys(records[0].data) : [];

    const columns = [
        ...preferredOrder.filter(c => allColumns.includes(c)),
        ...allColumns.filter(c => !preferredOrder.includes(c))
    ];
    
    let html = '<table>';
    
    // Header
    html += '<thead><tr>';
    columns.forEach(col => {
        html += `<th>${col}</th>`;
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
    
    html += '</table>';
    return html;
}

// ---------------- SEARCH ----------------
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
        
        html += '<table>';
        
        const preferredOrder = [
            "name",
            "email",
            "phone",
            "city",
            "address",
            "address1",
            "address2",
            "status"
        ];

        const allColumns = data.records[0].data ? Object.keys(data.records[0].data) : [];
        const columns = [
            ...preferredOrder.filter(c => allColumns.includes(c)),
            ...allColumns.filter(c => !preferredOrder.includes(c))
        ];
        
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
        
        html += '</table>';
        
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

// ---------------- HELPERS ----------------
function showLoading() {
    document.getElementById("resultsContainer").innerHTML = `
        <div style="text-align: center; padding: 60px; color: #666;">
            ⏳ Loading...
        </div>
    `;
}

function goBack() {
    window.location.href = `/preview.html?upload_id=${uploadId}`;
}

// ---------------- INIT ----------------
async function init() {
    await loadStats();
    await loadGroupedView();
}

init();