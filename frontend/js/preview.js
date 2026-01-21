/* ---------- AUTH ---------- */
const token = localStorage.getItem("access_token");

if (!token) {
    window.location.href = "/static/login.html";
}

function authFetch(url, options = {}) {
    return fetch(url, {
        ...options,
        headers: {
            ...(options.headers || {}),
            "Authorization": "Bearer " + token
        }
    });
}

/* ---------- PARAMS ---------- */
const params = new URLSearchParams(window.location.search);
const uploadId = params.get("upload_id");

let page = 1;
let pageSize = 50;
let totalRecords = 0;

/* ---------- PAGE SIZE ---------- */
document.getElementById("pageSize").onchange = e => {
    pageSize = parseInt(e.target.value);
    page = 1;
    loadData();
};

/* ---------- LOAD DATA ---------- */
async function loadData() {
    const res = await authFetch(
        `/preview?upload_id=${uploadId}&page=${page}&page_size=${pageSize}`
    );

    if (!res.ok) {
        alert("Unauthorized or data not found");
        return;
    }

    const data = await res.json();
    totalRecords = data.total_records;

    renderTable(data);
    renderPagination(Math.ceil(totalRecords / pageSize));
}

/* ---------- TABLE ---------- */
function renderTable(data) {
    const table = document.getElementById("dataTable");
    table.innerHTML = "";

    if (!data.rows || data.rows.length === 0) {
        table.innerHTML = "<tr><td>No data</td></tr>";
        return;
    }

    // Header
    table.innerHTML =
        "<tr>" +
        data.columns.map(c => `<th>${c}</th>`).join("") +
        "</tr>";

    // Rows
    data.rows.forEach(r => {
        table.innerHTML +=
            "<tr>" +
            r.values.map(v => `<td>${v ?? ""}</td>`).join("") +
            "</tr>";
    });
}

/* ---------- PAGINATION ---------- */
function renderPagination(totalPages) {
    const pagination = document.getElementById("pagination");
    pagination.innerHTML = "";

    if (totalPages <= 1) return;

    const MAX_VISIBLE = 5;

    const prev = document.createElement("button");
    prev.innerText = "Previous";
    prev.disabled = page === 1;
    prev.onclick = () => {
        page--;
        loadData();
    };
    pagination.appendChild(prev);

    let start = Math.max(1, page - Math.floor(MAX_VISIBLE / 2));
    let end = start + MAX_VISIBLE - 1;

    if (end > totalPages) {
        end = totalPages;
        start = Math.max(1, end - MAX_VISIBLE + 1);
    }

    if (start > 1) {
        addPageButton(1);
        if (start > 2) pagination.append("...");
    }

    for (let i = start; i <= end; i++) {
        addPageButton(i);
    }

    if (end < totalPages) {
        if (end < totalPages - 1) pagination.append("...");
        addPageButton(totalPages);
    }

    const next = document.createElement("button");
    next.innerText = "Next";
    next.disabled = page === totalPages;
    next.onclick = () => {
        page++;
        loadData();
    };
    pagination.appendChild(next);
}

function addPageButton(n) {
    const b = document.createElement("button");
    b.innerText = n;
    if (n === page) b.classList.add("active");
    b.onclick = () => {
        page = n;
        loadData();
    };
    pagination.appendChild(b);
}

/* ---------- EXPORT ---------- */
function exportData(format) {
    const btn = document.getElementById("exportBtn");
    const text = document.getElementById("exportText");
    const spinner = document.getElementById("exportSpinner");

    // Disable button + show spinner
    btn.disabled = true;
    spinner.style.display = "inline-block";
    text.innerText = "Exporting...";

    authFetch(`/export?upload_id=${uploadId}&format=${format}`)
        .then(r => {
            if (!r.ok) throw new Error("Export failed");
            return r.blob();
        })
        .then(blob => {
            const url = URL.createObjectURL(blob);
            const a = document.createElement("a");
            a.href = url;
            a.download =
                format === "excel"
                    ? `cleaned_${uploadId}.xlsx`
                    : `cleaned_${uploadId}.csv`;
            a.click();
            URL.revokeObjectURL(url);
        })
        .catch(() => {
            alert("Export failed or session expired");
            localStorage.removeItem("access_token");
            window.location.href = "/static/login.html";
        })
        .finally(() => {
            // Restore button state
            btn.disabled = false;
            spinner.style.display = "none";
            text.innerText = "Export";
        });
}

loadData();

/* ---------- RELATED RECORDS ---------- */
async function viewRelated(rowId) {
    const res = await authFetch(
        `/related-records?upload_id=${uploadId}&row_id=${rowId}`
    );

    if (!res.ok) {
        alert("Failed to load related records");
        return;
    }

    const data = await res.json();

    document.getElementById("relatedMatchInfo").innerText =
        `Matched Email: ${data.match_email || "N/A"} | ` +
        `Matched Phone: ${data.match_phone || "N/A"} | ` +
        `Total: ${data.total}`;

    const table = document.getElementById("relatedTable");
    const thead = table.querySelector("thead");
    const tbody = table.querySelector("tbody");

    thead.innerHTML = "";
    tbody.innerHTML = "";

    if (data.records.length === 0) {
        tbody.innerHTML = "<tr><td>No related records</td></tr>";
    } else {
        const columns = Object.keys(data.records[0].data);

        thead.innerHTML =
            "<tr>" +
            columns.map(c => `<th>${c}</th>`).join("") +
            "</tr>";

        data.records.forEach(r => {
            tbody.innerHTML +=
                "<tr>" +
                columns.map(c => `<td>${r.data[c] ?? ""}</td>`).join("") +
                "</tr>";
        });
    }

    document.getElementById("relatedModal").classList.remove("hidden");
}

function closeRelatedModal() {
    document.getElementById("relatedModal").classList.add("hidden");
}

function openRelatedPage() {
    window.location.href = `/static/related.html?upload_id=${uploadId}`;
}


// Add this function to your preview.js file

function openRelatedRecords() {
    const params = new URLSearchParams(window.location.search);
    const uploadId = params.get("upload_id");
    
    if (!uploadId) {
        alert("No upload ID found");
        return;
    }

    console.log("Opening related records for upload:", uploadId);
    window.location.href = `/related.html?upload_id=${uploadId}`;
}

function openRelatedRecords() {
    const params = new URLSearchParams(window.location.search);
    const uploadId = params.get("upload_id");
    
    if (!uploadId) {
        alert("No upload ID found");
        return;
    }
    
    console.log("Opening related records for upload:", uploadId);
    window.location.href = `/related.html?upload_id=${uploadId}`;
}