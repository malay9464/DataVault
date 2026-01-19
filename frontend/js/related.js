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

const params = new URLSearchParams(window.location.search);
const uploadId = params.get("upload_id");

let allRecords = [];
let page = 1;
let pageSize = 20;

/* ---------- LOAD SUMMARY ---------- */
async function loadRelatedSummary() {
    const res = await authFetch(
        `/related-summary?upload_id=${uploadId}`
    );

    const data = await res.json();
    allRecords = data.records;
    page = 1;
    renderPage();
}

/* ---------- SEARCH ---------- */
async function searchRelated() {
    const val = document.getElementById("searchValue").value.trim();
    if (!val) return;

    const res = await authFetch(
        `/related-search?upload_id=${uploadId}&value=${val}`
    );

    const data = await res.json();
    allRecords = data.records;
    page = 1;
    renderPage();
}

/* ---------- RENDER CURRENT PAGE ---------- */
function renderPage() {
    const start = (page - 1) * pageSize;
    const end = start + pageSize;
    const pageRecords = allRecords.slice(start, end);

    renderTable(pageRecords);
    renderPagination(Math.ceil(allRecords.length / pageSize));
}

/* ---------- TABLE ---------- */
function renderTable(records) {
    const table = document.getElementById("relatedTable");
    table.innerHTML = "";

    if (records.length === 0) {
        table.innerHTML = "<tr><td>No related records found</td></tr>";
        return;
    }

    const cols = Object.keys(records[0].data).slice(0, 12);

    table.innerHTML =
        "<tr>" +
        cols.map(c => `<th>${c}</th>`).join("") +
        "</tr>";

    records.forEach(r => {
        table.innerHTML +=
            "<tr>" +
            cols.map(c => `<td>${r.data[c] ?? ""}</td>`).join("") +
            "</tr>";
    });
}

/* ---------- PAGINATION ---------- */
function renderPagination(totalPages) {
    const pagination = document.getElementById("relatedPagination");
    pagination.innerHTML = "";

    if (totalPages <= 1) return;

    const MAX_VISIBLE = 5;

    const prev = document.createElement("button");
    prev.innerText = "Previous";
    prev.disabled = page === 1;
    prev.onclick = () => {
        page--;
        renderPage();
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
        renderPage();
    };
    pagination.appendChild(next);
}

function addPageButton(n) {
    const b = document.createElement("button");
    b.innerText = n;
    if (n === page) b.classList.add("active");
    b.onclick = () => {
        page = n;
        renderPage();
    };
    pagination.appendChild(b);
}


/* ---------- INIT ---------- */
loadRelatedSummary();
