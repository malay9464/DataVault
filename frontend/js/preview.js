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

    table.innerHTML =
        "<tr>" +
        data.columns.map(c => `<th>${c}</th>`).join("") +
        "</tr>";

    data.rows.forEach(r => {
        table.innerHTML +=
            "<tr>" +
            r.map(v => `<td>${v ?? ""}</td>`).join("") +
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
