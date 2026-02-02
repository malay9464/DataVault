const token = localStorage.getItem("access_token");

if (!token) {
    window.location.href = "/static/login.html";
}

let currentUser = null;
let selectedFile = null;
let currentFilter = "all";
let categoriesCache = [];
let allUploads = [];
let filteredUploads = [];
let page = 1;
const PAGE_SIZE = 10;

const fileInput = document.getElementById("fileInput"); // Assuming ID based on usage
const fileName = document.getElementById("fileName");   // Assuming ID based on usage
const categoryList = document.getElementById("categoryList");
const categorySelect = document.getElementById("categorySelect");
const searchInput = document.getElementById("searchInput");
const uploadTable = document.getElementById("uploadTable");
const pagination = document.getElementById("pagination");
const newCategory = document.getElementById("newCategory");
const allCountSpan = document.getElementById("allCount");
const uncatCountSpan = document.getElementById("uncatCount");
const totalMin = document.getElementById("totalMin");
const totalMax = document.getElementById("totalMax");
const dupMin = document.getElementById("dupMin");
const dupMax = document.getElementById("dupMax");
const dateFrom = document.getElementById("dateFrom");
const dateTo = document.getElementById("dateTo");


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


function showToast(message, type = "success", timeout = 4000) {
    console.log("TOAST:", message, type); // DEBUG

    let container = document.getElementById("toastContainer");

    if (!container) {
        alert("Toast container missing!");
        return;
    }

    const toast = document.createElement("div");
    toast.className = `toast ${type}`;
    toast.textContent = message;

    container.appendChild(toast);

    setTimeout(() => toast.remove(), timeout);
}


fileInput.onchange = () => {
    selectedFile = fileInput.files[0];
    fileName.innerText = selectedFile ? selectedFile.name : "";
};

function buildSearchParams() {
    const p = new URLSearchParams();

    if (searchInput && searchInput.value.trim())
        p.append("filename", searchInput.value.trim());

    if (totalMin && totalMin.value)
        p.append("total_min", totalMin.value);

    if (totalMax && totalMax.value)
        p.append("total_max", totalMax.value);

    if (dupMin && dupMin.value)
        p.append("dup_min", dupMin.value);

    if (dupMax && dupMax.value)
        p.append("dup_max", dupMax.value);

    if (dateFrom && dateFrom.value)
        p.append("date_from", dateFrom.value);

    if (dateTo && dateTo.value)
        p.append("date_to", dateTo.value);

    return p.toString();
}

async function addCategoryPrompt() {
    const name = prompt("Enter category name");
    if (!name) return;

    const res = await authFetch(
        `/categories?name=${encodeURIComponent(name.trim())}`,
        { method: "POST" }
    );

    if (!res.ok) {
        alert("Category already exists");
        return;
    }

    loadCategories();
}

function applyAdvancedSearch() {
    page = 1;
    loadUploads();
}

function resetSearch() {
    searchInput.value = "";
    totalMin.value = "";
    totalMax.value = "";
    dupMin.value = "";
    dupMax.value = "";
    dateFrom.value = "";
    dateTo.value = "";

    page = 1;
    loadUploads();
}

/* ---------- CATEGORIES ---------- */
async function loadCategories() {
    const res = await authFetch("/categories");
    const cats = await res.json();
    categoriesCache = cats;

    categoryList.innerHTML = "";
    categorySelect.innerHTML = "";

    let allCount = 0;
    let uncatCount = 0;

    cats.forEach(c => {
        allCount += c.uploads;

        if (c.name.toLowerCase() === "uncategorized") {
            uncatCount = c.uploads;
        }

        categorySelect.innerHTML += `<option value="${c.id}">${c.name}</option>`;

        if (c.name.toLowerCase() === "uncategorized") {
            return;
        }

        const div = document.createElement("div");
        div.className = "category";

        div.innerHTML = `
            <span onclick="applyFilter(${c.id}, this.parentElement)">
                ${c.name} (${c.uploads})
            </span>
            <span class="cat-actions">
                <button onclick="renameCategory(${c.id}, '${c.name}')">✎</button>
                <button onclick="deleteCategory(${c.id})">✖</button>
            </span>
        `;
        categoryList.appendChild(div);
    });
                   
    allCountSpan.innerText = allCount;
    uncatCountSpan.innerText = uncatCount;
}

/* ---------- FILTER ---------- */
function applyFilter(type, el) {
    currentFilter = type;
    page = 1;
    searchInput.value = "";
               
    document.querySelectorAll(".category")
        .forEach(c => c.classList.remove("active"));
    el.classList.add("active");

    loadUploads();
}

/* ---------- UPLOAD ---------- */
async function upload() {
    console.log("Upload clicked");

    if (!selectedFile) {
        showToast("Please select a file", "warn");
        return;
    }

    const btn = document.getElementById("uploadBtn");
    const spinner = document.getElementById("uploadSpinner");

    btn.disabled = true;
    spinner.style.display = "inline-block";

    try {
        const fd = new FormData();
        fd.append("file", selectedFile);

        const res = await authFetch(
            `/upload?category_id=${categorySelect.value}`,
            { method: "POST", body: fd }
        );

        console.log("Upload response status:", res.status);

        if (res.status === 409) {
            showToast("File already uploaded", "error");
            return;
        }

        if (!res.ok) {
            showToast("Upload failed", "error");
            return;
        }

        // ========== HANDLE RESPONSE ==========
        const result = await res.json();

        // Check if headers need resolution
        if (result.success === false && result.status === 'pending_headers') {
            // Headers need user review - redirect to resolution page
            showToast("Headers need review. Redirecting...", "warn", 2000);
            
            setTimeout(() => {
                window.location.href = `/header.html?upload_id=${result.upload_id}`;
            }, 2000);
            
            return;
        }

        // Normal success case
        if (result.success) {
            showToast("Upload successful", "success");

            selectedFile = null;
            fileInput.value = "";
            fileName.innerText = "";

            loadCategories();
            loadUploads();
        }

    } catch (err) {
        console.error(err);
        showToast("Network error", "error");
    } finally {
        btn.disabled = false;
        spinner.style.display = "none";
    }
}

function openAddUserModal() {
    document.getElementById("addUserModal").style.display = "flex";
}

function closeAddUserModal() {
    document.getElementById("addUserModal").style.display = "none";
}

async function createUser() {
    const email = document.getElementById("newUserEmail").value.trim();
    const password = document.getElementById("newUserPassword").value.trim();
    const role = document.getElementById("newUserRole").value;

    if (!email || !password) {
        alert("Email and password required");
        return;
    }

    const res = await authFetch(
        `/admin/users?email=${encodeURIComponent(email)}&password=${encodeURIComponent(password)}&role=${role}`,
        { method: "POST" }
    );

    if (!res.ok) {
        const err = await res.json();
        alert(err.detail || "Failed to create user");
        return;
    }

    closeAddUserModal();
    alert("User added successfully");

    document.getElementById("newUserEmail").value = "";
    document.getElementById("newUserPassword").value = "";
}


async function loadUser() {
    const res = await authFetch("/me");
    if (!res.ok) {
        logout();
        return;
    }

    currentUser = await res.json();

    if (currentUser.role !== "admin") {
        document
          .querySelectorAll(".new-category-btn")
          .forEach(b => b.style.display = "none");

        document
          .querySelectorAll(".cat-actions")
          .forEach(a => a.style.display = "none");
    }

    if (currentUser.role === "user") {
        document.getElementById("myUploadsTab").style.display = "flex";
    }
}

/* ---------- LOAD UPLOADS ---------- */
async function loadUploads() {
    let url = "";

    // ---------- DECIDE DATA SOURCE ----------
    if (currentFilter === "mine") {
        url = "/my-uploads";
    } else {
        url = "/uploads";
        const params = new URLSearchParams();

        if (currentFilter === "uncat") {
            const u = categoriesCache.find(
                c => c.name.toLowerCase() === "uncategorized"
            );
            if (u) params.append("category_id", u.id);
        } 
        else if (currentFilter !== "all") {
            params.append("category_id", currentFilter);
        }

        // Advanced search params
        const adv = buildSearchParams();
        if (adv) {
            adv.split("&").forEach(p => {
                const [k, v] = p.split("=");
                params.append(k, decodeURIComponent(v));
            });
        }

        if (params.toString()) {
            url += "?" + params.toString();
        }
    }

    // ---------- FETCH ----------
    const res = await authFetch(url);

    if (!res.ok) {
        alert("Failed to load uploads");
        return;
    }

    allUploads = await res.json();
    filteredUploads = [...allUploads];
    renderTable();
}

/* ---------- TABLE + PAGINATION ---------- */
function renderTable() {
    const totalPages = Math.ceil(filteredUploads.length / PAGE_SIZE);
    const start = (page - 1) * PAGE_SIZE;
    const data = filteredUploads.slice(start, start + PAGE_SIZE);

    /* ---------- TABLE HEADER ---------- */
    let header = `
        <tr>
            <th>File</th>
            <th>Category</th>
            <th>Total</th>
            <th>Duplicates</th>
    `;

    if (currentUser.role === "admin") {
        header += `<th>Uploaded By</th>`;
    }

    header += `<th>Actions</th></tr>`;
    uploadTable.innerHTML = header;

    /* ---------- TABLE ROWS ---------- */
    data.forEach(r => {
        let row = `
            <tr>
                <td>${r.filename}</td>
                <td>${r.category}</td>
                <td>${r.total_records}</td>
                <td>${r.duplicate_records}</td>
        `;

        if (currentUser.role === "admin") {
            row += `<td>${r.uploaded_by}</td>`;
        }

        const canDelete =
            currentUser.role === "admin" ||
            r.created_by_user_id === currentUser.id;

        row += `
            <td>
                <div class="action-group">
                    <button class="btn-view"
                        onclick="location.href='/preview.html?upload_id=${r.upload_id}'">
                        View
                    </button>

                    <button class="btn-delete ${canDelete ? "" : "disabled"}"
                        ${canDelete ? `onclick="del(${r.upload_id})"` : "disabled"}>
                        Delete
                    </button>
                </div>
            </td>
        </tr>
        `;

        uploadTable.innerHTML += row;
    });

    renderPagination(totalPages);
}

function renderPagination(totalPages) {
    pagination.innerHTML = "";
    if (totalPages <= 1) return;

    const prev = document.createElement("button");
    prev.innerText = "Previous";
    prev.disabled = page === 1;
    prev.onclick = () => {
        page--;
        renderTable();
    };
    pagination.appendChild(prev);

    let start = Math.max(1, page - 2);
    let end = Math.min(totalPages, page + 2);

    if (start > 1) addPage(1);
    if (start > 2) pagination.append("...");

    for (let i = start; i <= end; i++) addPage(i);

    if (end < totalPages - 1) pagination.append("...");
    if (end < totalPages) addPage(totalPages);

    const next = document.createElement("button");
    next.innerText = "Next";
    next.disabled = page === totalPages;
    next.onclick = () => {
        page++;
        renderTable();
    };
    pagination.appendChild(next);
}

function addPage(n) {
    const b = document.createElement("button");
    b.innerText = n;
    if (n === page) b.classList.add("active");
    b.onclick = () => {
        page = n;
        renderTable();
    };
    pagination.appendChild(b);
}

async function renameCategory(id, oldName) {
    const name = prompt("Rename category", oldName);
    if (!name) return;
    await authFetch(`/categories/${id}?name=${encodeURIComponent(name)}`, {
        method: "PUT"
    });
    loadCategories();
    loadUploads();
}

async function deleteCategory(id) {
    if (!confirm("Delete category?")) return;
    const res = await authFetch(`/categories/${id}`, {
        method: "DELETE"
    });
    if (!res.ok) alert("Category has uploads");
    loadCategories();
    loadUploads();
}

/* ---------- DELETE UPLOAD ---------- */
async function del(uid) {
    if (!confirm("Delete this upload?")) return;
    await authFetch(`/upload/${uid}`, {
        method: "DELETE"
    });
    loadCategories();
    loadUploads();
}

function logout() {
    localStorage.removeItem("access_token");
    window.location.href = "/static/login.html";
}

async function initPage() {
    await loadUser();        // sets currentUser
    await loadCategories();  // fills categoriesCache
    await loadUploads();     // now SAFE
}

initPage();

async function resetPasswordPrompt(userId, email) {
    const pwd = prompt(`Enter new password for ${email}`);
    if (!pwd) return;

    const res = await authFetch(
        `/admin/users/${userId}/reset-password?new_password=${encodeURIComponent(pwd)}`,
        { method: "POST" }
    );

    if (!res.ok) {
        alert("Failed to reset password");
        return;
    }

    alert("Password reset successfully");
}
