const token = localStorage.getItem("access_token");

if (!token) {
    window.location.href = "/static/login.html";
}

const urlParams = new URLSearchParams(window.location.search);

let page = parseInt(urlParams.get("page")) || 1;
let currentFilter = urlParams.get("filter") || "all";
let currentUser = null;
let selectedFile = null;
let categoriesCache = [];
let allUploads = [];
let filteredUploads = [];
const PAGE_SIZE = 10;
let selectedUserId = null;

// DOM Elements
const fileInput = document.getElementById("fileInput");
const fileName = document.getElementById("fileName");
const uploadBox = document.getElementById("uploadBox");
const categoryList = document.getElementById("categoryList");
const categorySelect = document.getElementById("categorySelect");
const searchInput = document.getElementById("searchInput");
const uploadTable = document.getElementById("uploadTable");
const pagination = document.getElementById("pagination");
const allCountSpan = document.getElementById("allCount");
const uncatCountSpan = document.getElementById("uncatCount");
const totalMin = document.getElementById("totalMin");
const totalMax = document.getElementById("totalMax");
const dupMin = document.getElementById("dupMin");
const dupMax = document.getElementById("dupMax");
const dateFrom = document.getElementById("dateFrom");
const dateTo = document.getElementById("dateTo");
const advancedFilters = document.getElementById("advancedFilters");
const uploadProgress = document.getElementById("uploadProgress");
const progressFill = document.getElementById("progressFill");
const progressText = document.getElementById("progressText");
const emptyState = document.getElementById("emptyState");
const tableWrapper = document.getElementById("tableWrapper");
const tableSkeleton = document.getElementById("tableSkeleton");

document.addEventListener("keydown", (e) => {
    if (["INPUT", "TEXTAREA", "SELECT"].includes(e.target.tagName)) {
        if (e.key === "Escape") {
            e.target.blur();
        }
        return;
    }

    if (e.key === "/") {
        e.preventDefault();
        searchInput.focus();
    }

    if (e.key === "n" || e.key === "N") {
        e.preventDefault();
        fileInput.click();
    }

    if (e.key === "?") {
        e.preventDefault();
        toggleShortcuts();
    }

    if (e.key === "Escape") {
        closeAddUserModal();
        const shortcutsModal = document.getElementById("shortcutsModal");
        if (shortcutsModal) shortcutsModal.style.display = "none";
    }
});

function toggleShortcuts() {
    const modal = document.getElementById("shortcutsModal");
    if (modal.style.display === "none" || !modal.style.display) {
        modal.style.display = "flex";
    } else {
        modal.style.display = "none";
    }
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

function showToast(message, type = "success", timeout = 4000) {
    const container = document.getElementById("toastContainer");

    if (!container) {
        console.error("Toast container missing!");
        return;
    }

    const toast = document.createElement("div");
    toast.className = `toast ${type}`;
    toast.textContent = message;

    container.appendChild(toast);

    setTimeout(() => {
        toast.style.animation = "toastSlideOut 0.3s ease forwards";
        setTimeout(() => toast.remove(), 300);
    }, timeout);
}

const style = document.createElement("style");
style.textContent = `
    @keyframes toastSlideOut {
        to {
            opacity: 0;
            transform: translateX(100px);
        }
    }
`;
document.head.appendChild(style);

function checkUploadReady() {
    const hasFile = !!selectedFile;
    const hasCategory = !!categorySelect.value;
    const btn = document.getElementById("uploadBtn");
    btn.disabled = !(hasFile && hasCategory);
}

fileInput.onchange = () => {
    selectedFile = fileInput.files[0];

    if (selectedFile) {
        const fileSize = (selectedFile.size / 1024 / 1024).toFixed(2);
        const fileType = selectedFile.name.split('.').pop().toUpperCase();

        fileName.innerHTML = `
            <div style="display: flex; flex-direction: column; gap: 4px; text-align: left;">
                <div style="font-weight: 600; color: #1e40af;">${selectedFile.name}</div>
                <div style="font-size: 12px; color: #64748b;">
                    ${fileSize} MB • ${fileType} Format
                </div>
            </div>
        `;
        fileName.style.display = "flex";

        uploadBox.style.borderColor = "#16a34a";
        uploadBox.style.background = "#f0fdf4";
    } else {
        fileName.innerText = "";
        fileName.style.display = "none";
        uploadBox.style.borderColor = "";
        uploadBox.style.background = "";
    }

    checkUploadReady();
};

categorySelect.addEventListener("change", () => {
    checkUploadReady();
});

uploadBox.addEventListener("dragover", (e) => {
    e.preventDefault();
    uploadBox.style.borderColor = "#16a34a";
    uploadBox.style.background = "#f0fdf4";
    uploadBox.style.transform = "scale(1.02)";
});

uploadBox.addEventListener("dragleave", () => {
    uploadBox.style.borderColor = "";
    uploadBox.style.background = "";
    uploadBox.style.transform = "";
});

uploadBox.addEventListener("drop", (e) => {
    e.preventDefault();
    uploadBox.style.borderColor = "";
    uploadBox.style.background = "";
    uploadBox.style.transform = "";

    const files = e.dataTransfer.files;
    if (files.length > 0) {
        fileInput.files = files;
        fileInput.dispatchEvent(new Event("change"));
    }
});

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

let searchTimeout;
searchInput.addEventListener("input", () => {
    clearTimeout(searchTimeout);
    searchTimeout = setTimeout(() => {
        page = 1;
        loadUploads();
    }, 300);
});

async function addCategoryPrompt() {
    const name = prompt("Enter category name");
    if (!name) return;

    const res = await authFetch(
        `/categories?name=${encodeURIComponent(name.trim())}`,
        { method: "POST" }
    );

    if (!res.ok) {
        showToast("Category already exists", "error");
        return;
    }

    showToast("Category created successfully", "success");
    loadCategories();
}

async function loadCategories() {
    const res = await authFetch("/categories");
    const cats = await res.json();
    categoriesCache = cats;

    categorySelect.innerHTML = `<option value="" disabled selected>— Select a category —</option>`;
    cats.forEach(c => {
        categorySelect.innerHTML += `<option value="${c.id}">${c.name}</option>`;
    });

    // Only render category sidebar for non-admin
    if (currentUser && currentUser.role !== "admin") {
        categoryList.innerHTML = "";

        let allCount = 0;
        let uncatCount = 0;

        cats.forEach(c => {
            allCount += c.uploads;
            if (c.name.toLowerCase() === "uncategorized") {
                uncatCount = c.uploads;
                return;
            }

            const div = document.createElement("div");
            div.className = "category";

            div.innerHTML = `
                <span onclick="applyFilter(${c.id}, this.parentElement)">
                    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <path d="M22 19a2 2 0 01-2 2H4a2 2 0 01-2-2V9a2 2 0 00-2-2h-6l-2-2H5a2 2 0 00-2 2z"/>
                    </svg>
                    ${c.name}
                </span>
                <span style="display: flex; align-items: center; gap: 8px;">
                    <span class="count-badge">${c.uploads}</span>
                    <span class="cat-actions" style="display:flex">
                        <button onclick="event.stopPropagation(); renameCategory(${c.id}, '${c.name}')">✎</button>
                        <button onclick="event.stopPropagation(); deleteCategory(${c.id})">✖</button>
                    </span>
                </span>
            `;
            categoryList.appendChild(div);
        });

        allCountSpan.innerText = allCount;
        uncatCountSpan.innerText = uncatCount;
    }

    checkUploadReady();
}

async function loadAdminUserList() {
    const res = await authFetch("/admin/users-with-stats");

    if (!res || !res.ok) {
        console.error("Failed to load user list:", res?.status);
        return;
    }

    const users = await res.json();
    categoryList.innerHTML = "";

    let totalUploads = 0;
    users.forEach(u => totalUploads += u.upload_count);
    allCountSpan.innerText = totalUploads;

    if (users.length === 0) {
        categoryList.innerHTML = `<div style="padding:12px; color:#94a3b8; font-size:13px;">No users found</div>`;
        return;
    }

    users.forEach(u => {
        const div = document.createElement("div");
        div.className = "category";
        div.id = `user-item-${u.id}`;

        const label = u.email.length > 22
            ? u.email.substring(0, 20) + "…"
            : u.email;

        div.innerHTML = `
            <span onclick="filterByUser(${u.id}, this.parentElement)" title="${u.email}">
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none"
                     stroke="currentColor" stroke-width="2">
                    <path d="M20 21v-2a4 4 0 00-4-4H8a4 4 0 00-4 4v2"/>
                    <circle cx="12" cy="7" r="4"/>
                </svg>
                ${label}
            </span>
            <span class="count-badge">${u.upload_count}</span>
        `;
        categoryList.appendChild(div);
    });
}

function filterByUser(userId, el) {
    selectedUserId = userId;
    currentFilter = "all";
    page = 1;

    document.querySelectorAll(".category")
        .forEach(c => c.classList.remove("active"));
    el.classList.add("active");

    loadUploads();
}

function toggleAdvancedFilters() {
    if (advancedFilters.style.display === "none" || !advancedFilters.style.display) {
        advancedFilters.style.display = "grid";
    } else {
        advancedFilters.style.display = "none";
    }
}

function applyFilter(type, el) {
    currentFilter = type;
    selectedUserId = null;
    page = 1;
    updateURL();
    searchInput.value = "";

    document.querySelectorAll(".category")
        .forEach(c => c.classList.remove("active"));
    el.classList.add("active");

    loadUploads();
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

async function upload() {
    if (!selectedFile) {
        showToast("Please select a file", "warn");
        return;
    }

    if (!categorySelect.value) {
        showToast("Please select a category", "warn");
        return;
    }

    const btn = document.getElementById("uploadBtn");
    const spinner = document.getElementById("uploadSpinner");

    btn.disabled = true;
    spinner.style.display = "inline-block";
    uploadProgress.style.display = "block";

    fileInput.disabled = true;
    uploadBox.classList.add("disabled");

    let progress = 0;
    const progressInterval = setInterval(() => {
        progress += Math.random() * 15;
        if (progress > 90) progress = 90;
        progressFill.style.width = progress + "%";
        progressText.textContent = `Uploading... ${Math.round(progress)}%`;
    }, 200);

    try {
        const fd = new FormData();
        fd.append("file", selectedFile);

        const res = await authFetch(
            `/upload?category_id=${categorySelect.value}`,
            { method: "POST", body: fd }
        );

        clearInterval(progressInterval);
        progressFill.style.width = "100%";
        progressText.textContent = "Processing...";

        if (res.status === 409) {
            showToast("File already uploaded", "error");
            uploadProgress.style.display = "none";
            return;
        }

        if (!res.ok) {
            showToast("Upload failed", "error");
            uploadProgress.style.display = "none";
            return;
        }

        const result = await res.json();

        if (result.success === false && result.status === 'pending_headers') {
            showToast("Headers need review. Redirecting...", "warn", 2000);
            setTimeout(() => {
                window.location.href = `/header.html?upload_id=${result.upload_id}`;
            }, 2000);
            return;
        }

        if (result.success) {
            showToast("Upload successful! ✨", "success");

            selectedFile = null;
            fileInput.value = "";
            fileName.innerText = "";
            fileName.style.display = "none";
            uploadBox.style.borderColor = "";
            uploadBox.style.background = "";
            uploadProgress.style.display = "none";
            progressFill.style.width = "0%";

            loadCategories();
            loadUploads();
        }

    } catch (err) {
        clearInterval(progressInterval);
        console.error(err);
        showToast("Network error", "error");
        uploadProgress.style.display = "none";
    } finally {
        spinner.style.display = "none";

        fileInput.disabled = false;
        uploadBox.classList.remove("disabled");

        checkUploadReady();
    }
}

// ========== USER MANAGEMENT ==========
function openAddUserModal() {
    document.getElementById("addUserModal").style.display = "flex";
}

function closeAddUserModal() {
    document.getElementById("addUserModal").style.display = "none";

    document.getElementById("newUserEmail").value = "";
    document.getElementById("newUserPassword").value = "";
    document.querySelector('input[name="userRole"][value="user"]').checked = true;

    const passwordStrength = document.getElementById("passwordStrength");
    if (passwordStrength) passwordStrength.style.display = "none";
}

document.getElementById("newUserPassword")?.addEventListener("input", (e) => {
    const password = e.target.value;
    const strengthEl = document.getElementById("passwordStrength");
    const fillEl = document.getElementById("strengthFill");
    const textEl = document.getElementById("strengthText");

    if (!password) {
        strengthEl.style.display = "none";
        return;
    }

    strengthEl.style.display = "block";

    let strength = 0;
    let label = "";
    let color = "";

    if (password.length >= 8) strength += 25;
    if (password.length >= 12) strength += 25;
    if (/[a-z]/.test(password) && /[A-Z]/.test(password)) strength += 25;
    if (/[0-9]/.test(password)) strength += 12;
    if (/[^a-zA-Z0-9]/.test(password)) strength += 13;

    if (strength < 40) {
        label = "Weak";
        color = "#dc2626";
    } else if (strength < 70) {
        label = "Medium";
        color = "#f59e0b";
    } else {
        label = "Strong";
        color = "#16a34a";
    }

    fillEl.style.width = strength + "%";
    fillEl.style.background = color;
    textEl.textContent = label;
    textEl.style.color = color;
});

async function createUser() {
    const email = document.getElementById("newUserEmail").value.trim();
    const password = document.getElementById("newUserPassword").value.trim();
    const role = document.querySelector('input[name="userRole"]:checked').value;

    if (!email || !password) {
        showToast("Email and password required", "error");
        return;
    }

    const res = await authFetch(
        `/admin/users?email=${encodeURIComponent(email)}&password=${encodeURIComponent(password)}&role=${role}`,
        { method: "POST" }
    );

    if (!res.ok) {
        const err = await res.json();
        showToast(err.detail || "Failed to create user", "error");
        return;
    }

    closeAddUserModal();
    showToast("User added successfully ✓", "success");
}

async function loadUser() {
    const res = await authFetch("/me");
    if (!res.ok) {
        logout();
        return;
    }

    currentUser = await res.json();
    console.log("Current user role:", currentUser.role);

    const newCategoryBtn = document.getElementById("newCategoryBtn");
    const addUserBtn = document.getElementById("addUserBtn");
    const manageUsersBtn = document.getElementById("manageUsersBtn");

    if (currentUser.role === "admin") {
        const label = document.getElementById("sidebarSectionLabel");
        if (label) label.textContent = "Users";
        if (newCategoryBtn) newCategoryBtn.style.display = "none";
        if (addUserBtn) addUserBtn.style.display = "flex";
        if (manageUsersBtn) manageUsersBtn.style.display = "flex";

        // Also hide rename/delete buttons on categories (populated dynamically)
        document.querySelectorAll(".cat-actions")
            .forEach(a => a.style.display = "none");

    } else {
        // User: can manage categories, cannot manage users
        if (newCategoryBtn) newCategoryBtn.style.display = "flex";
        if (addUserBtn) addUserBtn.style.display = "none";
        if (manageUsersBtn) manageUsersBtn.style.display = "none";
    }
}

async function loadUploads(showSkeleton = false) {
    if (showSkeleton) {
        tableSkeleton.style.display = "block";
        tableWrapper.style.display = "none";
        emptyState.style.display = "none";
    }

    const params = new URLSearchParams();

    // Admin: filter by selected user
    if (currentUser.role === "admin" && selectedUserId) {
        params.append("created_by_user_id", selectedUserId);
    }

    // Category / uncat filter — works for both roles
    if (currentFilter === "uncat") {
        // fetch all categories to find uncategorized id
        const uncatEntry = categoriesCache.find(
            c => c.name.toLowerCase() === "uncategorized"
        );
        if (uncatEntry) {
            params.append("category_id", uncatEntry.id);
        }
    } else if (currentFilter !== "all") {
        // numeric category id
        params.append("category_id", currentFilter);
    }

    // Advanced search params
    const adv = buildSearchParams();
    if (adv) {
        adv.split("&").forEach(p => {
            const [k, v] = p.split("=");
            if (v) params.append(k, decodeURIComponent(v));
        });
    }

    const url = "/uploads" + (params.toString() ? "?" + params.toString() : "");

    try {
        const res = await authFetch(url);

        if (!res.ok) {
            showToast("Failed to load uploads", "error");
            tableSkeleton.style.display = "none";
            return;
        }

        allUploads = await res.json();
        filteredUploads = [...allUploads];

        tableSkeleton.style.display = "none";

        if (filteredUploads.length === 0) {
            emptyState.style.display = "block";
            tableWrapper.style.display = "none";
        } else {
            emptyState.style.display = "none";
            tableWrapper.style.display = "block";
            renderTable();
        }
    } catch (err) {
        console.error(err);
        showToast("Network error", "error");
        tableSkeleton.style.display = "none";
    }
}

function renderTable() {
    const totalPages = Math.ceil(filteredUploads.length / PAGE_SIZE);
    const start = (page - 1) * PAGE_SIZE;
    const data = filteredUploads.slice(start, start + PAGE_SIZE);

    let header = `
        <thead>
        <tr>
            <th>File</th>
            <th>Category</th>
            <th>Total Records</th>
            <th>Duplicates</th>
    `;

    if (currentUser.role === "admin") {
        header += `<th>Uploaded By</th>`;
    }

    header += `<th>Status</th><th>Actions</th></tr></thead><tbody>`;
    uploadTable.innerHTML = header;

    data.forEach(r => {
        const dupPercentage = r.total_records > 0
            ? (r.duplicate_records / r.total_records * 100).toFixed(1)
            : 0;

        let statusClass = "status-clean";
        let statusText = "Clean";

        if (dupPercentage > 20) {
            statusClass = "status-warning";
            statusText = `${dupPercentage}% Dup`;
        } else if (dupPercentage > 0) {
            statusText = `${dupPercentage}% Dup`;
        }

        let row = `
            <tr>
                <td>
                    <div style="display: flex; align-items: center; gap: 8px;">
                        <span style="font-size: 18px;">📄</span>
                        <strong>${r.filename}</strong>
                    </div>
                </td>
                <td>${r.category}</td>
                <td>${r.total_records.toLocaleString()}</td>
                <td>${r.duplicate_records.toLocaleString()}</td>
        `;

        if (currentUser.role === "admin") {
            row += `<td>${r.uploaded_by}</td>`;
        }

        row += `
                <td>
                    <span class="status-indicator ${statusClass}">${statusText}</span>
                </td>
        `;

        const canDelete =
            currentUser.role === "admin" ||
            r.created_by_user_id === currentUser.id;

        row += `
            <td>
                <div class="action-group">
                    <button class="btn-view"
                        onclick="location.href='/preview.html?upload_id=${r.upload_id}&from_page=${page}&from_filter=${currentFilter}'">
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

    uploadTable.innerHTML += `</tbody>`;
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
        updateURL();
        renderTable();
        window.scrollTo({ top: 0, behavior: "smooth" });
    };
    pagination.appendChild(prev);

    let start = Math.max(1, page - 2);
    let end = Math.min(totalPages, page + 2);

    if (start > 1) addPage(1);
    if (start > 2) pagination.append(document.createTextNode("..."));

    for (let i = start; i <= end; i++) addPage(i);

    if (end < totalPages - 1) pagination.append(document.createTextNode("..."));
    if (end < totalPages) addPage(totalPages);

    const next = document.createElement("button");
    next.innerText = "Next";
    next.disabled = page === totalPages;
    next.onclick = () => {
        page++;
        updateURL();
        renderTable();
        window.scrollTo({ top: 0, behavior: "smooth" });
    };
    pagination.appendChild(next);
}

function addPage(n) {
    const b = document.createElement("button");
    b.innerText = n;
    if (n === page) b.classList.add("active");
    b.onclick = () => {
        page = n;
        updateURL();
        renderTable();
        window.scrollTo({ top: 0, behavior: "smooth" });
    };
    pagination.appendChild(b);
}

async function renameCategory(id, oldName) {
    const name = prompt("Rename category", oldName);
    if (!name) return;

    const res = await authFetch(`/categories/${id}?name=${encodeURIComponent(name)}`, {
        method: "PUT"
    });

    if (res.ok) {
        showToast("Category renamed successfully", "success");
        loadCategories();
        loadUploads();
    } else {
        showToast("Failed to rename category", "error");
    }
}

async function deleteCategory(id) {
    if (!confirm("Delete this category? Uploads will be moved to Uncategorized.")) return;

    const res = await authFetch(`/categories/${id}`, {
        method: "DELETE"
    });

    if (!res.ok) {
        showToast("Cannot delete - category has uploads", "error");
    } else {
        showToast("Category deleted successfully", "success");
        loadCategories();
        loadUploads();
    }
}

async function del(uid) {
    if (!confirm("Delete this upload? This action cannot be undone.")) return;

    const res = await authFetch(`/upload/${uid}`, {
        method: "DELETE"
    });

    if (res.ok) {
        showToast("Upload deleted successfully", "success");
        loadCategories();
        loadUploads();
    } else {
        showToast("Failed to delete upload", "error");
    }
}

function logout() {
    localStorage.removeItem("access_token");
    window.location.href = "/static/login.html";
}

function updateURL() {
    const params = new URLSearchParams();
    params.set("page", page);
    params.set("filter", currentFilter);
    history.pushState({}, "", `${window.location.pathname}?${params.toString()}`);
}

async function initPage() {
    await loadUser();
    await loadCategories();

    if (currentUser && currentUser.role === "admin") {
        await loadAdminUserList();
    }

    await loadUploads(true);
    checkUploadReady();
}

initPage();