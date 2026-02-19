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
let selectedUserId = null;
let selectedCategoryId = null;
const PAGE_SIZE = 10;

// Bulk delete state
let selectedUploadIds = new Set();

// Drag state
let dragUploadId = null;
let dragCurrentCategoryId = null;

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
const userFilterId = urlParams.get("user_filter");
const userFilterEmail = urlParams.get("user_email");

// ─── AUTH FETCH ───────────────────────────────────────────────────────────────
async function authFetch(url, options = {}) {
    const res = await fetch(url, {
        ...options,
        headers: { ...(options.headers || {}), "Authorization": "Bearer " + token }
    });
    if (res.status === 401) {
        localStorage.removeItem("access_token");
        window.location.href = "/static/login.html";
        return;
    }
    return res;
}

// ─── TOAST ────────────────────────────────────────────────────────────────────
function showToast(message, type = "success", timeout = 4000) {
    const container = document.getElementById("toastContainer");
    if (!container) return;
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
style.textContent = `@keyframes toastSlideOut { to { opacity:0; transform:translateX(100px); } }`;
document.head.appendChild(style);

// ─── UPLOAD READINESS ─────────────────────────────────────────────────────────
function checkUploadReady() {
    const btn = document.getElementById("uploadBtn");
    if (btn) btn.disabled = !(selectedFile && categorySelect && categorySelect.value);
}

fileInput.onchange = () => {
    selectedFile = fileInput.files[0];
    if (selectedFile) {
        const fileSize = (selectedFile.size / 1024 / 1024).toFixed(2);
        const fileType = selectedFile.name.split('.').pop().toUpperCase();
        fileName.innerHTML = `
            <div style="display:flex;flex-direction:column;gap:4px;text-align:left;">
                <div style="font-weight:600;color:#1e40af;">${selectedFile.name}</div>
                <div style="font-size:12px;color:#64748b;">${fileSize} MB • ${fileType} Format</div>
            </div>`;
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

if (categorySelect) categorySelect.addEventListener("change", checkUploadReady);

if (uploadBox) {
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
}

// ─── SEARCH PARAMS ────────────────────────────────────────────────────────────
function buildSearchParams() {
    const p = new URLSearchParams();
    if (searchInput?.value.trim()) p.append("filename", searchInput.value.trim());
    if (totalMin?.value) p.append("total_min", totalMin.value);
    if (totalMax?.value) p.append("total_max", totalMax.value);
    if (dupMin?.value) p.append("dup_min", dupMin.value);
    if (dupMax?.value) p.append("dup_max", dupMax.value);
    if (dateFrom?.value) p.append("date_from", dateFrom.value);
    if (dateTo?.value) p.append("date_to", dateTo.value);
    return p.toString();
}

if (searchInput) {
    let searchTimeout;
    searchInput.addEventListener("input", () => {
        clearTimeout(searchTimeout);
        searchTimeout = setTimeout(() => { page = 1; loadUploads(); }, 300);
    });
}

// ─── PANEL SWAP ───────────────────────────────────────────────────────────────
function showPanel(panel) {
    const filesPanel     = document.getElementById("filesPanel");
    const dashboardPanel = document.getElementById("dashboardPanel");
    const dashBtn        = document.getElementById("dashboardBtn");

    if (panel === "dashboard") {
        if (filesPanel)     filesPanel.style.display     = "none";
        if (dashboardPanel) dashboardPanel.style.display = "block";
        document.querySelectorAll(".sidebar .category").forEach(c => c.classList.remove("active"));
        if (dashBtn) dashBtn.classList.add("active");
        history.pushState({}, "", "/?view=dashboard");
        if (typeof loadDashboard === "function") loadDashboard();
    } else {
        if (filesPanel)     filesPanel.style.display     = "block";
        if (dashboardPanel) dashboardPanel.style.display = "none";
        if (dashBtn) dashBtn.classList.remove("active");
        history.pushState({}, "", "/");
    }
}

// ─── MOVE FILE (shared by drag-drop and inline edit) ──────────────────────────
async function moveFile(uploadId, newCategoryId, newCategoryName) {
    const res = await authFetch(`/upload/${uploadId}/move`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ category_id: newCategoryId })
    });
    if (res && res.ok) {
        showToast(`Moved to "${newCategoryName}" ✓`, "success");
        loadCategories();
        loadUploads();
    } else {
        const err = await res.json().catch(() => ({}));
        showToast(err.detail || "Failed to move file", "error");
    }
}

// ─── DRAG & DROP ──────────────────────────────────────────────────────────────
function onRowDragStart(e, uploadId, currentCategoryId) {
    dragUploadId = uploadId;
    dragCurrentCategoryId = currentCategoryId;
    e.dataTransfer.effectAllowed = "move";
    const row = document.getElementById(`row-${uploadId}`);
    setTimeout(() => { if (row) row.classList.add("row-dragging"); }, 0);
}

function onRowDragEnd(uploadId) {
    dragUploadId = null;
    dragCurrentCategoryId = null;
    const row = document.getElementById(`row-${uploadId}`);
    if (row) row.classList.remove("row-dragging");
    document.querySelectorAll(".category.drop-target").forEach(el => {
        el.classList.remove("drop-target");
    });
}

function onCategoryDragOver(e, categoryId) {
    if (!dragUploadId) return;
    if (categoryId === dragCurrentCategoryId) return;
    e.preventDefault();
    e.dataTransfer.dropEffect = "move";
}

function onCategoryDragEnter(e, categoryId) {
    if (!dragUploadId) return;
    if (categoryId === dragCurrentCategoryId) return;
    e.preventDefault();
    e.currentTarget.classList.add("drop-target");
}

function onCategoryDragLeave(e) {
    e.currentTarget.classList.remove("drop-target");
}

async function onCategoryDrop(e, categoryId, categoryName) {
    e.preventDefault();
    e.currentTarget.classList.remove("drop-target");
    if (!dragUploadId) return;
    if (categoryId === dragCurrentCategoryId) return;
    await moveFile(dragUploadId, categoryId, categoryName);
}

// ─── INLINE CATEGORY EDIT ────────────────────────────────────────────────────
function makeInlineCategoryCell(uploadId, currentCategoryId, currentCategoryName, canMove) {
    if (!canMove) {
        return `<td>${currentCategoryName}</td>`;
    }
    return `<td class="category-cell" 
        title="Click to change category"
        onclick="openInlineEdit(this, ${uploadId}, ${currentCategoryId})"
        style="cursor:pointer; position:relative;">
        <span class="category-cell-text">
            ${currentCategoryName}
            <svg class="category-edit-icon" width="12" height="12" viewBox="0 0 24 24" 
                fill="none" stroke="currentColor" stroke-width="2">
                <path d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7"/>
                <path d="M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z"/>
            </svg>
        </span>
    </td>`;
}

function openInlineEdit(cell, uploadId, currentCategoryId) {
    if (cell.querySelector("select")) return;

    const currentText = cell.querySelector(".category-cell-text").textContent.trim();

    const sel = document.createElement("select");
    sel.className = "inline-cat-select";
    // Force dropdown to stay within the column — prevents wide category names from overflowing
    sel.style.cssText = "width:100%;max-width:100%;min-width:0;box-sizing:border-box;";
    sel.innerHTML = categoriesCache
        .map(c => {
            const label = c.name.length > 40 ? c.name.substring(0, 38) + "…" : c.name;
            return `<option value="${c.id}" ${c.id === currentCategoryId ? "selected" : ""}>${label}</option>`;
        })
        .join("");

    cell.innerHTML = "";
    cell.style.overflow = "hidden"; // lock cell during editing
    cell.appendChild(sel);
    sel.focus();

    sel.addEventListener("change", async () => {
        const newId   = parseInt(sel.value);
        const newName = categoriesCache.find(c => c.id === newId)?.name || "";
        if (newId === currentCategoryId) { loadUploads(); return; }
        cell.innerHTML = `<span style="color:#64748b;font-size:13px;">Saving...</span>`;
        await moveFile(uploadId, newId, newName);
    });

    sel.addEventListener("blur", () => {
        setTimeout(() => {
            if (cell.querySelector("select")) {
                cell.style.overflow = "hidden"; // restore after edit closes
                cell.innerHTML = `<span class="category-cell-text">
                    ${currentText}
                    <svg class="category-edit-icon" width="12" height="12" viewBox="0 0 24 24" 
                        fill="none" stroke="currentColor" stroke-width="2">
                        <path d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7"/>
                        <path d="M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z"/>
                    </svg>
                </span>`;
                cell.onclick = () => openInlineEdit(cell, uploadId, currentCategoryId);
            }
        }, 150);
    });

    sel.addEventListener("keydown", (e) => {
        if (e.key === "Escape") { sel.blur(); }
    });
}

// ─── CATEGORIES ───────────────────────────────────────────────────────────────
async function addCategoryPrompt() {
    const name = prompt("Enter category name");
    if (!name) return;
    const res = await authFetch(`/categories?name=${encodeURIComponent(name.trim())}`, { method: "POST" });
    if (!res.ok) { showToast("Category already exists", "error"); return; }
    showToast("Category created successfully", "success");
    loadCategories();
}

async function loadCategories() {
    const res = await authFetch("/categories");
    const cats = await res.json();
    categoriesCache = cats;

    if (categorySelect) {
        categorySelect.innerHTML = `<option value="" disabled selected>— Select a category —</option>`;
        cats.forEach(c => {
            const displayName = c.name.length > 35 ? c.name.substring(0, 33) + '…' : c.name;
            categorySelect.innerHTML += `<option value="${c.id}" title="${c.name}">${displayName}</option>`;
        });
    }

    if (currentUser && currentUser.role !== "admin") {
        categoryList.innerHTML = "";
        let allCount = 0, uncatCount = 0;

        cats.forEach(c => {
            allCount += c.uploads;
            const div = document.createElement("div");
            div.className = "category";
            div.dataset.categoryId = c.id;
            div.dataset.categoryName = c.name;

            div.addEventListener("dragover",  (e) => onCategoryDragOver(e, c.id));
            div.addEventListener("dragenter", (e) => onCategoryDragEnter(e, c.id));
            div.addEventListener("dragleave", (e) => onCategoryDragLeave(e));
            div.addEventListener("drop",      (e) => onCategoryDrop(e, c.id, c.name));

            const escapedName = c.name.replace(/'/g, "\\'");
            div.innerHTML = `
                <span onclick="applyFilter(${c.id}, this.parentElement)" title="${c.name}">
                    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <path d="M22 19a2 2 0 01-2 2H4a2 2 0 01-2-2V9a2 2 0 00-2-2h-6l-2-2H5a2 2 0 00-2 2z"/>
                    </svg>
                    <span style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${c.name}</span>
                </span>
                <span style="display:flex;align-items:center;gap:8px;">
                    <span class="count-badge">${c.uploads}</span>
                    <span class="cat-actions">
                        <button onclick="event.stopPropagation(); renameCategory(${c.id}, '${escapedName}')">✎</button>
                        <button onclick="event.stopPropagation(); deleteCategory(${c.id})">✖</button>
                    </span>
                </span>`;            
                categoryList.appendChild(div);
        });

        if (allCountSpan) allCountSpan.innerText = allCount;

        if (allCountSpan) allCountSpan.innerText = allCount;
        if (uncatCountSpan) uncatCountSpan.innerText = uncatCount;
    }

    checkUploadReady();
}

// ─── ADMIN USER LIST ──────────────────────────────────────────────────────────
async function loadAdminUserList() {
    const res = await authFetch("/admin/users-with-stats");
    if (!res || !res.ok) return;
    const users = await res.json();
    categoryList.innerHTML = "";

    let totalUploads = 0;
    users.forEach(u => totalUploads += u.upload_count);
    if (allCountSpan) allCountSpan.innerText = totalUploads;

    if (users.length === 0) {
        categoryList.innerHTML = `<div style="padding:12px;color:#94a3b8;font-size:13px;">No users found</div>`;
        return;
    }

    users.forEach(u => {
        const div = document.createElement("div");
        div.className = "category";
        div.id = `user-item-${u.id}`;
        const label = u.email.length > 22 ? u.email.substring(0, 20) + "…" : u.email;
        div.innerHTML = `
            <span onclick="filterByUser(${u.id}, this.parentElement)" title="${u.email}">
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M20 21v-2a4 4 0 00-4-4H8a4 4 0 00-4 4v2"/>
                    <circle cx="12" cy="7" r="4"/>
                </svg>
                ${label}
            </span>
            <span class="count-badge">${u.upload_count}</span>`;
        categoryList.appendChild(div);
    });
}

async function loadUserCategories(userId) {
    const res = await authFetch(`/categories?user_id=${userId}`);
    if (!res || !res.ok) return;
    const cats = await res.json();
    removeUserCategoryBar();
    if (cats.length === 0) return;

    const wrapper = document.createElement("div");
    wrapper.id = "userCategoryBar";
    wrapper.style.cssText = "padding:8px 0 4px 0;position:relative;";
    wrapper.innerHTML = `
        <div style="display:flex;align-items:center;gap:8px;padding:0 0 8px 0;">
            <span style="font-size:12px;font-weight:600;color:#64748b;text-transform:uppercase;letter-spacing:0.5px;">Sort by</span>
            <div style="position:relative;flex:1;">
                <select id="adminCatDropdown" onchange="filterByCategoryDropdown(this)"
                    style="width:100%;max-width:400px;padding:7px 32px 7px 12px;border:1.5px solid #e2e8f0;border-radius:8px;
                        background:#fff;color:#374151;font-size:13px;font-weight:500;cursor:pointer;
                        appearance:none;outline:none;">
                    <option value="">All Categories</option>
                    ${cats.map(c => {
                        const displayName = c.name.length > 45 ? c.name.substring(0, 43) + '…' : c.name;
                        return `<option value="${c.id}" title="${c.name}">${displayName} (${c.uploads})</option>`;
                    }).join("")}
                </select>
            </div>
        </div>`;

        const searchContainer = document.querySelector(".search-container");
    if (searchContainer) searchContainer.parentNode.insertBefore(wrapper, searchContainer);
}

function removeUserCategoryBar() {
    const existing = document.getElementById("userCategoryBar");
    if (existing) existing.remove();
}

function filterByCategoryDropdown(select) {
    selectedCategoryId = select.value ? parseInt(select.value) : null;
    page = 1;
    loadUploads();
}

function filterByUser(userId, el) {
    selectedUserId = userId;
    selectedCategoryId = null;
    currentFilter = "all";
    page = 1;
    clearSelection();
    removeUserCategoryBar();
    document.querySelectorAll(".category").forEach(c => c.classList.remove("active"));
    el.classList.add("active");
    showPanel("files");
    loadUserCategories(userId);

    // ── UPDATE HEADING ──
    const heading = document.querySelector(".section-header h3");
    if (heading) {
        const userItem = document.getElementById(`user-item-${userId}`);
        const email = userItem?.querySelector("span")?.title || `User #${userId}`;
        heading.innerHTML = `<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M20 21v-2a4 4 0 00-4-4H8a4 4 0 00-4 4v2"/><circle cx="12" cy="7" r="4"/></svg> ${email}`;
    }

    loadUploads();
}

function toggleAdvancedFilters() {
    advancedFilters.style.display =
        (advancedFilters.style.display === "none" || !advancedFilters.style.display) ? "grid" : "none";
}

function applyFilter(type, el) {
    currentFilter = type;
    selectedUserId = null;
    selectedCategoryId = null;
    page = 1;
    clearSelection();
    removeUserCategoryBar();
    updateURL();
    if (searchInput) searchInput.value = "";
    document.querySelectorAll(".category").forEach(c => c.classList.remove("active"));
    if (el) el.classList.add("active");
    showPanel("files");

    // ── UPDATE HEADING ──
    const heading = document.querySelector(".section-header h3");
    if (heading) {
        if (type === "all") {
            heading.innerHTML = `<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 19a2 2 0 01-2 2H4a2 2 0 01-2-2V5a2 2 0 012-2h5l2 3h9a2 2 0 012 2z"/></svg> Recent Uploads`;
        } else {
            const catName = categoriesCache.find(c => c.id === type)?.name || "Category";
            heading.innerHTML = `<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 19a2 2 0 01-2 2H4a2 2 0 01-2-2V5a2 2 0 012-2h5l2 3h9a2 2 0 012 2z"/></svg> ${catName}`;
        }
    }

    loadUploads();
}

function applyAdvancedSearch() { page = 1; loadUploads(); }

function resetSearch() {
    if (searchInput) searchInput.value = "";
    if (totalMin) totalMin.value = "";
    if (totalMax) totalMax.value = "";
    if (dupMin) dupMin.value = "";
    if (dupMax) dupMax.value = "";
    if (dateFrom) dateFrom.value = "";
    if (dateTo) dateTo.value = "";
    page = 1;
    loadUploads();
}

// ─── UPLOAD ───────────────────────────────────────────────────────────────────
async function upload() {
    if (!selectedFile) { showToast("Please select a file", "warn"); return; }
    if (!categorySelect.value) { showToast("Please select a category", "warn"); return; }

    const btn = document.getElementById("uploadBtn");
    const spinner = document.getElementById("uploadSpinner");
    btn.disabled = true;
    spinner.style.display = "inline-block";
    uploadProgress.style.display = "block";
    fileInput.disabled = true;
    uploadBox.classList.add("disabled");
    progressFill.style.width = "0%";
    progressText.textContent = "Starting...";

    const uploadId = Date.now() * 1000;
    const sseUrl = `/upload-progress/${uploadId}?token=${localStorage.getItem("access_token")}`;
    const eventSource = new EventSource(sseUrl);

    eventSource.onmessage = (e) => {
        try {
            const data = JSON.parse(e.data);
            const pct = data.percent || 0;
            progressFill.style.width = pct + "%";
            progressText.textContent = data.message || `${pct}%`;
            if (data.status === "done") { progressFill.style.width = "100%"; eventSource.close(); }
            if (data.status === "error") {
                eventSource.close();
                showToast(data.message || "Upload failed", "error");
                uploadProgress.style.display = "none";
                resetUploadState();
            }
        } catch (err) { console.error("SSE parse error:", err); }
    };
    eventSource.onerror = () => { eventSource.close(); };

    try {
        const fd = new FormData();
        fd.append("file", selectedFile);
        const res = await authFetch(
            `/upload?category_id=${categorySelect.value}&upload_id_hint=${uploadId}`,
            { method: "POST", body: fd }
        );
        eventSource.close();

        if (res.status === 409) {
            showToast("File already uploaded", "error");
            uploadProgress.style.display = "none";
            resetUploadState();
            return;
        }
        if (!res.ok) {
            const err = await res.json().catch(() => ({}));
            showToast(err.detail || "Upload failed", "error");
            uploadProgress.style.display = "none";
            resetUploadState();
            return;
        }

        const result = await res.json();
        if (result.success === false && result.status === 'pending_headers') {
            showToast("Headers need review. Redirecting...", "warn", 2000);
            uploadProgress.style.display = "none";
            setTimeout(() => { window.location.href = `/header.html?upload_id=${result.upload_id}`; }, 2000);
            return;
        }
        if (result.success) {
            showToast("File uploaded! Processing in background... ⏳", "success");
            uploadProgress.style.display = "none";
            progressFill.style.width = "0%";
            clearUploadFields();
            loadCategories();
            loadUploads();
            if (result.status === "processing") {
                pollUploadStatus(result.upload_id);
            }
        }
    } catch (err) {
        eventSource.close();
        showToast("Network error", "error");
        uploadProgress.style.display = "none";
        resetUploadState();
    } finally {
        spinner.style.display = "none";
        fileInput.disabled = false;
        uploadBox.classList.remove("disabled");
        checkUploadReady();
    }
}

function pollUploadStatus(uploadId) {
    const maxAttempts = 120;
    let attempts = 0;
    const interval = setInterval(async () => {
        attempts++;
        if (attempts > maxAttempts) { clearInterval(interval); return; }
        try {
            const res = await authFetch(`/upload/${uploadId}/status`);
            if (!res.ok) { clearInterval(interval); return; }
            const data = await res.json();
            if (data.processing_status === "ready") {
                clearInterval(interval);
                showToast(`✅ Processing complete! ${fmtNum(data.total_records)} records ready.`, "success", 4000);
                loadUploads();
                loadCategories();
            } else if (data.processing_status === "failed") {
                clearInterval(interval);
                showToast("❌ File processing failed. Please try again.", "error", 5000);
                loadUploads();
            }
        } catch (err) { clearInterval(interval); }
    }, 5000);
}

function clearUploadFields() {
    selectedFile = null;
    fileInput.value = "";
    fileName.innerText = "";
    fileName.style.display = "none";
    uploadBox.style.borderColor = "";
    uploadBox.style.background = "";
}

function resetUploadState() {
    clearUploadFields();
    document.getElementById("uploadBtn").disabled = false;
    document.getElementById("uploadSpinner").style.display = "none";
    fileInput.disabled = false;
    uploadBox.classList.remove("disabled");
    checkUploadReady();
}

// ─── USER MANAGEMENT ──────────────────────────────────────────────────────────
function openAddUserModal() { document.getElementById("addUserModal").style.display = "flex"; }

function closeAddUserModal() {
    document.getElementById("addUserModal").style.display = "none";
    document.getElementById("newUserEmail").value = "";
    document.getElementById("newUserPassword").value = "";
    document.querySelector('input[name="userRole"][value="user"]').checked = true;
    const ps = document.getElementById("passwordStrength");
    if (ps) ps.style.display = "none";
}

document.getElementById("newUserPassword")?.addEventListener("input", (e) => {
    const password = e.target.value;
    const strengthEl = document.getElementById("passwordStrength");
    const fillEl = document.getElementById("strengthFill");
    const textEl = document.getElementById("strengthText");
    if (!password) { strengthEl.style.display = "none"; return; }
    strengthEl.style.display = "block";
    let strength = 0;
    if (password.length >= 8) strength += 25;
    if (password.length >= 12) strength += 25;
    if (/[a-z]/.test(password) && /[A-Z]/.test(password)) strength += 25;
    if (/[0-9]/.test(password)) strength += 12;
    if (/[^a-zA-Z0-9]/.test(password)) strength += 13;
    let label, color;
    if (strength < 40) { label = "Weak"; color = "#dc2626"; }
    else if (strength < 70) { label = "Medium"; color = "#f59e0b"; }
    else { label = "Strong"; color = "#16a34a"; }
    fillEl.style.width = strength + "%";
    fillEl.style.background = color;
    textEl.textContent = label;
    textEl.style.color = color;
});

async function createUser() {
    const email = document.getElementById("newUserEmail").value.trim();
    const password = document.getElementById("newUserPassword").value.trim();
    const role = document.querySelector('input[name="userRole"]:checked').value;
    if (!email || !password) { showToast("Email and password required", "error"); return; }
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
    if (currentUser && currentUser.role === "admin") loadAdminUserList();
}

// ─── LOAD USER ────────────────────────────────────────────────────────────────
async function loadUser() {
    const res = await authFetch("/me");
    if (!res.ok) { logout(); return; }
    currentUser = await res.json();

    const newCategoryBtn = document.getElementById("newCategoryBtn");
    const addUserBtn     = document.getElementById("addUserBtn");
    const manageUsersBtn = document.getElementById("manageUsersBtn");
    const uploadSection  = document.querySelector(".upload-section");
    const divider        = document.querySelector(".divider");
    const dashBtn        = document.getElementById("dashboardBtn");
    const label          = document.getElementById("sidebarSectionLabel");

    if (currentUser.role === "admin") {
        if (label)          label.textContent          = "Users";
        if (newCategoryBtn) newCategoryBtn.style.display = "none";
        if (addUserBtn)     addUserBtn.style.display     = "flex";
        if (manageUsersBtn) manageUsersBtn.style.display = "flex";
        if (uploadSection)  uploadSection.style.display  = "none";
        if (divider)        divider.style.display        = "none";
        if (dashBtn)        dashBtn.style.display        = "flex";
        document.querySelectorAll(".cat-actions").forEach(a => a.style.display = "none");
        const params = new URLSearchParams(window.location.search);
        if (params.get("view") === "dashboard") {
            showPanel("dashboard");
        }
    } else {
        if (dashBtn)        dashBtn.style.display        = "none";
        if (addUserBtn)     addUserBtn.style.display      = "none";
        if (manageUsersBtn) manageUsersBtn.style.display  = "none";
    }
}

// ─── LOAD UPLOADS ─────────────────────────────────────────────────────────────
async function loadUploads(showSkeleton = false) {
    if (showSkeleton) {
        tableSkeleton.style.display = "block";
        tableWrapper.style.display = "none";
        emptyState.style.display = "none";
    }

    const params = new URLSearchParams();

    if (currentUser.role === "admin" && selectedUserId)
        params.append("created_by_user_id", selectedUserId);
    if (currentUser.role === "admin" && selectedCategoryId)
        params.append("category_id", selectedCategoryId);

    if (currentUser.role !== "admin") {
        if (currentFilter !== "all") {
            params.append("category_id", currentFilter);
        }
    }

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
        if (!res.ok) { showToast("Failed to load uploads", "error"); tableSkeleton.style.display = "none"; return; }

        allUploads = await res.json();
        filteredUploads = [...allUploads];
        tableSkeleton.style.display = "none";
        clearSelection();

        if (filteredUploads.length === 0) {
            tableWrapper.style.display = "none";
            const hasAdvancedFilter = (
                (totalMin?.value) || (totalMax?.value) ||
                (dupMin?.value) || (dupMax?.value) ||
                (dateFrom?.value) || (dateTo?.value) ||
                (searchInput?.value.trim())
            );
            const emptyMsg = document.getElementById("emptyMessage");
            const emptyReset = document.getElementById("emptyReset");
            if (hasAdvancedFilter) {
                if (emptyMsg) emptyMsg.textContent = "No uploads match your filters";
                if (emptyReset) emptyReset.style.display = "inline-block";
            } else {
                if (emptyMsg) emptyMsg.textContent = "No files have been uploaded yet";
                if (emptyReset) emptyReset.style.display = "none";
            }
            emptyState.style.display = "block";
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

// ─── NUMBER FORMAT HELPERS ────────────────────────────────────────────────────
function fmtNum(n) {
    if (n === null || n === undefined) return "—";
    if (n >= 1_000_000) return (n / 1_000_000).toFixed(1).replace(/\.0$/, "") + "M";
    if (n >= 1_000)     return (n / 1_000).toFixed(1).replace(/\.0$/, "") + "K";
    return n.toLocaleString();
}

function fmtNumFull(n) {
    if (n === null || n === undefined) return "";
    return n.toLocaleString();
}

function fmtDate(dateStr) {
    if (!dateStr) return "—";
    return new Date(dateStr).toLocaleDateString("en-GB", {
        day: "numeric", month: "short", year: "numeric"
    });
}

// ─── RENDER TABLE ─────────────────────────────────────────────────────────────
function renderTable() {
    const totalPages = Math.ceil(filteredUploads.length / PAGE_SIZE);
    const start = (page - 1) * PAGE_SIZE;
    const data = filteredUploads.slice(start, start + PAGE_SIZE);

    const pageIds = data.map(r => r.upload_id);
    const allPageSelected = pageIds.length > 0 && pageIds.every(id => selectedUploadIds.has(id));

    let header = `<thead><tr>
        <th style="width:44px; text-align:center;">
            <input type="checkbox" id="selectAllCheckbox"
                ${allPageSelected ? "checked" : ""}
                onchange="toggleSelectAll(this)"
                style="cursor:pointer; width:16px; height:16px;">
        </th>
        <th>File</th>
        <th>Category</th>
        <th>Total Records</th>
        <th>Duplicates</th>`;

    if (currentUser.role === "admin") header += `<th>Uploaded By</th>`;
    header += `<th>Status</th><th>Actions</th></tr></thead><tbody>`;
    uploadTable.innerHTML = header;

    data.forEach(r => {
        const isProcessing = r.processing_status === 'processing';
        const isFailed = r.processing_status === 'failed';

        const dupPercentage = r.total_records > 0
            ? (r.duplicate_records / r.total_records * 100).toFixed(1) : 0;
        let statusClass = "status-clean";
        let statusText = "Clean";
        if (isProcessing) { statusClass = "status-processing"; statusText = "Processing"; }
        else if (isFailed) { statusClass = "status-warning"; statusText = "Failed"; }
        else if (dupPercentage > 20) { statusClass = "status-warning"; statusText = `${dupPercentage}% Dup`; }
        else if (dupPercentage > 0) { statusText = `${dupPercentage}% Dup`; }

        const isChecked = selectedUploadIds.has(r.upload_id);
        const canDelete = currentUser.role === "admin" || r.created_by_user_id === currentUser.id;
        const canMove   = currentUser.role !== "admin" && !isProcessing;

        // ── TOTAL RECORDS cell — compact number + tooltip with full number ──
        let totalCell, totalAttr = "";
        if (isProcessing) {
            totalCell = '<span class="processing-badge">⏳ Processing...</span>';
        } else if (isFailed) {
            totalCell = '<span class="failed-badge">❌ Failed</span>';
        } else {
            const raw = r.total_records;
            totalCell = fmtNum(raw);
            if (raw >= 1000) totalAttr = `data-full-number="${fmtNumFull(raw)}"`;
        }

        // ── DUPLICATES cell — compact number + tooltip ──
        let dupCell = "—", dupAttr = "";
        if (!isProcessing && r.duplicate_records !== null && r.duplicate_records !== undefined) {
            const rawDup = r.duplicate_records;
            dupCell = fmtNum(rawDup);
            if (rawDup >= 1000) dupAttr = `data-full-number="${fmtNumFull(rawDup)}"`;
        }

        let viewBtn = isProcessing
            ? `<button class="btn-view disabled" disabled title="File is still processing...">⏳ Processing</button>`
            : `<button class="btn-view" onclick="location.href='/preview.html?upload_id=${r.upload_id}&from_page=${page}&from_filter=${currentFilter}'">View</button>`;

        const deleteBtn = `<button class="btn-delete ${canDelete ? '' : 'disabled'}"
            ${canDelete ? `onclick="del(${r.upload_id})"` : 'disabled'}>Delete</button>`;

        const adminCol = currentUser.role === "admin" ? `<td>${r.uploaded_by}</td>` : '';

        const categoryCell = makeInlineCategoryCell(r.upload_id, r.category_id, r.category, canMove);

        const draggable = canMove ? `draggable="true"
            ondragstart="onRowDragStart(event, ${r.upload_id}, ${r.category_id})"
            ondragend="onRowDragEnd(${r.upload_id})"` : '';

        uploadTable.innerHTML += `<tr id="row-${r.upload_id}" 
            class="${isChecked ? 'row-selected' : ''}" ${draggable}>
            <td style="text-align:center;">
                <input type="checkbox" class="row-checkbox"
                    data-id="${r.upload_id}"
                    ${isChecked ? "checked" : ""}
                    ${canDelete ? "" : "disabled"}
                    onchange="toggleRowSelect(this, ${r.upload_id})"
                    style="cursor:pointer; width:16px; height:16px;">
            </td>
            <td title="${r.filename}">
                <div style="display:flex;align-items:center;gap:8px;">
                    ${canMove ? `<span class="drag-handle" title="Drag to move">⠿</span>` : ''}
                    <span style="font-size:18px;flex-shrink:0;">📄</span>
                    <strong style="overflow:hidden;white-space:nowrap;text-overflow:ellipsis;display:block;">${r.filename}</strong>
                </div>
            </td>
            ${categoryCell}
            <td ${totalAttr}>${totalCell}</td>
            <td ${dupAttr}>${dupCell}</td>
            ${adminCol}
            <td><span class="status-indicator ${statusClass}">${statusText}</span></td>
            <td><div class="action-group">${viewBtn}${deleteBtn}</div></td>
        </tr>`;
    });

    uploadTable.innerHTML += `</tbody>`;
    renderPagination(totalPages);
    updateBulkBar();
}

// ─── BULK SELECT ──────────────────────────────────────────────────────────────
function toggleSelectAll(checkbox) {
    const start = (page - 1) * PAGE_SIZE;
    const data = filteredUploads.slice(start, start + PAGE_SIZE);
    data.forEach(r => {
        const canDelete = currentUser.role === "admin" || r.created_by_user_id === currentUser.id;
        if (!canDelete) return;
        if (checkbox.checked) selectedUploadIds.add(r.upload_id);
        else selectedUploadIds.delete(r.upload_id);
    });
    document.querySelectorAll(".row-checkbox").forEach(cb => {
        const id = parseInt(cb.dataset.id);
        cb.checked = selectedUploadIds.has(id);
        const row = document.getElementById(`row-${id}`);
        if (row) row.classList.toggle("row-selected", cb.checked);
    });
    updateBulkBar();
}

function toggleRowSelect(checkbox, uploadId) {
    if (checkbox.checked) selectedUploadIds.add(uploadId);
    else selectedUploadIds.delete(uploadId);
    const row = document.getElementById(`row-${uploadId}`);
    if (row) row.classList.toggle("row-selected", checkbox.checked);
    const start = (page - 1) * PAGE_SIZE;
    const data = filteredUploads.slice(start, start + PAGE_SIZE);
    const allSelected = data.map(r => r.upload_id).every(id => selectedUploadIds.has(id));
    const selectAll = document.getElementById("selectAllCheckbox");
    if (selectAll) selectAll.checked = allSelected;
    updateBulkBar();
}

function updateBulkBar() {
    const bar = document.getElementById("bulkActionBar");
    const countEl = document.getElementById("bulkSelectedCount");
    const count = selectedUploadIds.size;
    if (count > 0) {
        bar.style.display = "flex";
        countEl.textContent = `${count} file${count !== 1 ? "s" : ""} selected`;
    } else {
        bar.style.display = "none";
    }
}

function clearSelection() {
    selectedUploadIds.clear();
    updateBulkBar();
    document.querySelectorAll(".row-checkbox").forEach(cb => { cb.checked = false; });
    const selectAll = document.getElementById("selectAllCheckbox");
    if (selectAll) selectAll.checked = false;
    document.querySelectorAll(".row-selected").forEach(row => row.classList.remove("row-selected"));
}

// ─── BULK DELETE ──────────────────────────────────────────────────────────────
async function bulkDelete() {
    const ids = Array.from(selectedUploadIds);
    if (ids.length === 0) return;
    if (!confirm(`Delete ${ids.length} file${ids.length !== 1 ? "s" : ""}? This action cannot be undone.`)) return;
    const res = await authFetch("/uploads/bulk", {
        method: "DELETE",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ upload_ids: ids })
    });
    if (res.ok) {
        const result = await res.json();
        showToast(`${result.deleted_count} file${result.deleted_count !== 1 ? "s" : ""} deleted successfully`, "success");
        clearSelection();
        loadCategories();
        loadUploads();
    } else {
        const err = await res.json().catch(() => ({}));
        showToast(err.detail || "Bulk delete failed", "error");
    }
}

// ─── PAGINATION ───────────────────────────────────────────────────────────────
function renderPagination(totalPages) {
    pagination.innerHTML = "";
    if (totalPages <= 1) return;
    const prev = document.createElement("button");
    prev.innerText = "Previous";
    prev.disabled = page === 1;
    prev.onclick = () => { page--; updateURL(); renderTable(); window.scrollTo({ top: 0, behavior: "smooth" }); };
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
    next.onclick = () => { page++; updateURL(); renderTable(); window.scrollTo({ top: 0, behavior: "smooth" }); };
    pagination.appendChild(next);
}

function addPage(n) {
    const b = document.createElement("button");
    b.innerText = n;
    if (n === page) b.classList.add("active");
    b.onclick = () => { page = n; updateURL(); renderTable(); window.scrollTo({ top: 0, behavior: "smooth" }); };
    pagination.appendChild(b);
}

// ─── CATEGORY CRUD ────────────────────────────────────────────────────────────
async function renameCategory(id, oldName) {
    const name = prompt("Rename category", oldName);
    if (!name) return;
    const res = await authFetch(`/categories/${id}?name=${encodeURIComponent(name)}`, { method: "PUT" });
    if (res.ok) { showToast("Category renamed successfully", "success"); loadCategories(); loadUploads(); }
    else showToast("Failed to rename category", "error");
}

async function deleteCategory(id) {
    if (!confirm("Delete this category? This cannot be undone.")) return;
    const res = await authFetch(`/categories/${id}`, { method: "DELETE" });
    if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        showToast(err.detail || "Cannot delete category", "error");
    } else {
        showToast("Category deleted successfully", "success");
        loadCategories();
        loadUploads();
    }
}

// ─── SINGLE DELETE ────────────────────────────────────────────────────────────
async function del(uid) {
    if (!confirm("Delete this upload? This action cannot be undone.")) return;
    const res = await authFetch(`/upload/${uid}`, { method: "DELETE" });
    if (res.ok) {
        showToast("File deleted successfully", "success");
        selectedUploadIds.delete(uid);
        loadCategories();
        loadUploads();
    } else {
        showToast("Failed to delete file", "error");
    }
}

// ─── PROFILE PANEL ────────────────────────────────────────────────────────────
async function openProfilePanel() {
    const overlay = document.getElementById("profileOverlay");
    const panel   = document.getElementById("profilePanel");
    overlay.style.display = "block";
    panel.style.display   = "flex";
    requestAnimationFrame(() => { panel.style.transform = "translateX(0)"; });
    resetPasswordForm();
    try {
        const res = await authFetch("/me/profile");
        if (!res || !res.ok) { showToast("Failed to load profile", "error"); return; }
        const d = await res.json();
        const initials = d.email ? d.email[0].toUpperCase() : "?";
        document.getElementById("profileAvatar").textContent = initials;
        document.getElementById("profileEmail").textContent     = d.email;
        document.getElementById("profileEmailInfo").textContent = d.email;
        const roleBadge = document.getElementById("profileRoleBadge");
        if (d.role === "admin") {
            roleBadge.textContent = "🛡 Admin";
            roleBadge.style.cssText += "background:linear-gradient(135deg,#f59e0b,#d97706); color:#fff;";
        } else {
            roleBadge.textContent = "User";
            roleBadge.style.cssText += "background:rgba(99,102,241,0.25); color:#e0e7ff;";
        }
        const statusBadge = document.getElementById("profileStatusBadge");
        if (d.is_active) {
            statusBadge.textContent = "● Active";
            statusBadge.style.cssText += "background:rgba(22,163,74,0.2); color:#86efac;";
        } else {
            statusBadge.textContent = "● Inactive";
            statusBadge.style.cssText += "background:rgba(220,38,38,0.2); color:#fca5a5;";
        }
        document.getElementById("profileUploads").textContent = fmtNum(d.total_uploads);
        document.getElementById("profileRecords").textContent = fmtNum(d.total_records);
        document.getElementById("profileRoleInfo").textContent = d.role === "admin" ? "Administrator" : "User";
        const statusInfo = document.getElementById("profileStatusInfo");
        statusInfo.textContent = d.is_active ? "Active" : "Inactive";
        statusInfo.style.color = d.is_active ? "#16a34a" : "#dc2626";
        document.getElementById("profileSince").textContent = fmtDate(d.created_at);
    } catch (err) {
        console.error(err);
        showToast("Network error loading profile", "error");
    }
}

function closeProfilePanel() {
    const overlay = document.getElementById("profileOverlay");
    const panel   = document.getElementById("profilePanel");
    panel.style.transform = "translateX(100%)";
    setTimeout(() => { overlay.style.display = "none"; panel.style.display = "none"; }, 300);
}

function togglePasswordForm() {
    const form    = document.getElementById("passwordForm");
    const chevron = document.getElementById("pwChevron");
    const isOpen  = form.style.display !== "none";
    form.style.display = isOpen ? "none" : "block";
    chevron.style.transform = isOpen ? "rotate(0deg)" : "rotate(180deg)";
    if (!isOpen) {
        document.getElementById("profileNewPw").value = "";
        document.getElementById("profileConfirmPw").value = "";
        document.getElementById("pwError").style.display = "none";
    }
}

function resetPasswordForm() {
    const form = document.getElementById("passwordForm");
    if (form) form.style.display = "none";
    const chevron = document.getElementById("pwChevron");
    if (chevron) chevron.style.transform = "rotate(0deg)";
    const newPw = document.getElementById("profileNewPw");
    if (newPw) newPw.value = "";
    const confirmPw = document.getElementById("profileConfirmPw");
    if (confirmPw) confirmPw.value = "";
    const pwError = document.getElementById("pwError");
    if (pwError) pwError.style.display = "none";
}

async function savePassword() {
    const newPw     = document.getElementById("profileNewPw").value.trim();
    const confirmPw = document.getElementById("profileConfirmPw").value.trim();
    const errorEl   = document.getElementById("pwError");
    errorEl.style.display = "none";
    if (!newPw) { errorEl.textContent = "Please enter a new password."; errorEl.style.display = "block"; return; }
    if (newPw.length < 6) { errorEl.textContent = "Password must be at least 6 characters."; errorEl.style.display = "block"; return; }
    if (newPw !== confirmPw) { errorEl.textContent = "Passwords do not match."; errorEl.style.display = "block"; return; }
    try {
        const res = await authFetch("/me/change-password", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ new_password: newPw })
        });
        if (!res.ok) {
            const err = await res.json().catch(() => ({}));
            errorEl.textContent = err.detail || "Failed to change password.";
            errorEl.style.display = "block";
            return;
        }
        showToast("Password changed successfully ✓", "success");
        resetPasswordForm();
    } catch (err) {
        errorEl.textContent = "Network error. Please try again.";
        errorEl.style.display = "block";
    }
}

// ─── MISC ─────────────────────────────────────────────────────────────────────
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

function resumePollingForProcessingFiles() {
    const processing = allUploads.filter(r => r.processing_status === 'processing');
    processing.forEach(r => pollUploadStatus(r.upload_id));
}

function fmtNum(n) {
    if (n === null || n === undefined) return "—";
    if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + "M";
    if (n >= 1_000)     return (n / 1_000).toFixed(1) + "k";
    return n.toLocaleString();
}

function fmtDate(dateStr) {
    if (!dateStr) return "—";
    return new Date(dateStr).toLocaleDateString("en-GB", {
        day: "numeric", month: "short", year: "numeric"
    });
}

async function openProfilePanel() {
    const overlay = document.getElementById("profileOverlay");
    const panel   = document.getElementById("profilePanel");

    // Show overlay + panel
    overlay.style.display = "block";
    panel.style.display   = "flex";
    // Trigger slide-in animation
    requestAnimationFrame(() => {
        panel.style.transform = "translateX(0)";
    });

    // Reset password form
    resetPasswordForm();

    try {
        const res = await authFetch("/me/profile");
        if (!res || !res.ok) {
            showToast("Failed to load profile", "error");
            return;
        }
        const d = await res.json();

        // Avatar initials
        const initials = d.email ? d.email[0].toUpperCase() : "?";
        document.getElementById("profileAvatar").textContent = initials;

        // Email
        document.getElementById("profileEmail").textContent     = d.email;
        document.getElementById("profileEmailInfo").textContent = d.email;

        // Role badge
        const roleBadge = document.getElementById("profileRoleBadge");
        if (d.role === "admin") {
            roleBadge.textContent = "🛡 Admin";
            roleBadge.style.cssText += "background:linear-gradient(135deg,#f59e0b,#d97706); color:#fff;";
        } else {
            roleBadge.textContent = "User";
            roleBadge.style.cssText += "background:rgba(99,102,241,0.25); color:#e0e7ff;";
        }

        // Status badge
        const statusBadge = document.getElementById("profileStatusBadge");
        if (d.is_active) {
            statusBadge.textContent = "● Active";
            statusBadge.style.cssText += "background:rgba(22,163,74,0.2); color:#86efac;";
        } else {
            statusBadge.textContent = "● Inactive";
            statusBadge.style.cssText += "background:rgba(220,38,38,0.2); color:#fca5a5;";
        }

        // Stats
        document.getElementById("profileUploads").textContent = fmtNum(d.total_uploads);
        document.getElementById("profileRecords").textContent = fmtNum(d.total_records);

        // Info rows
        document.getElementById("profileRoleInfo").textContent = d.role === "admin" ? "Administrator" : "User";

        const statusInfo = document.getElementById("profileStatusInfo");
        statusInfo.textContent    = d.is_active ? "Active" : "Inactive";
        statusInfo.style.color    = d.is_active ? "#16a34a" : "#dc2626";

        document.getElementById("profileSince").textContent = fmtDate(d.created_at);

    } catch (err) {
        console.error(err);
        showToast("Network error loading profile", "error");
    }
}

function closeProfilePanel() {
    const overlay = document.getElementById("profileOverlay");
    const panel   = document.getElementById("profilePanel");

    panel.style.transform = "translateX(100%)";
    setTimeout(() => {
        overlay.style.display = "none";
        panel.style.display   = "none";
    }, 300);
}

function togglePasswordForm() {
    const form    = document.getElementById("passwordForm");
    const chevron = document.getElementById("pwChevron");
    const isOpen  = form.style.display !== "none";

    form.style.display   = isOpen ? "none" : "block";
    chevron.style.transform = isOpen ? "rotate(0deg)" : "rotate(180deg)";

    if (!isOpen) {
        // Reset fields on open
        document.getElementById("profileNewPw").value     = "";
        document.getElementById("profileConfirmPw").value = "";
        document.getElementById("pwError").style.display  = "none";
    }
}

function resetPasswordForm() {
    const form = document.getElementById("passwordForm");
    if (form) form.style.display = "none";
    const chevron = document.getElementById("pwChevron");
    if (chevron) chevron.style.transform = "rotate(0deg)";
    const newPw = document.getElementById("profileNewPw");
    if (newPw) newPw.value = "";
    const confirmPw = document.getElementById("profileConfirmPw");
    if (confirmPw) confirmPw.value = "";
    const pwError = document.getElementById("pwError");
    if (pwError) pwError.style.display = "none";
}

async function savePassword() {
    const newPw     = document.getElementById("profileNewPw").value.trim();
    const confirmPw = document.getElementById("profileConfirmPw").value.trim();
    const errorEl   = document.getElementById("pwError");

    errorEl.style.display = "none";

    if (!newPw) {
        errorEl.textContent    = "Please enter a new password.";
        errorEl.style.display  = "block";
        return;
    }
    if (newPw.length < 6) {
        errorEl.textContent    = "Password must be at least 6 characters.";
        errorEl.style.display  = "block";
        return;
    }
    if (newPw !== confirmPw) {
        errorEl.textContent    = "Passwords do not match.";
        errorEl.style.display  = "block";
        return;
    }

    try {
        const res = await authFetch("/me/change-password", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ new_password: newPw })
        });

        if (!res.ok) {
            const err = await res.json().catch(() => ({}));
            errorEl.textContent   = err.detail || "Failed to change password.";
            errorEl.style.display = "block";
            return;
        }

        showToast("Password changed successfully ✓", "success");
        resetPasswordForm();

    } catch (err) {
        errorEl.textContent   = "Network error. Please try again.";
        errorEl.style.display = "block";
    }
}

function showUserFilterBanner(userLabel) {
    const uploadsSection = document.querySelector(".uploads-section");
    if (!uploadsSection || document.getElementById("userFilterBanner")) return;

    const banner = document.createElement("div");
    banner.id = "userFilterBanner";
    banner.style.cssText = `
        display:flex; align-items:center; justify-content:space-between;
        padding:10px 16px; margin-bottom:12px;
        background:#eff6ff; border:1px solid #bfdbfe;
        border-radius:8px; font-size:13px; color:#1e40af; font-weight:500;
    `;
    banner.innerHTML = `
        <span>
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="vertical-align:middle; margin-right:6px;">
                <path d="M20 21v-2a4 4 0 00-4-4H8a4 4 0 00-4 4v2"/><circle cx="12" cy="7" r="4"/>
            </svg>
            Showing files for: <strong style="margin-left:4px;">${userLabel}</strong>
        </span>
        <button onclick="clearUserFilter()" style="
            background:none; border:none; cursor:pointer;
            color:#2563eb; font-size:13px; font-weight:600; padding:2px 8px; border-radius:4px;">
            ✕ Clear filter
        </button>
    `;
    uploadsSection.insertBefore(banner, uploadsSection.firstChild);
}

function clearUserFilter() {
    const url = new URL(window.location.href);
    url.searchParams.delete("user_filter");
    url.searchParams.delete("user_email");
    window.history.replaceState({}, "", url);
    document.getElementById("userFilterBanner")?.remove();
    selectedUserId = null;
    document.querySelectorAll(".category").forEach(c => c.classList.remove("active"));

    // ── RESET HEADING ──
    const heading = document.querySelector(".section-header h3");
    if (heading) heading.innerHTML = `<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 19a2 2 0 01-2 2H4a2 2 0 01-2-2V5a2 2 0 012-2h5l2 3h9a2 2 0 012 2z"/></svg> Recent Uploads`;

    loadUploads();
}

// ─── INIT ─────────────────────────────────────────────────────────────────────
async function initPage() {
    await loadUser();
    const dashBtn = document.getElementById("dashboardBtn");
    if (dashBtn) { dashBtn.onclick = () => showPanel("dashboard"); }
    await loadCategories();
    if (currentUser && currentUser.role === "admin") {
        await loadAdminUserList();

        // ── NEW: auto-filter if coming from users.html ──
        if (userFilterId) {
            const userItem = document.getElementById(`user-item-${userFilterId}`);
            if (userItem) {
                // Simulate clicking that user in the sidebar
                selectedUserId = parseInt(userFilterId);
                document.querySelectorAll(".category").forEach(c => c.classList.remove("active"));
                userItem.classList.add("active");
                // Show a banner
                showUserFilterBanner(userFilterEmail || `User #${userFilterId}`);
            }
        }
    }
    await loadUploads(true);
    checkUploadReady();
    resumePollingForProcessingFiles();
}
initPage();