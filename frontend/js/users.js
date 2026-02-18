const token = localStorage.getItem("access_token");
if (!token) location.href = "/static/login.html";

let allUsers = [];
let currentResetUserId = null;

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

function logout() {
    localStorage.removeItem("access_token");
    location.href = "/static/login.html";
}

// ── TOAST ────────────────────────────────────────────────────────────────────
function showToast(message, type = "success") {
    const container = document.getElementById("toastContainer");
    const toast = document.createElement("div");
    toast.className = `toast toast--${type}`;
    const icons = {
        success: `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M20 6L9 17l-5-5"/></svg>`,
        error:   `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>`,
        info:    `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg>`,
    };
    toast.innerHTML = `${icons[type] || icons.info}<span>${message}</span>`;
    container.appendChild(toast);
    setTimeout(() => toast.classList.add("toast--visible"), 10);
    setTimeout(() => {
        toast.classList.remove("toast--visible");
        setTimeout(() => toast.remove(), 300);
    }, 3500);
}

// ── LOAD USERS ───────────────────────────────────────────────────────────────
async function loadUsers() {
    const res = await authFetch("/admin/users-with-stats");
    if (!res || !res.ok) {
        showToast("Access denied or failed to load users", "error");
        location.href = "/";
        return;
    }
    allUsers = await res.json();
    updateStats();
    renderUsers(allUsers);
}

function updateStats() {
    document.getElementById("statTotal").textContent    = allUsers.length;
    document.getElementById("statActive").textContent   = allUsers.filter(u => u.is_active).length;
    document.getElementById("statDisabled").textContent = allUsers.filter(u => !u.is_active).length;
    document.getElementById("statAdmins").textContent   = allUsers.filter(u => u.role === "admin").length;
}

// ── FILTER ───────────────────────────────────────────────────────────────────
function filterUsers() {
    const q      = document.getElementById("searchInput").value.toLowerCase();
    const role   = document.getElementById("roleFilter").value;
    const status = document.getElementById("statusFilter").value;

    const filtered = allUsers.filter(u => {
        const matchEmail  = u.email.toLowerCase().includes(q);
        const matchRole   = !role   || u.role === role;
        const matchStatus = !status || (status === "active" ? u.is_active : !u.is_active);
        return matchEmail && matchRole && matchStatus;
    });

    renderUsers(filtered);
}

// ── RENDER ───────────────────────────────────────────────────────────────────
function renderUsers(users) {
    const tbody      = document.getElementById("usersTable");
    const emptyState = document.getElementById("emptyState");
    tbody.innerHTML  = "";

    if (users.length === 0) {
        emptyState.style.display = "flex";
        return;
    }
    emptyState.style.display = "none";

    // Detect logged-in user id from JWT (simple decode — no verify needed on client)
    let myId = null;
    try {
        myId = JSON.parse(atob(token.split(".")[1])).user_id;
    } catch {}

    users.forEach(u => {
        const isSelf    = u.id === myId;
        const isActive  = u.is_active;
        const initials  = u.email.charAt(0).toUpperCase();

        const row = document.createElement("tr");
        if (!isActive) row.classList.add("row--disabled");

        row.innerHTML = `
            <td>
                <div class="user-cell">
                    <div class="avatar ${u.role === 'admin' ? 'avatar--admin' : ''}">${initials}</div>
                    <div class="user-info">
                        <span class="user-email">${u.email}</span>
                        ${isSelf ? '<span class="badge badge--self">You</span>' : ''}
                    </div>
                </div>
            </td>
            <td>
                <span class="badge ${u.role === 'admin' ? 'badge--admin' : 'badge--user'}">
                    ${u.role === 'admin' ? '🛡 Admin' : '👤 User'}
                </span>
            </td>
            <td>
                <button
                    class="toggle-btn ${isActive ? 'toggle-btn--active' : 'toggle-btn--inactive'}"
                    onclick="toggleUserStatus(${u.id}, ${isActive})"
                    ${isSelf ? 'disabled title="Cannot disable your own account"' : ''}
                >
                    <span class="toggle-dot"></span>
                    <span>${isActive ? 'Active' : 'Disabled'}</span>
                </button>
            </td>
            <td>
                <a
                    href="/?user_filter=${u.id}&user_email=${encodeURIComponent(u.email)}"
                    class="upload-count-link"
                    title="View files"
                >
                    <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <path d="M22 19a2 2 0 01-2 2H4a2 2 0 01-2-2V5a2 2 0 012-2h5l2 3h9a2 2 0 012 2z"/>
                    </svg>
                    ${u.upload_count ?? 0} files
                </a>
            </td>
            <td>
                <div class="action-group">
                    <button class="action-btn action-btn--reset"
                        onclick="openResetPwModal(${u.id}, '${u.email.replace(/'/g, "\\'")}')">
                        <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <rect x="3" y="11" width="18" height="11" rx="2" ry="2"/>
                            <path d="M7 11V7a5 5 0 0110 0v4"/>
                        </svg>
                        Reset Password
                    </button>
                    <button class="action-btn action-btn--delete"
                        onclick="openDeleteUserModal(${u.id}, '${u.email.replace(/'/g, "\\'")}')"
                        ${isSelf ? 'disabled title="Cannot delete your own account"' : ''}
                    >
                        <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <polyline points="3 6 5 6 21 6"/>
                            <path d="M19 6l-1 14a2 2 0 01-2 2H8a2 2 0 01-2-2L5 6"/>
                            <path d="M9 6V4h6v2"/>
                        </svg>
                        Delete
                    </button>
                </div>
            </td>
        `;
        tbody.appendChild(row);
    });
}

// ── TOGGLE STATUS ─────────────────────────────────────────────────────────────
async function toggleUserStatus(userId, currentlyActive) {
    const action = currentlyActive ? "disable" : "enable";
    const res = await authFetch(
        `/admin/users/${userId}/toggle-status`,
        { method: "PATCH" }
    );
    if (!res || !res.ok) {
        const err = await res?.json().catch(() => ({}));
        showToast(err.detail || `Failed to ${action} user`, "error");
        return;
    }
    showToast(`User ${action}d successfully`, "success");
    loadUsers();
}

// ── RESET PASSWORD MODAL ──────────────────────────────────────────────────────
function openResetPwModal(userId, email) {
    currentResetUserId = userId;
    document.getElementById("resetPwEmail").textContent = email;
    document.getElementById("newPwInput").value = "";
    document.getElementById("pwStrengthBar").style.display   = "none";
    document.getElementById("pwStrengthLabel").style.display = "none";
    document.getElementById("resetPwModal").style.display    = "flex";

    document.getElementById("newPwInput").addEventListener("input", checkPwStrength);
}

function closeResetPwModal() {
    document.getElementById("resetPwModal").style.display = "none";
    currentResetUserId = null;
}

function togglePwVisibility() {
    const input = document.getElementById("newPwInput");
    const icon  = document.getElementById("eyeIcon");
    if (input.type === "password") {
        input.type = "text";
        icon.innerHTML = `<path d="M17.94 17.94A10.07 10.07 0 0112 20c-7 0-11-8-11-8a18.45 18.45 0 015.06-5.94M9.9 4.24A9.12 9.12 0 0112 4c7 0 11 8 11 8a18.5 18.5 0 01-2.16 3.19m-6.72-1.07a3 3 0 11-4.24-4.24"/><line x1="1" y1="1" x2="23" y2="23"/>`;
    } else {
        input.type = "password";
        icon.innerHTML = `<path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/>`;
    }
}

function checkPwStrength() {
    const pw  = document.getElementById("newPwInput").value;
    const bar = document.getElementById("pwStrengthBar");
    const fill = document.getElementById("pwStrengthFill");
    const label = document.getElementById("pwStrengthLabel");

    if (!pw) { bar.style.display = "none"; label.style.display = "none"; return; }
    bar.style.display   = "block";
    label.style.display = "block";

    let score = 0;
    if (pw.length >= 8)          score++;
    if (/[A-Z]/.test(pw))        score++;
    if (/[0-9]/.test(pw))        score++;
    if (/[^A-Za-z0-9]/.test(pw)) score++;

    const levels = [
        { pct: "25%", color: "#ef4444", text: "Weak" },
        { pct: "50%", color: "#f97316", text: "Fair" },
        { pct: "75%", color: "#eab308", text: "Good" },
        { pct: "100%",color: "#22c55e", text: "Strong" },
    ];
    const l = levels[score - 1] || levels[0];
    fill.style.width      = l.pct;
    fill.style.background = l.color;
    label.textContent     = l.text;
    label.style.color     = l.color;
}

async function submitResetPassword() {
    const pwd = document.getElementById("newPwInput").value.trim();
    if (!pwd || pwd.length < 6) {
        showToast("Password must be at least 6 characters", "error");
        return;
    }
    const res = await authFetch(
        `/admin/users/${currentResetUserId}/reset-password?new_password=${encodeURIComponent(pwd)}`,
        { method: "POST" }
    );
    closeResetPwModal();
    if (!res || !res.ok) {
        showToast("Failed to reset password", "error");
        return;
    }
    showToast("Password reset successfully", "success");
}

// ── DELETE MODAL ──────────────────────────────────────────────────────────────
function openDeleteUserModal(userId, email) {
    document.getElementById("deleteUserEmail").textContent = email;
    const modal = document.getElementById("deleteUserModal");
    modal.dataset.userId = String(userId);
    modal.style.display  = "flex";
}

function closeDeleteUserModal() {
    const modal = document.getElementById("deleteUserModal");
    modal.style.display  = "none";
    modal.dataset.userId = "";
}

async function confirmDeleteUser(policy) {
    const modal  = document.getElementById("deleteUserModal");
    const userId = modal.dataset.userId;
    if (!userId) return;

    const res = await authFetch(
        `/admin/users/${userId}?policy=${policy}`,
        { method: "DELETE" }
    );
    closeDeleteUserModal();

    if (!res || !res.ok) {
        const err = await res?.json().catch(() => ({}));
        showToast(err.detail || "Failed to delete user", "error");
        return;
    }

    showToast(
        policy === "delete_all"
            ? "User and all their data deleted."
            : "User deleted. Data transferred to admin.",
        "success"
    );
    loadUsers();
}

// Close modals on overlay click
document.getElementById("deleteUserModal").addEventListener("click", function(e) {
    if (e.target === this) closeDeleteUserModal();
});
document.getElementById("resetPwModal").addEventListener("click", function(e) {
    if (e.target === this) closeResetPwModal();
});

loadUsers();