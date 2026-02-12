const token = localStorage.getItem("access_token");
if (!token) {
    location.href = "/static/login.html";
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

function logout() {
    localStorage.removeItem("access_token");
    location.href = "/static/login.html";
}

async function loadUsers() {
    const res = await authFetch("/users");

    if (!res || !res.ok) {
        alert("Access denied");
        location.href = "/";
        return;
    }

    const users = await res.json();
    const tbody = document.getElementById("usersTable");
    tbody.innerHTML = "";

    if (users.length === 0) {
        tbody.innerHTML = `<tr><td colspan="4" style="text-align:center; color:#64748b; padding:24px;">No users found</td></tr>`;
        return;
    }

    users.forEach(u => {
        const row = document.createElement("tr");
        row.innerHTML = `
            <td>${u.email}</td>
            <td>${u.role}</td>
            <td class="${u.is_active ? 'status-active' : 'status-inactive'}">
                ${u.is_active ? "Active" : "Disabled"}
            </td>
            <td style="display:flex; gap:8px;">
                <button class="btn btn-reset"
                    onclick="resetPassword(${u.id}, '${u.email}')">
                    Reset Password
                </button>
                <button class="btn btn-delete"
                    onclick="openDeleteUserModal(${u.id}, '${u.email}')">
                    Delete
                </button>
            </td>
        `;
        tbody.appendChild(row);
    });
}

async function resetPassword(userId, email) {
    const pwd = prompt(`Enter new password for ${email}`);
    if (!pwd) return;

    const res = await authFetch(
        `/admin/users/${userId}/reset-password?new_password=${encodeURIComponent(pwd)}`,
        { method: "POST" }
    );

    if (!res || !res.ok) {
        alert("Failed to reset password");
        return;
    }

    alert("Password reset successfully");
}

function openDeleteUserModal(userId, email) {
    const modal = document.getElementById("deleteUserModal");
    const emailEl = document.getElementById("deleteUserEmail");

    if (!modal || !emailEl) {
        alert("Modal not found in DOM");
        return;
    }

    emailEl.textContent = email;
    modal.dataset.userId = String(userId);
    modal.style.display = "flex";
}

function closeDeleteUserModal() {
    const modal = document.getElementById("deleteUserModal");
    if (!modal) return;
    modal.style.display = "none";
    modal.dataset.userId = "";
}

async function confirmDeleteUser(policy) {
    const modal = document.getElementById("deleteUserModal");
    const userId = modal.dataset.userId;

    if (!userId) {
        alert("No user selected");
        return;
    }

    const res = await authFetch(
        `/admin/users/${userId}?policy=${policy}`,
        { method: "DELETE" }
    );

    closeDeleteUserModal();

    if (!res || !res.ok) {
        const err = await res.json().catch(() => ({}));
        alert(err.detail || "Failed to delete user");
        return;
    }

    const msg = policy === "delete_all"
        ? "User and all their data deleted."
        : "User deleted. Data transferred to admin.";

    alert(msg);
    loadUsers();
}

// Start
loadUsers();