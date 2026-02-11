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
    const res = await authFetch("/admin/users");

    if (!res.ok) {
        alert("Access denied");
        location.href = "/";
        return;
    }

    const users = await res.json();
    const tbody = document.getElementById("usersTable");
    tbody.innerHTML = "";

    users.forEach(u => {
        tbody.innerHTML += `
            <tr>
                <td>${u.email}</td>
                <td>${u.role}</td>
                <td class="status-active">
                    ${u.is_active ? "Active" : "Disabled"}
                </td>
                <td>
                    <button class="btn btn-reset"
                        onclick="resetPassword(${u.id}, '${u.email}')">
                        Reset Password
                    </button>
                    <button class="btn btn-delete"
                        onclick="openDeleteUserModal(${u.id}, '${u.email}')">
                        Delete
                    </button>
                </td>
            </tr>
        `;
    });
}

async function resetPassword(userId, email) {
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

loadUsers();

function openDeleteUserModal(userId, email) {
    document.getElementById("deleteUserEmail").textContent = email;
    document.getElementById("deleteUserModal").dataset.userId = userId;
    document.getElementById("deleteUserModal").style.display = "flex";
}

function closeDeleteUserModal() {
    document.getElementById("deleteUserModal").style.display = "none";
    document.getElementById("deleteUserModal").dataset.userId = "";
}

async function confirmDeleteUser(policy) {
    const modal = document.getElementById("deleteUserModal");
    const userId = modal.dataset.userId;

    if (!userId) return;

    const res = await authFetch(
        `/admin/users/${userId}?policy=${policy}`,
        { method: "DELETE" }
    );

    closeDeleteUserModal();

    if (!res.ok) {
        const err = await res.json();
        alert(err.detail || "Failed to delete user");
        return;
    }

    const msg = policy === "delete_all"
        ? "User and all their data deleted."
        : "User deleted. Data transferred to admin.";

    alert(msg);
    loadUsers();
}