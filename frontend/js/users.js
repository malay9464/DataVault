const token = localStorage.getItem("access_token");
if (!token) {
    location.href = "/static/login.html";
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
