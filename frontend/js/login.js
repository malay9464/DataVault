async function login() {
    const email = document.getElementById("email").value.trim();
    const password = document.getElementById("password").value.trim();

    if (!email || !password) {
        alert("Email and password are required");
        return;
    }

    const form = new URLSearchParams();
    form.append("username", email);
    form.append("password", password);

    const res = await fetch("/login", {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: form.toString()
    });

    if (!res.ok) {
        alert("Invalid email or password");
        return;
    }

    const data = await res.json();
    localStorage.setItem("access_token", data.access_token);

    // ── Role-based redirect ──────────────────────────────
    // Decode JWT payload (no library needed)
    const payload = JSON.parse(atob(data.access_token.split(".")[1]));

    if (payload.role === "admin") {
        window.location.href = "/?view=dashboard";   // ← Admin → dashboard panel
    } else {
        window.location.href = "/";                   // ← User → upload page
    }
}

document.getElementById("loginForm").addEventListener("submit", function (e) {
    e.preventDefault();
    login();
});