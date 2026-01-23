async function login() {
    const email = document.getElementById("email").value.trim();
    const password = document.getElementById("password").value.trim();

    if (!email || !password) {
        alert("Email and password are required");
        return;
    }

    const form = new URLSearchParams();
    form.append("username", email); // OAuth2 expects "username"
    form.append("password", password);

    const res = await fetch("/login", {
        method: "POST",
        headers: {
            "Content-Type": "application/x-www-form-urlencoded"
        },
        body: form.toString()
    });

    if (!res.ok) {
        alert("Invalid email or password");
        return;
    }

    const data = await res.json();
    localStorage.setItem("access_token", data.access_token);
    window.location.href = "/";
}

document.getElementById("loginForm").addEventListener("submit", function (e) {
    e.preventDefault(); // stop page reload
    login();
});
