// Password visibility toggle
function togglePasswordVisibility() {
    const passwordInput = document.getElementById("password");
    const eyeIcon = document.getElementById("eyeIcon");
    
    if (passwordInput.type === "password") {
        passwordInput.type = "text";
        // Change to eye-off icon
        eyeIcon.innerHTML = `
            <path d="M17.94 17.94A10.07 10.07 0 0112 20c-7 0-11-8-11-8a18.45 18.45 0 015.06-5.94M9.9 4.24A9.12 9.12 0 0112 4c7 0 11 8 11 8a18.5 18.5 0 01-2.16 3.19m-6.72-1.07a3 3 0 11-4.24-4.24"/>
            <line x1="1" y1="1" x2="23" y2="23"/>
        `;
    } else {
        passwordInput.type = "password";
        // Change back to eye icon
        eyeIcon.innerHTML = `
            <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/>
            <circle cx="12" cy="12" r="3"/>
        `;
    }
}

// Show error message
function showError(message) {
    const errorMsg = document.getElementById("errorMsg");
    errorMsg.textContent = message;
    errorMsg.style.display = "flex";
    
    // Auto-hide after 5 seconds
    setTimeout(() => {
        errorMsg.style.display = "none";
    }, 5000);
}

// Hide error message
function hideError() {
    const errorMsg = document.getElementById("errorMsg");
    errorMsg.style.display = "none";
}

// Login function with loading state
async function login() {
    const email = document.getElementById("email").value.trim();
    const password = document.getElementById("password").value.trim();
    const loginBtn = document.getElementById("loginBtn");
    const btnText = document.getElementById("btnText");
    const btnSpinner = document.getElementById("btnSpinner");

    // Clear previous errors
    hideError();

    // Validation
    if (!email || !password) {
        showError("Please enter both email and password");
        return;
    }

    // Email format validation
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(email)) {
        showError("Please enter a valid email address");
        return;
    }

    // Show loading state
    loginBtn.disabled = true;
    btnText.style.display = "none";
    btnSpinner.style.display = "inline-block";

    try {
        const form = new URLSearchParams();
        form.append("username", email);
        form.append("password", password);

        const res = await fetch("/login", {
            method: "POST",
            headers: { "Content-Type": "application/x-www-form-urlencoded" },
            body: form.toString()
        });

        if (!res.ok) {
            const errorData = await res.json().catch(() => ({}));
            showError(errorData.detail || "Invalid email or password");
            
            // Reset button state
            loginBtn.disabled = false;
            btnText.style.display = "inline";
            btnSpinner.style.display = "none";
            return;
        }

        const data = await res.json();
        localStorage.setItem("access_token", data.access_token);

        // Decode JWT payload to get role
        const payload = JSON.parse(atob(data.access_token.split(".")[1]));

        // Show success briefly before redirect
        btnText.textContent = "Success!";
        btnText.style.display = "inline";
        btnSpinner.style.display = "none";

        // Small delay for visual feedback
        setTimeout(() => {
            if (payload.role === "admin") {
                window.location.href = "/?view=dashboard";
            } else {
                window.location.href = "/";
            }
        }, 500);

    } catch (error) {
        console.error("Login error:", error);
        showError("Network error. Please check your connection and try again.");
        
        // Reset button state
        loginBtn.disabled = false;
        btnText.style.display = "inline";
        btnSpinner.style.display = "none";
    }
}

// Form submission handler
document.getElementById("loginForm").addEventListener("submit", function (e) {
    e.preventDefault();
    login();
});

// Enter key should submit form (already handled by form submit, but explicit for clarity)
document.getElementById("email").addEventListener("keypress", function(e) {
    if (e.key === "Enter") {
        e.preventDefault();
        login();
    }
});

document.getElementById("password").addEventListener("keypress", function(e) {
    if (e.key === "Enter") {
        e.preventDefault();
        login();
    }
});

// Clear error on input change
document.getElementById("email").addEventListener("input", hideError);
document.getElementById("password").addEventListener("input", hideError);

// Auto-focus email field on page load
window.addEventListener("load", function() {
    document.getElementById("email").focus();
});