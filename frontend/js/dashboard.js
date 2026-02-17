// dashboard.js — chart logic only.
// token, authFetch, showToast, logout, openAddUserModal,
// closeAddUserModal, createUser are all provided by upload.js
// which loads before this file.

let currentDays = 7;
let charts = {};

// ── PERIOD SWITCH ────────────────────────────────────────────────────────────
function setPeriod(days, btn) {
    currentDays = days;
    document.querySelectorAll(".period-btn").forEach(b => b.classList.remove("active"));
    btn.classList.add("active");
    loadDashboard();
}

// ── FORMAT HELPERS ───────────────────────────────────────────────────────────
function fmt(n) {
    if (n === null || n === undefined) return "—";
    if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + "M";
    if (n >= 1_000)     return (n / 1_000).toFixed(1) + "k";
    return n.toLocaleString();
}

function timeAgo(dateStr) {
    const diff = Date.now() - new Date(dateStr).getTime();
    const mins = Math.floor(diff / 60000);
    if (mins < 1)   return "just now";
    if (mins < 60)  return `${mins}m ago`;
    const hrs = Math.floor(mins / 60);
    if (hrs < 24)   return `${hrs}h ago`;
    const days = Math.floor(hrs / 24);
    return `${days}d ago`;
}

// ── DESTROY CHART ────────────────────────────────────────────────────────────
function destroyChart(key) {
    if (charts[key]) {
        charts[key].destroy();
        delete charts[key];
    }
}

// ── CHART DEFAULTS ───────────────────────────────────────────────────────────
Chart.defaults.font.family = "'DM Sans', sans-serif";
Chart.defaults.color = "#64748b";

// ── STAT CARDS ───────────────────────────────────────────────────────────────
function renderStatCards(d) {
    document.getElementById("stat-total-users").textContent = d.users.total;
    document.getElementById("stat-users-sub").textContent =
        `${d.users.active} active · ${d.users.disabled} disabled`;

    document.getElementById("stat-total-files").textContent = fmt(d.files.total);
    document.getElementById("stat-files-sub").textContent =
        `${d.files.processing} processing · ${d.files.failed} failed`;

    document.getElementById("stat-total-records").textContent = fmt(d.records.total);
    document.getElementById("stat-records-sub").textContent =
        `avg ${fmt(Math.round(d.records.avg_per_file))} per file`;

    const orphans = d.health.orphaned_rows;
    const healthEl  = document.getElementById("stat-health");
    const healthSub = document.getElementById("stat-health-sub");
    const cleanupBtn = document.getElementById("cleanupBtn");

    healthEl.textContent = fmt(orphans);

    if (orphans === 0) {
        healthEl.className = "stat-value stat-health--good";
        healthSub.textContent = "✓ No orphaned rows";
        // Hide button — nothing to clean
        if (cleanupBtn) cleanupBtn.style.display = "none";
    } else if (orphans < 100000) {
        healthEl.className = "stat-value stat-health--warn";
        healthSub.textContent = "orphaned rows detected";
        if (cleanupBtn) cleanupBtn.style.display = "inline-flex";
    } else {
        healthEl.className = "stat-value stat-health--bad";
        healthSub.textContent = "⚠ High orphan count!";
        if (cleanupBtn) cleanupBtn.style.display = "inline-flex";
    }
}

// ── CLEANUP ORPHANED ROWS ────────────────────────────────────────────────────
async function cleanupOrphans() {
    if (!confirm("Delete all orphaned rows from cleaned_data? This cannot be undone.")) return;

    const btn     = document.getElementById("cleanupBtn");
    const btnText = document.getElementById("cleanupBtnText");
    const healthEl  = document.getElementById("stat-health");
    const healthSub = document.getElementById("stat-health-sub");

    // ── Loading state ──
    btn.disabled = true;
    btnText.textContent = "Cleaning...";
    btn.style.opacity = "0.7";
    btn.style.cursor = "not-allowed";
    healthEl.textContent = "...";
    healthSub.textContent = "Deleting orphaned rows";

    try {
        const res = await authFetch("/admin/cleanup-orphaned-rows", { method: "DELETE" });

        if (!res || !res.ok) {
            const err = await res.json().catch(() => ({}));
            showToast(err.detail || "Cleanup failed", "error");
            // Restore previous state
            btnText.textContent = "Clean Up";
            btn.disabled = false;
            btn.style.opacity = "1";
            btn.style.cursor = "pointer";
            loadDashboard();
            return;
        }

        const data = await res.json();

        // ── Success state ──
        healthEl.className = "stat-value stat-health--good";
        healthEl.textContent = "0";
        healthSub.textContent = "✓ No orphaned rows";
        btn.style.display = "none";

        showToast(
            `Cleaned up ${data.deleted_rows.toLocaleString()} orphaned row${data.deleted_rows !== 1 ? "s" : ""} ✓`,
            "success",
            5000
        );

    } catch (e) {
        showToast("Network error during cleanup", "error");
        btnText.textContent = "Clean Up";
        btn.disabled = false;
        btn.style.opacity = "1";
        btn.style.cursor = "pointer";
    }
}

// ── ACTIVITY CHART ───────────────────────────────────────────────────────────
function renderActivityChart(data) {
    destroyChart("activity");
    const ctx = document.getElementById("activityChart").getContext("2d");
    const labels = data.map(d => {
        const dt = new Date(d.date);
        return dt.toLocaleDateString("en-GB", { day: "numeric", month: "short" });
    });

    charts.activity = new Chart(ctx, {
        type: "line",
        data: {
            labels,
            datasets: [
                {
                    label: "Files",
                    data: data.map(d => d.files),
                    borderColor: "#2563eb",
                    backgroundColor: "rgba(37,99,235,0.08)",
                    borderWidth: 2.5,
                    pointRadius: 3,
                    pointHoverRadius: 6,
                    fill: true,
                    tension: 0.4,
                    yAxisID: "y"
                },
                {
                    label: "Records (k)",
                    data: data.map(d => Math.round(d.records / 1000)),
                    borderColor: "#16a34a",
                    backgroundColor: "rgba(22,163,74,0.06)",
                    borderWidth: 2.5,
                    pointRadius: 3,
                    pointHoverRadius: 6,
                    fill: true,
                    tension: 0.4,
                    yAxisID: "y1"
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: "index", intersect: false },
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: "#0f172a",
                    titleColor: "#f1f5f9",
                    bodyColor: "#94a3b8",
                    padding: 12,
                    cornerRadius: 8
                }
            },
            scales: {
                x: {
                    grid: { display: false },
                    ticks: { maxTicksLimit: 10, font: { size: 11 } }
                },
                y: {
                    position: "left",
                    grid: { color: "#f1f5f9" },
                    ticks: { stepSize: 1, font: { size: 11 } },
                    title: { display: true, text: "Files", font: { size: 11 } }
                },
                y1: {
                    position: "right",
                    grid: { display: false },
                    ticks: { font: { size: 11 } },
                    title: { display: true, text: "Records (k)", font: { size: 11 } }
                }
            }
        }
    });
}

// ── TOP USERS CHARTS ─────────────────────────────────────────────────────────
function renderTopUsersCharts(data) {
    destroyChart("topFiles");
    destroyChart("topRecords");

    const sorted_files   = [...data].sort((a, b) => b.files - a.files).slice(0, 8);
    const sorted_records = [...data].sort((a, b) => b.records - a.records).slice(0, 8);

    const barOpts = (labelText) => ({
        responsive: true,
        maintainAspectRatio: false,
        indexAxis: "y",
        plugins: {
            legend: { display: false },
            tooltip: {
                backgroundColor: "#0f172a",
                titleColor: "#f1f5f9",
                bodyColor: "#94a3b8",
                padding: 10,
                cornerRadius: 8
            }
        },
        scales: {
            x: {
                grid: { color: "#f8fafc" },
                ticks: { font: { size: 11 } },
                title: { display: true, text: labelText, font: { size: 11 } }
            },
            y: {
                grid: { display: false },
                ticks: {
                    font: { size: 11 },
                    callback: function(val) {
                        const label = this.getLabelForValue(val);
                        return label.length > 18 ? label.slice(0, 17) + "…" : label;
                    }
                }
            }
        }
    });

    charts.topFiles = new Chart(
        document.getElementById("topUsersFilesChart").getContext("2d"),
        {
            type: "bar",
            data: {
                labels: sorted_files.map(u => u.email),
                datasets: [{
                    data: sorted_files.map(u => u.files),
                    backgroundColor: "rgba(37,99,235,0.75)",
                    borderRadius: 5,
                    borderSkipped: false
                }]
            },
            options: barOpts("Files uploaded")
        }
    );

    charts.topRecords = new Chart(
        document.getElementById("topUsersRecordsChart").getContext("2d"),
        {
            type: "bar",
            data: {
                labels: sorted_records.map(u => u.email),
                datasets: [{
                    data: sorted_records.map(u => u.records),
                    backgroundColor: "rgba(124,58,237,0.75)",
                    borderRadius: 5,
                    borderSkipped: false
                }]
            },
            options: barOpts("Total records")
        }
    );
}

// ── DUPLICATE RATE CHART ─────────────────────────────────────────────────────
function renderDupRateChart(data) {
    destroyChart("dupRate");
    const sorted = [...data]
        .filter(u => u.files > 0)
        .sort((a, b) => b.avg_dup_rate - a.avg_dup_rate)
        .slice(0, 8);

    charts.dupRate = new Chart(
        document.getElementById("dupRateChart").getContext("2d"),
        {
            type: "bar",
            data: {
                labels: sorted.map(u => u.email),
                datasets: [{
                    data: sorted.map(u => parseFloat(u.avg_dup_rate.toFixed(1))),
                    backgroundColor: sorted.map(u =>
                        u.avg_dup_rate > 20 ? "rgba(220,38,38,0.7)" :
                        u.avg_dup_rate > 5  ? "rgba(234,88,12,0.7)" :
                                              "rgba(22,163,74,0.7)"
                    ),
                    borderRadius: 5,
                    borderSkipped: false
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                indexAxis: "y",
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        backgroundColor: "#0f172a",
                        titleColor: "#f1f5f9",
                        bodyColor: "#94a3b8",
                        padding: 10,
                        cornerRadius: 8,
                        callbacks: { label: ctx => ` ${ctx.raw}% avg duplicates` }
                    }
                },
                scales: {
                    x: {
                        grid: { color: "#f8fafc" },
                        ticks: { font: { size: 11 }, callback: v => v + "%" },
                        title: { display: true, text: "Avg duplicate %", font: { size: 11 } }
                    },
                    y: {
                        grid: { display: false },
                        ticks: {
                            font: { size: 11 },
                            callback: function(val) {
                                const label = this.getLabelForValue(val);
                                return label.length > 18 ? label.slice(0, 17) + "…" : label;
                            }
                        }
                    }
                }
            }
        }
    );
}

// ── FILE TYPE CHART ──────────────────────────────────────────────────────────
function renderFileTypeChart(data) {
    destroyChart("fileType");
    const labels = data.map(d => d.ext.toUpperCase());
    const values = data.map(d => d.count);

    charts.fileType = new Chart(
        document.getElementById("fileTypeChart").getContext("2d"),
        {
            type: "doughnut",
            data: {
                labels,
                datasets: [{
                    data: values,
                    backgroundColor: ["#2563eb", "#16a34a", "#7c3aed", "#ea580c"],
                    borderWidth: 3,
                    borderColor: "#ffffff",
                    hoverOffset: 6
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                cutout: "65%",
                plugins: {
                    legend: {
                        position: "bottom",
                        labels: { padding: 14, font: { size: 12 }, usePointStyle: true }
                    },
                    tooltip: {
                        backgroundColor: "#0f172a",
                        titleColor: "#f1f5f9",
                        bodyColor: "#94a3b8",
                        padding: 10,
                        cornerRadius: 8,
                        callbacks: { label: ctx => ` ${ctx.label}: ${ctx.raw} files` }
                    }
                }
            }
        }
    );
}

// ── PROCESSING STATUS CHART ──────────────────────────────────────────────────
function renderStatusChart(data) {
    destroyChart("status");
    const colorMap = {
        ready:      "#16a34a",
        processing: "#f59e0b",
        failed:     "#dc2626"
    };
    const labels = data.map(d => d.status.charAt(0).toUpperCase() + d.status.slice(1));
    const values = data.map(d => d.count);
    const colors = data.map(d => colorMap[d.status] || "#94a3b8");

    charts.status = new Chart(
        document.getElementById("statusChart").getContext("2d"),
        {
            type: "doughnut",
            data: {
                labels,
                datasets: [{
                    data: values,
                    backgroundColor: colors,
                    borderWidth: 3,
                    borderColor: "#ffffff",
                    hoverOffset: 6
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                cutout: "65%",
                plugins: {
                    legend: {
                        position: "bottom",
                        labels: { padding: 14, font: { size: 12 }, usePointStyle: true }
                    },
                    tooltip: {
                        backgroundColor: "#0f172a",
                        titleColor: "#f1f5f9",
                        bodyColor: "#94a3b8",
                        padding: 10,
                        cornerRadius: 8,
                        callbacks: { label: ctx => ` ${ctx.label}: ${ctx.raw} files` }
                    }
                }
            }
        }
    );
}

// ── ACTIVITY FEED ────────────────────────────────────────────────────────────
function renderFeed(data) {
    const tbody = document.getElementById("feedBody");
    if (!data || data.length === 0) {
        tbody.innerHTML = `<tr><td colspan="7" class="feed-loading">No uploads yet</td></tr>`;
        return;
    }
    tbody.innerHTML = data.map(r => {
        const statusBadge =
            r.processing_status === "processing"
                ? `<span class="badge badge--processing">Processing</span>`
            : r.processing_status === "failed"
                ? `<span class="badge badge--failed">Failed</span>`
                : `<span class="badge badge--ready">Ready</span>`;

        const dupPct = r.total_records > 0
            ? ((r.duplicate_records / r.total_records) * 100).toFixed(1) + "%"
            : "—";

        return `<tr>
            <td><span class="feed-filename" title="${r.filename}">📄 ${r.filename}</span></td>
            <td><span class="feed-user" title="${r.uploaded_by}">${r.uploaded_by}</span></td>
            <td style="color:var(--text-secondary);font-size:12px;">${r.category || "—"}</td>
            <td><span class="feed-num">${fmt(r.total_records)}</span></td>
            <td><span class="feed-num">${dupPct}</span></td>
            <td>${statusBadge}</td>
            <td><span class="feed-time">${timeAgo(r.uploaded_at)}</span></td>
        </tr>`;
    }).join("");
}

// ── MAIN LOAD — called by showPanel() in upload.js ───────────────────────────
async function loadDashboard() {
    try {
        const res = await authFetch(`/admin/dashboard-stats?days=${currentDays}`);
        if (!res || !res.ok) {
            showToast("Failed to load dashboard data");
            return;
        }
        const d = await res.json();

        renderStatCards(d);
        renderActivityChart(d.activity);
        renderTopUsersCharts(d.users.breakdown);
        renderDupRateChart(d.users.breakdown);
        renderFileTypeChart(d.file_types);
        renderStatusChart(d.processing_status);
        renderFeed(d.recent_activity);

    } catch (err) {
        console.error(err);
        showToast("Network error loading dashboard");
    }
}