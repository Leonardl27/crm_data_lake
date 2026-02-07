/**
 * Life Insurance Data Lake Dashboard
 * Loads and visualizes data from the PROD layer
 */

const COLORS = {
    primary: ['#1e3a5f', '#2c5282', '#3182ce', '#4299e1', '#63b3ed'],
    accent: ['#38a169', '#d69e2e', '#e53e3e', '#805ad5', '#ed64a6'],
    status: {
        'Active': '#38a169',
        'Approved': '#38a169',
        'Paid': '#2f855a',
        'Pending': '#d69e2e',
        'In Review': '#dd6b20',
        'Declined': '#e53e3e',
        'Denied': '#e53e3e',
        'Lapsed': '#a0aec0',
        'Closed': '#718096'
    },
    products: {
        'Term Life': '#3182ce',
        'Whole Life': '#2c5282',
        'Universal Life': '#38a169',
        'Variable Life': '#805ad5',
        'Final Expense': '#d69e2e'
    }
};

Chart.defaults.font.family = "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif";
Chart.defaults.plugins.legend.position = 'bottom';
Chart.defaults.plugins.legend.labels.usePointStyle = true;

/**
 * Fetch dashboard data from JSON file
 */
async function fetchDashboardData() {
    try {
        const response = await fetch('assets/data/dashboard_data.json');
        if (!response.ok) {
            throw new Error('Data not yet available. Run the pipeline first.');
        }
        return await response.json();
    } catch (error) {
        console.error('Error fetching data:', error);
        showNoDataMessage();
        return null;
    }
}

/**
 * Show message when no data is available
 */
function showNoDataMessage() {
    const statsSection = document.querySelector('.stats-section');
    if (statsSection) {
        const notice = document.createElement('div');
        notice.className = 'notice';
        notice.innerHTML = `
            <p><strong>No data available yet.</strong><br>
            Run the pipeline with: <code>python pipelines/run_pipeline.py</code></p>
        `;
        statsSection.insertBefore(notice, statsSection.querySelector('.stats-grid'));
    }
}

/**
 * Update statistics cards
 */
function updateStats(data) {
    const summary = data.summary || {};

    document.getElementById('total-customers').textContent =
        (summary.total_customers || 0).toLocaleString();
    document.getElementById('total-quotes').textContent =
        (summary.total_quotes || 0).toLocaleString();
    document.getElementById('total-applications').textContent =
        (summary.total_applications || 0).toLocaleString();
    document.getElementById('total-policies').textContent =
        (summary.total_policies || 0).toLocaleString();
    document.getElementById('total-claims').textContent =
        (summary.total_claims || 0).toLocaleString();

    if (data.applications?.approval_rate) {
        document.getElementById('approval-rate').textContent =
            data.applications.approval_rate + '%';
    }

    // Update last updated timestamp
    const lastUpdated = document.getElementById('last-updated');
    if (data.generated_at) {
        const date = new Date(data.generated_at);
        lastUpdated.textContent = `Last updated: ${date.toLocaleString()}`;
    }
}

/**
 * Update conversion funnel
 */
function updateFunnel(data) {
    const funnel = data.funnel || {};

    // Update counts
    const quotesEl = document.querySelector('#funnel-quotes .funnel-count');
    const appsEl = document.querySelector('#funnel-applications .funnel-count');
    const policiesEl = document.querySelector('#funnel-policies .funnel-count');
    const claimsEl = document.querySelector('#funnel-claims .funnel-count');

    if (quotesEl) quotesEl.textContent = (funnel.quotes || 0).toLocaleString();
    if (appsEl) appsEl.textContent = (funnel.applications || 0).toLocaleString();
    if (policiesEl) policiesEl.textContent = (funnel.policies || 0).toLocaleString();
    if (claimsEl) claimsEl.textContent = (funnel.claims || 0).toLocaleString();

    // Update conversion rates
    const rates = funnel.conversion_rates || {};
    document.getElementById('conv-quote-app').textContent =
        (rates.quote_to_application || 0) + '%';
    document.getElementById('conv-app-policy').textContent =
        (rates.application_to_policy || 0) + '%';
    document.getElementById('conv-policy-claim').textContent =
        (rates.policy_to_claim || 0) + '%';
}

/**
 * Create product type chart
 */
function createProductTypeChart(data) {
    const ctx = document.getElementById('productTypeChart');
    if (!ctx || !data.quotes?.by_product_type) return;

    const byType = data.quotes.by_product_type;
    const labels = Object.keys(byType);
    const values = Object.values(byType);
    const colors = labels.map(l => COLORS.products[l] || COLORS.primary[0]);

    new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: labels,
            datasets: [{
                data: values,
                backgroundColor: colors,
                borderWidth: 2,
                borderColor: '#fff'
            }]
        },
        options: {
            responsive: true,
            plugins: {
                legend: { position: 'right' }
            }
        }
    });
}

/**
 * Create quote status chart
 */
function createQuoteStatusChart(data) {
    const ctx = document.getElementById('quoteStatusChart');
    if (!ctx || !data.quotes?.by_status) return;

    const byStatus = data.quotes.by_status;
    const labels = Object.keys(byStatus);
    const values = Object.values(byStatus);

    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Quotes',
                data: values,
                backgroundColor: COLORS.primary[2],
                borderRadius: 4
            }]
        },
        options: {
            responsive: true,
            plugins: { legend: { display: false } },
            scales: { y: { beginAtZero: true } }
        }
    });
}

/**
 * Create quote source chart
 */
function createQuoteSourceChart(data) {
    const ctx = document.getElementById('quoteSourceChart');
    if (!ctx || !data.quotes?.by_source) return;

    const bySource = data.quotes.by_source;
    const labels = Object.keys(bySource);
    const values = Object.values(bySource);

    new Chart(ctx, {
        type: 'pie',
        data: {
            labels: labels,
            datasets: [{
                data: values,
                backgroundColor: COLORS.accent,
                borderWidth: 2,
                borderColor: '#fff'
            }]
        },
        options: { responsive: true }
    });
}

/**
 * Create underwriting status chart
 */
function createUnderwritingChart(data) {
    const ctx = document.getElementById('underwritingChart');
    if (!ctx || !data.applications?.by_underwriting_status) return;

    const byStatus = data.applications.by_underwriting_status;
    const labels = Object.keys(byStatus);
    const values = Object.values(byStatus);
    const colors = labels.map(l => COLORS.status[l] || COLORS.primary[0]);

    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Applications',
                data: values,
                backgroundColor: colors,
                borderRadius: 4
            }]
        },
        options: {
            responsive: true,
            indexAxis: 'y',
            plugins: { legend: { display: false } },
            scales: { x: { beginAtZero: true } }
        }
    });
}

/**
 * Create health class chart
 */
function createHealthClassChart(data) {
    const ctx = document.getElementById('healthClassChart');
    if (!ctx || !data.applications?.by_health_class) return;

    const byClass = data.applications.by_health_class;
    const labels = Object.keys(byClass);
    const values = Object.values(byClass);

    new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: labels,
            datasets: [{
                data: values,
                backgroundColor: ['#38a169', '#48bb78', '#68d391', '#9ae6b4', '#c6f6d5'],
                borderWidth: 2,
                borderColor: '#fff'
            }]
        },
        options: { responsive: true }
    });
}

/**
 * Create policy status chart
 */
function createPolicyStatusChart(data) {
    const ctx = document.getElementById('policyStatusChart');
    if (!ctx || !data.policies?.by_status) return;

    const byStatus = data.policies.by_status;
    const labels = Object.keys(byStatus);
    const values = Object.values(byStatus);
    const colors = labels.map(l => COLORS.status[l] || COLORS.primary[0]);

    new Chart(ctx, {
        type: 'pie',
        data: {
            labels: labels,
            datasets: [{
                data: values,
                backgroundColor: colors,
                borderWidth: 2,
                borderColor: '#fff'
            }]
        },
        options: { responsive: true }
    });
}

/**
 * Create premium distribution chart
 */
function createPremiumChart(data) {
    const ctx = document.getElementById('premiumChart');
    if (!ctx || !data.policies?.premium_distribution) return;

    const dist = data.policies.premium_distribution;
    const labels = Object.keys(dist).map(k => '$' + k);
    const values = Object.values(dist);

    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Policies',
                data: values,
                backgroundColor: COLORS.primary[1],
                borderRadius: 4
            }]
        },
        options: {
            responsive: true,
            plugins: { legend: { display: false } },
            scales: { y: { beginAtZero: true } }
        }
    });
}

/**
 * Create payment frequency chart
 */
function createPaymentFreqChart(data) {
    const ctx = document.getElementById('paymentFreqChart');
    if (!ctx || !data.policies?.by_payment_frequency) return;

    const byFreq = data.policies.by_payment_frequency;
    const labels = Object.keys(byFreq);
    const values = Object.values(byFreq);

    new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: labels,
            datasets: [{
                data: values,
                backgroundColor: COLORS.accent,
                borderWidth: 2,
                borderColor: '#fff'
            }]
        },
        options: { responsive: true }
    });
}

/**
 * Create claim type chart
 */
function createClaimTypeChart(data) {
    const ctx = document.getElementById('claimTypeChart');
    if (!ctx || !data.claims?.by_type) return;

    const byType = data.claims.by_type;
    const labels = Object.keys(byType);
    const values = Object.values(byType);

    new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: labels,
            datasets: [{
                data: values,
                backgroundColor: ['#e53e3e', '#ed8936', '#ecc94b', '#48bb78'],
                borderWidth: 2,
                borderColor: '#fff'
            }]
        },
        options: { responsive: true }
    });
}

/**
 * Create claim status chart
 */
function createClaimStatusChart(data) {
    const ctx = document.getElementById('claimStatusChart');
    if (!ctx || !data.claims?.by_status) return;

    const byStatus = data.claims.by_status;
    const labels = Object.keys(byStatus);
    const values = Object.values(byStatus);
    const colors = labels.map(l => COLORS.status[l] || COLORS.primary[0]);

    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Claims',
                data: values,
                backgroundColor: colors,
                borderRadius: 4
            }]
        },
        options: {
            responsive: true,
            plugins: { legend: { display: false } },
            scales: { y: { beginAtZero: true } }
        }
    });
}

/**
 * Initialize the dashboard
 */
async function initDashboard() {
    const data = await fetchDashboardData();

    if (!data) {
        return;
    }

    // Update stats
    updateStats(data);

    // Update funnel
    updateFunnel(data);

    // Create all charts
    createProductTypeChart(data);
    createQuoteStatusChart(data);
    createQuoteSourceChart(data);
    createUnderwritingChart(data);
    createHealthClassChart(data);
    createPolicyStatusChart(data);
    createPremiumChart(data);
    createPaymentFreqChart(data);
    createClaimTypeChart(data);
    createClaimStatusChart(data);
}

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', initDashboard);
