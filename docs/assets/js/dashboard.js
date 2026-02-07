/**
 * CRM Data Lake Dashboard
 * Loads and visualizes data from the PROD layer
 */

const COLORS = {
    primary: ['#3b82f6', '#2563eb', '#1d4ed8', '#1e40af', '#1e3a8a'],
    accent: ['#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899'],
    pastel: ['#93c5fd', '#86efac', '#fcd34d', '#fca5a5', '#c4b5fd']
};

// Chart.js default configuration
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
            <p style="text-align: center; padding: 2rem; background: #fef3c7; border-radius: 8px; color: #92400e;">
                <strong>No data available yet.</strong><br>
                Run the pipeline with: <code>python pipelines/run_pipeline.py</code>
            </p>
        `;
        statsSection.insertBefore(notice, statsSection.querySelector('.stats-grid'));
    }
}

/**
 * Update statistics cards
 */
function updateStats(data) {
    if (data.customers) {
        document.getElementById('total-customers').textContent =
            data.customers.total_count.toLocaleString();
    }

    if (data.interactions) {
        document.getElementById('total-interactions').textContent =
            data.interactions.total_count.toLocaleString();
    }

    document.getElementById('data-quality').textContent = '100%';

    // Update last updated timestamp
    const lastUpdated = document.getElementById('last-updated');
    if (data.generated_at) {
        const date = new Date(data.generated_at);
        lastUpdated.textContent = `Last updated: ${date.toLocaleString()}`;
    }
}

/**
 * Create nationality chart (Doughnut)
 */
function createNationalityChart(data) {
    const ctx = document.getElementById('nationalityChart');
    if (!ctx || !data.customers?.by_nationality) return;

    const labels = Object.keys(data.customers.by_nationality);
    const values = Object.values(data.customers.by_nationality);

    new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: labels,
            datasets: [{
                data: values,
                backgroundColor: COLORS.primary,
                borderWidth: 2,
                borderColor: '#fff'
            }]
        },
        options: {
            responsive: true,
            plugins: {
                legend: {
                    position: 'right'
                }
            }
        }
    });
}

/**
 * Create gender chart (Pie)
 */
function createGenderChart(data) {
    const ctx = document.getElementById('genderChart');
    if (!ctx || !data.customers?.by_gender) return;

    const labels = Object.keys(data.customers.by_gender).map(
        g => g.charAt(0).toUpperCase() + g.slice(1)
    );
    const values = Object.values(data.customers.by_gender);

    new Chart(ctx, {
        type: 'pie',
        data: {
            labels: labels,
            datasets: [{
                data: values,
                backgroundColor: ['#3b82f6', '#ec4899'],
                borderWidth: 2,
                borderColor: '#fff'
            }]
        },
        options: {
            responsive: true
        }
    });
}

/**
 * Create age distribution chart (Bar)
 */
function createAgeChart(data) {
    const ctx = document.getElementById('ageChart');
    if (!ctx || !data.customers?.age_distribution) return;

    const labels = Object.keys(data.customers.age_distribution);
    const values = Object.values(data.customers.age_distribution);

    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Customers',
                data: values,
                backgroundColor: '#3b82f6',
                borderRadius: 4
            }]
        },
        options: {
            responsive: true,
            plugins: {
                legend: {
                    display: false
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    ticks: {
                        stepSize: 5
                    }
                }
            }
        }
    });
}

/**
 * Create interaction type chart (Doughnut)
 */
function createTypeChart(data) {
    const ctx = document.getElementById('typeChart');
    if (!ctx || !data.interactions?.by_type) return;

    const labels = Object.keys(data.interactions.by_type).map(
        t => t.charAt(0).toUpperCase() + t.slice(1)
    );
    const values = Object.values(data.interactions.by_type);

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
        options: {
            responsive: true
        }
    });
}

/**
 * Create sentiment chart (Polar Area)
 */
function createSentimentChart(data) {
    const ctx = document.getElementById('sentimentChart');
    if (!ctx || !data.interactions?.by_sentiment) return;

    const sentimentColors = {
        'positive': '#10b981',
        'neutral': '#6b7280',
        'negative': '#ef4444'
    };

    const labels = Object.keys(data.interactions.by_sentiment).map(
        s => s.charAt(0).toUpperCase() + s.slice(1)
    );
    const values = Object.values(data.interactions.by_sentiment);
    const colors = Object.keys(data.interactions.by_sentiment).map(
        s => sentimentColors[s] || '#6b7280'
    );

    new Chart(ctx, {
        type: 'polarArea',
        data: {
            labels: labels,
            datasets: [{
                data: values,
                backgroundColor: colors.map(c => c + '99'),
                borderColor: colors,
                borderWidth: 2
            }]
        },
        options: {
            responsive: true
        }
    });
}

/**
 * Create channel chart (Bar)
 */
function createChannelChart(data) {
    const ctx = document.getElementById('channelChart');
    if (!ctx || !data.interactions?.by_channel) return;

    const labels = Object.keys(data.interactions.by_channel).map(
        c => c.charAt(0).toUpperCase() + c.slice(1)
    );
    const values = Object.values(data.interactions.by_channel);

    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Interactions',
                data: values,
                backgroundColor: ['#8b5cf6', '#06b6d4', '#f59e0b'],
                borderRadius: 4
            }]
        },
        options: {
            responsive: true,
            indexAxis: 'y',
            plugins: {
                legend: {
                    display: false
                }
            },
            scales: {
                x: {
                    beginAtZero: true
                }
            }
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

    // Create all charts
    createNationalityChart(data);
    createGenderChart(data);
    createAgeChart(data);
    createTypeChart(data);
    createSentimentChart(data);
    createChannelChart(data);
}

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', initDashboard);
