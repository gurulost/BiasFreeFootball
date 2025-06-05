// CFB Bias-Free Rankings - JavaScript Application

// Global variables
let currentFilter = 'all';
let currentPage = 1;
const itemsPerPage = 25;

// Initialize application
document.addEventListener('DOMContentLoaded', function() {
    initializeEventListeners();
    updateTimestamps();
});

// Event Listeners
function initializeEventListeners() {
    // Pipeline control buttons
    const runLiveBtn = document.getElementById('run-live-btn');
    const runRetroBtn = document.getElementById('run-retro-btn');
    
    if (runLiveBtn) {
        runLiveBtn.addEventListener('click', runLivePipeline);
    }
    
    if (runRetroBtn) {
        runRetroBtn.addEventListener('click', runRetroPipeline);
    }
    
    // Filter dropdowns
    const filterItems = document.querySelectorAll('[data-filter]');
    filterItems.forEach(item => {
        item.addEventListener('click', function(e) {
            e.preventDefault();
            const filter = this.getAttribute('data-filter');
            applyFilter(filter);
        });
    });
    
    // Auto-refresh data every 5 minutes
    setInterval(refreshData, 300000);
}

// Pipeline Operations
async function runLivePipeline() {
    const weekInput = document.getElementById('live-week');
    const seasonInput = document.getElementById('live-season');
    const button = document.getElementById('run-live-btn');
    
    if (!weekInput || !seasonInput) return;
    
    const week = parseInt(weekInput.value) || 1;
    const season = parseInt(seasonInput.value) || 2023;
    
    showPipelineStatus('Running live pipeline...', 'info');
    setButtonLoading(button, true);
    
    try {
        const response = await fetch('/api/run-pipeline', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                type: 'live',
                week: week,
                season: season
            })
        });
        
        const result = await response.json();
        
        if (result.success) {
            showPipelineStatus('Live pipeline completed successfully!', 'success');
            setTimeout(() => {
                window.location.reload();
            }, 2000);
        } else {
            showPipelineStatus(`Pipeline failed: ${result.error}`, 'error');
        }
    } catch (error) {
        showPipelineStatus(`Error: ${error.message}`, 'error');
    } finally {
        setButtonLoading(button, false);
    }
}

async function runRetroPipeline() {
    const seasonInput = document.getElementById('retro-season');
    const button = document.getElementById('run-retro-btn');
    
    if (!seasonInput) return;
    
    const season = parseInt(seasonInput.value) || 2023;
    
    showPipelineStatus('Running retro pipeline... This may take several minutes.', 'info');
    setButtonLoading(button, true);
    
    try {
        const response = await fetch('/api/run-pipeline', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                type: 'retro',
                season: season
            })
        });
        
        const result = await response.json();
        
        if (result.success) {
            showPipelineStatus('Retro pipeline completed successfully!', 'success');
            setTimeout(() => {
                window.location.reload();
            }, 2000);
        } else {
            showPipelineStatus(`Pipeline failed: ${result.error}`, 'error');
        }
    } catch (error) {
        showPipelineStatus(`Error: ${error.message}`, 'error');
    } finally {
        setButtonLoading(button, false);
    }
}

// UI Helper Functions
function showPipelineStatus(message, type) {
    const statusDiv = document.getElementById('pipeline-status');
    const messageSpan = document.getElementById('pipeline-message');
    
    if (!statusDiv || !messageSpan) return;
    
    statusDiv.style.display = 'block';
    messageSpan.textContent = message;
    
    const alertDiv = statusDiv.querySelector('.alert');
    alertDiv.className = `alert alert-${type === 'error' ? 'danger' : type === 'success' ? 'success' : 'info'}`;
    
    if (type !== 'info') {
        setTimeout(() => {
            statusDiv.style.display = 'none';
        }, 5000);
    }
}

function setButtonLoading(button, loading) {
    if (!button) return;
    
    if (loading) {
        button.disabled = true;
        const icon = button.querySelector('i');
        if (icon) {
            icon.className = 'fas fa-spinner fa-spin me-1';
        }
    } else {
        button.disabled = false;
        const icon = button.querySelector('i');
        if (icon) {
            icon.className = button.id.includes('live') ? 'fas fa-play me-1' : 'fas fa-history me-1';
        }
    }
}

// Rankings Table Functions
function populateTop25Table(teamRatings) {
    const tbody = document.querySelector('#top25-table tbody');
    if (!tbody || !teamRatings) return;
    
    // Convert ratings object to sorted array
    const sortedTeams = Object.entries(teamRatings)
        .sort(([,a], [,b]) => b - a)
        .slice(0, 25);
    
    tbody.innerHTML = '';
    
    sortedTeams.forEach(([team, rating], index) => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td><strong>${index + 1}</strong></td>
            <td>${team}</td>
            <td>${rating.toFixed(4)}</td>
            <td><span class="badge conference-badge">${getTeamConference(team)}</span></td>
        `;
        tbody.appendChild(row);
    });
}

function populateRankingsTable(teamRatings, conferenceRatings) {
    const tbody = document.getElementById('rankings-tbody');
    if (!tbody || !teamRatings) return;
    
    // Convert ratings to rankings with additional data
    const sortedTeams = Object.entries(teamRatings)
        .sort(([,a], [,b]) => b - a);
    
    tbody.innerHTML = '';
    
    sortedTeams.forEach(([team, rating], index) => {
        const rank = index + 1;
        const conference = getTeamConference(team);
        const confWeight = conferenceRatings?.[conference] || 0.5;
        
        const row = document.createElement('tr');
        row.innerHTML = `
            <td><strong>${rank}</strong></td>
            <td>
                <div class="d-flex align-items-center">
                    <span>${team}</span>
                </div>
            </td>
            <td><span class="badge conference-badge conf-${conference.toLowerCase().replace(/[^a-z0-9]/g, '-')}">${conference}</span></td>
            <td>${rating.toFixed(4)}</td>
            <td><span class="rank-change neutral">-</span></td>
            <td>${confWeight.toFixed(3)}</td>
            <td><div class="quality-wins">-</div></td>
            <td>
                <button class="btn btn-sm btn-outline-primary" onclick="showTeamDetails('${team}', ${rank}, ${rating}, '${conference}')">
                    <i class="fas fa-info-circle"></i>
                </button>
            </td>
        `;
        tbody.appendChild(row);
    });
    
    updatePagination(sortedTeams.length);
}

// Conference Chart Functions
function createConferenceChart(conferenceRatings) {
    const canvas = document.getElementById('conference-chart');
    if (!canvas || !conferenceRatings) return;
    
    const ctx = canvas.getContext('2d');
    
    const sortedConferences = Object.entries(conferenceRatings)
        .sort(([,a], [,b]) => b - a);
    
    const labels = sortedConferences.map(([conf]) => conf);
    const data = sortedConferences.map(([,rating]) => rating);
    
    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Conference Strength',
                data: data,
                backgroundColor: 'rgba(64, 128, 191, 0.6)',
                borderColor: 'rgba(64, 128, 191, 1)',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    grid: {
                        color: 'rgba(255, 255, 255, 0.1)'
                    },
                    ticks: {
                        color: 'rgba(255, 255, 255, 0.8)'
                    }
                },
                x: {
                    grid: {
                        color: 'rgba(255, 255, 255, 0.1)'
                    },
                    ticks: {
                        color: 'rgba(255, 255, 255, 0.8)',
                        maxRotation: 45
                    }
                }
            }
        }
    });
}

// Bias Audit Functions
function createBiasTrendChart(metricsHistory) {
    const canvas = document.getElementById('bias-trend-chart');
    if (!canvas || !metricsHistory) return;
    
    const ctx = canvas.getContext('2d');
    
    const labels = metricsHistory.map(m => `Week ${m.week || 'N/A'}`);
    const data = metricsHistory.map(m => m.neutrality_metric || 0);
    
    new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: 'Neutrality Metric (B)',
                data: data,
                borderColor: 'rgba(64, 128, 191, 1)',
                backgroundColor: 'rgba(64, 128, 191, 0.1)',
                borderWidth: 2,
                fill: true,
                tension: 0.4
            }, {
                label: 'Threshold (0.06)',
                data: Array(data.length).fill(0.06),
                borderColor: 'rgba(255, 193, 7, 1)',
                borderWidth: 2,
                borderDash: [5, 5],
                fill: false
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    labels: {
                        color: 'rgba(255, 255, 255, 0.8)'
                    }
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    max: 0.1,
                    grid: {
                        color: 'rgba(255, 255, 255, 0.1)'
                    },
                    ticks: {
                        color: 'rgba(255, 255, 255, 0.8)',
                        callback: function(value) {
                            return value.toFixed(3);
                        }
                    }
                },
                x: {
                    grid: {
                        color: 'rgba(255, 255, 255, 0.1)'
                    },
                    ticks: {
                        color: 'rgba(255, 255, 255, 0.8)'
                    }
                }
            }
        }
    });
}

function createConferenceTrajectoryChart(trajectories) {
    const canvas = document.getElementById('conference-trajectory-chart');
    if (!canvas || !trajectories) return;
    
    const ctx = canvas.getContext('2d');
    
    const colors = [
        'rgba(255, 99, 132, 1)',
        'rgba(54, 162, 235, 1)',
        'rgba(255, 205, 86, 1)',
        'rgba(75, 192, 192, 1)',
        'rgba(153, 102, 255, 1)',
        'rgba(255, 159, 64, 1)'
    ];
    
    const datasets = Object.entries(trajectories).map(([conf, data], index) => ({
        label: conf,
        data: data.weeks.map((week, i) => ({
            x: week,
            y: data.mean_ratings[i]
        })),
        borderColor: colors[index % colors.length],
        backgroundColor: colors[index % colors.length].replace('1)', '0.1)'),
        borderWidth: 2,
        fill: false,
        tension: 0.4
    }));
    
    new Chart(ctx, {
        type: 'line',
        data: { datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    labels: {
                        color: 'rgba(255, 255, 255, 0.8)'
                    }
                }
            },
            scales: {
                y: {
                    grid: {
                        color: 'rgba(255, 255, 255, 0.1)'
                    },
                    ticks: {
                        color: 'rgba(255, 255, 255, 0.8)'
                    }
                },
                x: {
                    grid: {
                        color: 'rgba(255, 255, 255, 0.1)'
                    },
                    ticks: {
                        color: 'rgba(255, 255, 255, 0.8)'
                    }
                }
            }
        }
    });
}

function createRiskMultiplierChart() {
    const canvas = document.getElementById('risk-multiplier-chart');
    if (!canvas) return;
    
    const ctx = canvas.getContext('2d');
    
    // Generate sample risk multiplier distribution
    const multipliers = [];
    for (let i = 0; i < 1000; i++) {
        const p = Math.random();
        const risk = p < 0.5 ? (1 - p) / 0.5 : Math.pow(p / 0.5, 1);
        const surprise = 1 + 0.75 * Math.random() * 3;
        multipliers.push(risk * surprise);
    }
    
    // Create histogram bins
    const bins = Array(20).fill(0);
    const binSize = 4 / 20;
    
    multipliers.forEach(mult => {
        const binIndex = Math.min(Math.floor(mult / binSize), 19);
        bins[binIndex]++;
    });
    
    const labels = bins.map((_, i) => (i * binSize).toFixed(1));
    
    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Frequency',
                data: bins,
                backgroundColor: 'rgba(64, 128, 191, 0.6)',
                borderColor: 'rgba(64, 128, 191, 1)',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false
                }
            },
            scales: {
                y: {
                    grid: {
                        color: 'rgba(255, 255, 255, 0.1)'
                    },
                    ticks: {
                        color: 'rgba(255, 255, 255, 0.8)'
                    }
                },
                x: {
                    grid: {
                        color: 'rgba(255, 255, 255, 0.1)'
                    },
                    ticks: {
                        color: 'rgba(255, 255, 255, 0.8)'
                    }
                }
            }
        }
    });
}

function populateConferenceMetricsTable(latestMetrics) {
    const tbody = document.getElementById('conference-metrics-table');
    if (!tbody || !latestMetrics || !latestMetrics.conferences) return;
    
    tbody.innerHTML = '';
    
    Object.entries(latestMetrics.conferences).forEach(([conf, data]) => {
        const row = document.createElement('tr');
        const status = data.deviation_from_global > 0.06 ? 'danger' : 
                      data.deviation_from_global > 0.04 ? 'warning' : 'success';
        
        row.innerHTML = `
            <td><span class="badge conf-${conf.toLowerCase().replace(/[^a-z0-9]/g, '-')}">${conf}</span></td>
            <td>${data.mean_rating.toFixed(4)}</td>
            <td>${data.deviation_from_global.toFixed(4)}</td>
            <td>${data.team_count}</td>
            <td>${data.min_rating.toFixed(4)} - ${data.max_rating.toFixed(4)}</td>
            <td><span class="badge bg-${status}">${status.toUpperCase()}</span></td>
        `;
        tbody.appendChild(row);
    });
}

function populateConferenceBiasList(latestMetrics) {
    const container = document.getElementById('conference-bias-list');
    if (!container || !latestMetrics || !latestMetrics.conferences) return;
    
    const sortedConferences = Object.entries(latestMetrics.conferences)
        .sort(([,a], [,b]) => b.deviation_from_global - a.deviation_from_global)
        .slice(0, 5);
    
    container.innerHTML = '';
    
    sortedConferences.forEach(([conf, data]) => {
        const alertClass = data.deviation_from_global > 0.06 ? 'warning' : 'info';
        
        const alertDiv = document.createElement('div');
        alertDiv.className = `alert alert-${alertClass} alert-sm p-2 mb-2`;
        alertDiv.innerHTML = `
            <strong>${conf}</strong><br>
            <small>Deviation: ${data.deviation_from_global.toFixed(4)}</small>
        `;
        container.appendChild(alertDiv);
    });
}

// Utility Functions
function getTeamConference(team) {
    // Simplified conference mapping
    const conferences = {
        'Alabama': 'SEC', 'Georgia': 'SEC', 'LSU': 'SEC', 'Florida': 'SEC',
        'Auburn': 'SEC', 'Tennessee': 'SEC', 'Texas A&M': 'SEC',
        'Ohio State': 'Big Ten', 'Michigan': 'Big Ten', 'Penn State': 'Big Ten',
        'Wisconsin': 'Big Ten', 'Iowa': 'Big Ten', 'Nebraska': 'Big Ten',
        'Oklahoma': 'Big 12', 'Texas': 'Big 12', 'Oklahoma State': 'Big 12',
        'Baylor': 'Big 12', 'TCU': 'Big 12', 'Kansas State': 'Big 12',
        'Clemson': 'ACC', 'North Carolina': 'ACC', 'NC State': 'ACC',
        'Virginia Tech': 'ACC', 'Miami': 'ACC', 'Florida State': 'ACC',
        'USC': 'Pac-12', 'UCLA': 'Pac-12', 'Oregon': 'Pac-12',
        'Washington': 'Pac-12', 'Stanford': 'Pac-12', 'Utah': 'Pac-12'
    };
    
    return conferences[team] || 'Independent';
}

function showTeamDetails(team, rank, rating, conference) {
    const modal = new bootstrap.Modal(document.getElementById('teamDetailsModal'));
    
    document.getElementById('modal-team-name').textContent = team;
    document.getElementById('modal-rank').textContent = rank;
    document.getElementById('modal-rating').textContent = rating.toFixed(4);
    document.getElementById('modal-conference').textContent = conference;
    document.getElementById('modal-conf-weight').textContent = '-';
    document.getElementById('modal-rank-change').textContent = '-';
    document.getElementById('modal-prev-rank').textContent = '-';
    
    modal.show();
}

function applyFilter(filter) {
    currentFilter = filter;
    currentPage = 1;
    
    // Update active filter button
    document.querySelectorAll('[data-filter]').forEach(btn => {
        btn.classList.remove('active');
    });
    document.querySelector(`[data-filter="${filter}"]`).classList.add('active');
    
    // Re-populate table with filter
    if (window.ratingsData) {
        populateRankingsTable(window.ratingsData.team_ratings, window.ratingsData.conference_ratings);
    }
}

function updatePagination(totalItems) {
    const pagination = document.getElementById('rankings-pagination');
    if (!pagination) return;
    
    const totalPages = Math.ceil(totalItems / itemsPerPage);
    pagination.innerHTML = '';
    
    // Previous button
    const prevLi = document.createElement('li');
    prevLi.className = `page-item ${currentPage === 1 ? 'disabled' : ''}`;
    prevLi.innerHTML = '<a class="page-link" href="#" onclick="changePage(-1)">Previous</a>';
    pagination.appendChild(prevLi);
    
    // Page numbers
    for (let i = 1; i <= totalPages; i++) {
        if (i === 1 || i === totalPages || (i >= currentPage - 2 && i <= currentPage + 2)) {
            const li = document.createElement('li');
            li.className = `page-item ${i === currentPage ? 'active' : ''}`;
            li.innerHTML = `<a class="page-link" href="#" onclick="changePage(${i - currentPage})">${i}</a>`;
            pagination.appendChild(li);
        } else if (i === currentPage - 3 || i === currentPage + 3) {
            const li = document.createElement('li');
            li.className = 'page-item disabled';
            li.innerHTML = '<span class="page-link">...</span>';
            pagination.appendChild(li);
        }
    }
    
    // Next button
    const nextLi = document.createElement('li');
    nextLi.className = `page-item ${currentPage === totalPages ? 'disabled' : ''}`;
    nextLi.innerHTML = '<a class="page-link" href="#" onclick="changePage(1)">Next</a>';
    pagination.appendChild(nextLi);
}

function changePage(delta) {
    if (typeof delta === 'number') {
        currentPage = Math.max(1, currentPage + delta);
    }
    
    if (window.ratingsData) {
        populateRankingsTable(window.ratingsData.team_ratings, window.ratingsData.conference_ratings);
    }
}

function updateTimestamps() {
    const timestamps = document.querySelectorAll('[data-timestamp]');
    timestamps.forEach(el => {
        const timestamp = el.getAttribute('data-timestamp');
        if (timestamp) {
            const date = new Date(timestamp);
            el.textContent = date.toLocaleString();
        }
    });
}

async function refreshData() {
    try {
        const response = await fetch('/api/bias-metrics');
        const biasMetrics = await response.json();
        
        // Update bias metric display
        const metricElement = document.getElementById('neutrality-metric');
        const statusElement = document.getElementById('neutrality-status');
        
        if (metricElement && biasMetrics.neutrality_metric !== undefined) {
            metricElement.textContent = biasMetrics.neutrality_metric.toFixed(3);
            
            if (statusElement) {
                const metric = biasMetrics.neutrality_metric;
                if (metric <= 0.06) {
                    statusElement.innerHTML = '<span class="badge bg-success">PASS</span>';
                } else if (metric <= 0.08) {
                    statusElement.innerHTML = '<span class="badge bg-warning">CAUTION</span>';
                } else {
                    statusElement.innerHTML = '<span class="badge bg-danger">FAIL</span>';
                }
            }
        }
    } catch (error) {
        console.warn('Failed to refresh data:', error);
    }
}

// Rankings page specific functions
function initializeRankingsFilters() {
    // Initialize filter functionality for rankings page
    console.log('Rankings filters initialized');
}

function initializeRankingsPagination() {
    // Initialize pagination for rankings page
    console.log('Rankings pagination initialized');
}

// Export functions for global access
window.showTeamDetails = showTeamDetails;
window.changePage = changePage;
window.populateTop25Table = populateTop25Table;
window.createConferenceChart = createConferenceChart;
window.populateRankingsTable = populateRankingsTable;
window.initializeRankingsFilters = initializeRankingsFilters;
window.initializeRankingsPagination = initializeRankingsPagination;
