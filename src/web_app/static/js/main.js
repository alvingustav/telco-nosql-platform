// Global variables
let socket;

// Initialize application
document.addEventListener('DOMContentLoaded', function() {
    initializeSocket();
    initializeEventListeners();
});

// Socket.IO initialization
function initializeSocket() {
    socket = io();
    
    socket.on('connect', function() {
        console.log('Connected to server');
    });
    
    socket.on('loading_progress', function(data) {
        updateLoadingProgress(data.message, data.progress);
    });
    
    socket.on('system_status_update', function(data) {
        updateSystemStatus(data);
    });
}

// Event listeners
function initializeEventListeners() {
    // Global error handling
    window.addEventListener('error', function(e) {
        console.error('Global error:', e.error);
        showAlert('danger', 'An unexpected error occurred');
    });
}

// Utility functions
function showLoading(message = 'Processing...') {
    const modal = new bootstrap.Modal(document.getElementById('loadingModal'));
    document.getElementById('loadingMessage').textContent = message;
    modal.show();
}

function hideLoading() {
    const modal = bootstrap.Modal.getInstance(document.getElementById('loadingModal'));
    if (modal) {
        modal.hide();
    }
}

function updateLoadingProgress(message, progress) {
    document.getElementById('loadingMessage').textContent = message;
    const progressBar = document.querySelector('#loadingModal .progress-bar');
    progressBar.style.width = progress + '%';
    progressBar.setAttribute('aria-valuenow', progress);
}

function showAlert(type, message) {
    const alertHtml = `
        <div class="alert alert-${type} alert-dismissible fade show" role="alert">
            ${message}
            <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
        </div>
    `;
    
    // Insert at the top of main content
    const main = document.querySelector('main');
    main.insertAdjacentHTML('afterbegin', alertHtml);
    
    // Auto-dismiss after 5 seconds
    setTimeout(() => {
        const alert = main.querySelector('.alert');
        if (alert) {
            const bsAlert = new bootstrap.Alert(alert);
            bsAlert.close();
        }
    }, 5000);
}

function formatNumber(num) {
    return new Intl.NumberFormat().format(num);
}

function formatDuration(milliseconds) {
    if (milliseconds < 1000) {
        return `${milliseconds.toFixed(2)}ms`;
    } else {
        return `${(milliseconds / 1000).toFixed(2)}s`;
    }
}

function formatBytes(bytes) {
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    if (bytes === 0) return '0 Bytes';
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    return Math.round(bytes / Math.pow(1024, i) * 100) / 100 + ' ' + sizes[i];
}

// API helper functions
function apiCall(url, method = 'GET', data = null) {
    const options = {
        method: method,
        headers: {
            'Content-Type': 'application/json',
        }
    };
    
    if (data) {
        options.body = JSON.stringify(data);
    }
    
    return fetch(url, options)
        .then(response => {
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            return response.json();
        });
}

// Chart utilities
function createChart(ctx, type, data, options = {}) {
    return new Chart(ctx, {
        type: type,
        data: data,
        options: {
            responsive: true,
            maintainAspectRatio: false,
            ...options
        }
    });
}

// Date utilities
function formatDate(dateString) {
    const date = new Date(dateString);
    return date.toLocaleDateString() + ' ' + date.toLocaleTimeString();
}

function getDateRange(days) {
    const end = new Date();
    const start = new Date();
    start.setDate(start.getDate() - days);
    
    return {
        start: start.toISOString().split('T')[0],
        end: end.toISOString().split('T')[0]
    };
}

// System status updates
function updateSystemStatus(status) {
    // Update connection indicators
    updateConnectionStatus('cassandra', status.cassandra_connected);
    updateConnectionStatus('mongodb', status.mongodb_connected);
    
    // Update timestamp
    const timestampElements = document.querySelectorAll('.system-timestamp');
    timestampElements.forEach(el => {
        el.textContent = formatDate(status.timestamp);
    });
}

function updateConnectionStatus(database, connected) {
    const indicator = document.querySelector(`#${database}-status`);
    if (indicator) {
        indicator.className = connected ? 'badge bg-success' : 'badge bg-danger';
        indicator.textContent = connected ? 'Connected' : 'Disconnected';
    }
}

// Export functions for global use
window.telcoApp = {
    showLoading,
    hideLoading,
    showAlert,
    apiCall,
    formatNumber,
    formatDuration,
    formatBytes,
    formatDate,
    getDateRange,
    createChart
};
