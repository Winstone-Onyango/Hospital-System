/**
 * Main JavaScript file for Hospital Distributed System
 */

// Global variables
let currentTransaction = null;
let systemStatusInterval = null;

// DOM Ready
document.addEventListener('DOMContentLoaded', function () {
    // Initialize tooltips
    initializeTooltips();

    // Initialize modals
    initializeModals();

    // Initialize form validation
    initializeFormValidation();

    // Start system status updates
    startSystemStatusUpdates();
});

// Tooltips
function initializeTooltips() {
    const tooltips = document.querySelectorAll('[data-tooltip]');

    tooltips.forEach(element => {
        element.addEventListener('mouseenter', function (e) {
            const tooltipText = this.getAttribute('data-tooltip');
            if (!tooltipText) return;

            const tooltip = document.createElement('div');
            tooltip.className = 'tooltip';
            tooltip.textContent = tooltipText;
            document.body.appendChild(tooltip);

            const rect = this.getBoundingClientRect();
            tooltip.style.left = rect.left + (rect.width / 2) - (tooltip.offsetWidth / 2) + 'px';
            tooltip.style.top = rect.top - tooltip.offsetHeight - 10 + 'px';

            this.tooltipElement = tooltip;
        });

        element.addEventListener('mouseleave', function () {
            if (this.tooltipElement) {
                this.tooltipElement.remove();
                this.tooltipElement = null;
            }
        });
    });
}

// Modals
function initializeModals() {
    // Close modals when clicking outside
    document.addEventListener('click', function (e) {
        if (e.target.classList.contains('modal')) {
            e.target.style.display = 'none';
        }
    });

    // Escape key closes modals
    document.addEventListener('keydown', function (e) {
        if (e.key === 'Escape') {
            const modals = document.querySelectorAll('.modal');
            modals.forEach(modal => {
                modal.style.display = 'none';
            });
        }
    });
}

// Form Validation
function initializeFormValidation() {
    const forms = document.querySelectorAll('form[needs-validation]');

    forms.forEach(form => {
        form.addEventListener('submit', function (e) {
            if (!this.checkValidity()) {
                e.preventDefault();
                e.stopPropagation();

                // Add validation styles
                this.classList.add('was-validated');

                // Scroll to first invalid field
                const firstInvalid = this.querySelector(':invalid');
                if (firstInvalid) {
                    firstInvalid.scrollIntoView({ behavior: 'smooth', block: 'center' });
                    firstInvalid.focus();
                }
            }
        });
    });
}

// System Status Updates
function startSystemStatusUpdates() {
    // Update every 10 seconds
    systemStatusInterval = setInterval(updateSystemStatus, 10000);
    updateSystemStatus();
}

async function updateSystemStatus() {
    try {
        const response = await fetch('/api/status');
        if (!response.ok) throw new Error('Network response was not ok');

        const data = await response.json();

        if (data.success) {
            updateStatusDisplay(data.status);
        }
    } catch (error) {
        console.error('Failed to update system status:', error);
        showToast('Failed to update system status', 'error');
    }
}

function updateStatusDisplay(status) {
    // Update various status indicators on the page
    // This would be implemented based on the current page

    // Example: Update transaction count
    const txnCountElements = document.querySelectorAll('.txn-count');
    txnCountElements.forEach(el => {
        el.textContent = status?.active_transactions || 0;
    });

    // Example: Update node status indicators
    if (status?.nodes) {
        updateNodeStatusIndicators(status.nodes);
    }
}

function updateNodeStatusIndicators(nodes) {
    // Update visual indicators for node status
    Object.entries(nodes).forEach(([nodeId, nodeInfo]) => {
        const indicator = document.querySelector(`.node-status[data-node="${nodeId}"]`);
        if (indicator) {
            indicator.className = `node-status ${nodeInfo.status}`;
            indicator.textContent = nodeInfo.status === 'active' ? 'Online' : 'Offline';
        }
    });
}

// Toast Notifications
function showToast(message, type = 'info', duration = 3000) {
    // Remove existing toasts
    const existingToasts = document.querySelectorAll('.toast');
    existingToasts.forEach(toast => toast.remove());

    // Create toast
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;

    // Add icon based on type
    let icon = 'info-circle';
    if (type === 'success') icon = 'check-circle';
    if (type === 'error') icon = 'exclamation-circle';
    if (type === 'warning') icon = 'exclamation-triangle';

    toast.innerHTML = `
        <i class="fas fa-${icon}"></i>
        <span>${message}</span>
    `;

    // Add to DOM
    document.body.appendChild(toast);

    // Animate in
    setTimeout(() => toast.classList.add('show'), 10);

    // Remove after duration
    setTimeout(() => {
        toast.classList.remove('show');
        setTimeout(() => toast.remove(), 300);
    }, duration);

    // Click to dismiss
    toast.addEventListener('click', () => {
        toast.classList.remove('show');
        setTimeout(() => toast.remove(), 300);
    });
}

// Transaction Progress
function showTransactionProgress(transactionId) {
    currentTransaction = transactionId;

    const progressModal = document.getElementById('transactionProgressModal');
    if (progressModal) {
        progressModal.style.display = 'flex';

        // Start progress simulation
        simulateTransactionProgress();
    }
}

function simulateTransactionProgress() {
    if (!currentTransaction) return;

    const steps = [
        { step: 1, label: 'Initializing transaction', duration: 1000 },
        { step: 2, label: 'Acquiring locks', duration: 1500 },
        { step: 3, label: 'Prepare phase - collecting votes', duration: 2000 },
        { step: 4, label: 'Commit phase - finalizing', duration: 1500 },
        { step: 5, label: 'Transaction completed', duration: 1000 }
    ];

    let currentStep = 0;

    function nextStep() {
        if (currentStep >= steps.length) {
            // Transaction complete
            completeTransaction();
            return;
        }

        const step = steps[currentStep];
        updateProgressStep(step.step, step.label);

        currentStep++;
        setTimeout(nextStep, step.duration);
    }

    nextStep();
}

function updateProgressStep(stepNumber, label) {
    const stepElements = document.querySelectorAll('.progress-step');
    const statusLabel = document.getElementById('progressStatus');

    // Reset all steps
    stepElements.forEach(el => {
        el.classList.remove('active', 'completed');
    });

    // Mark previous steps as completed
    for (let i = 1; i < stepNumber; i++) {
        const step = document.querySelector(`.progress-step[data-step="${i}"]`);
        if (step) step.classList.add('completed');
    }

    // Mark current step as active
    const currentStep = document.querySelector(`.progress-step[data-step="${stepNumber}"]`);
    if (currentStep) currentStep.classList.add('active');

    // Update status label
    if (statusLabel) {
        statusLabel.textContent = label;
    }
}

function completeTransaction() {
    showToast('Transaction completed successfully!', 'success');

    const progressModal = document.getElementById('transactionProgressModal');
    if (progressModal) {
        setTimeout(() => {
            progressModal.style.display = 'none';
            currentTransaction = null;
        }, 2000);
    }
}

// Node Management
async function restartNode(nodeId) {
    if (!confirm(`Restart node ${nodeId}? This will temporarily interrupt service.`)) {
        return;
    }

    showToast(`Restarting node ${nodeId}...`, 'info');

    try {
        // In a real implementation, this would call an API
        await new Promise(resolve => setTimeout(resolve, 2000));

        showToast(`Node ${nodeId} restarted successfully`, 'success');
    } catch (error) {
        showToast(`Failed to restart node: ${error.message}`, 'error');
    }
}

async function checkNodeHealth(nodeId) {
    try {
        const response = await fetch(`/api/nodes/${nodeId}/health`);
        const data = await response.json();

        if (data.success) {
            return data.health;
        } else {
            throw new Error(data.error || 'Health check failed');
        }
    } catch (error) {
        console.error(`Health check failed for node ${nodeId}:`, error);
        return 'unhealthy';
    }
}

// Data Export
function exportSystemData() {
    // This would export system logs, transaction history, etc.
    showToast('Preparing data for export...', 'info');

    // Simulate export process
    setTimeout(() => {
        const data = {
            timestamp: new Date().toISOString(),
            system: 'Hospital Distributed System',
            version: '1.0.0',
            data: 'Sample exported data'
        };

        const dataStr = JSON.stringify(data, null, 2);
        const dataBlob = new Blob([dataStr], { type: 'application/json' });
        const url = URL.createObjectURL(dataBlob);

        const a = document.createElement('a');
        a.href = url;
        a.download = `system-export-${new Date().toISOString().split('T')[0]}.json`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);

        showToast('Data exported successfully', 'success');
    }, 1000);
}

// Utility Functions
function formatDateTime(date) {
    if (!date) date = new Date();
    if (typeof date === 'string') date = new Date(date);

    return date.toLocaleDateString() + ' ' + date.toLocaleTimeString();
}

function formatDuration(seconds) {
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const secs = Math.floor(seconds % 60);

    if (hours > 0) {
        return `${hours}h ${minutes}m ${secs}s`;
    } else if (minutes > 0) {
        return `${minutes}m ${secs}s`;
    } else {
        return `${secs}s`;
    }
}

function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

function throttle(func, limit) {
    let inThrottle;
    return function (...args) {
        if (!inThrottle) {
            func.apply(this, args);
            inThrottle = true;
            setTimeout(() => inThrottle = false, limit);
        }
    };
}

// Clean up on page unload
window.addEventListener('beforeunload', function () {
    if (systemStatusInterval) {
        clearInterval(systemStatusInterval);
    }
});

// Export for use in other modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        showToast,
        formatDateTime,
        formatDuration,
        debounce,
        throttle
    };
}