"""
Configuration for the distributed hospital system
"""

# System Configuration
SYSTEM_NAME = "GROUP 4 MATIBABU REFERRAL HOSPITAL"
HOSPITAL_NAME = "Matibabu Referral Hospital"

# Node Configuration
NODES = {
    'node1': {
        'id': 'N001',
        'name': 'Cardiology Department',
        'host': 'localhost',
        'port': 5001,
        'slots': 15,
        'service': 'Cardiac Consultation',
        'color': '#FF6B6B'
    },
    'node2': {
        'id': 'N002',
        'name': 'Radiology Department',
        'host': 'localhost',
        'port': 5002,
        'slots': 10,
        'service': 'MRI/X-Ray Scanning',
        'color': '#4ECDC4'
    },
    'node3': {
        'id': 'N003',
        'name': 'Orthopedics Department',
        'host': 'localhost',
        'port': 5003,
        'slots': 12,
        'service': 'Bone & Joint Consultation',
        'color': '#95E1D3'
    },
    'node4': {
        'id': 'N004',
        'name': 'Emergency Department',
        'host': 'localhost',
        'port': 5004,
        'slots': 20,
        'service': 'Emergency Care',
        'color': '#FFD166'
    }
}

# Coordinator Configuration
COORDINATOR = {
    'host': 'localhost',
    'port': 5000,
    'heartbeat_interval': 5,  # seconds
    'timeout': 10  # seconds
}

# Transaction Configuration
TRANSACTION_TIMEOUT = 30  # seconds
MAX_RETRIES = 3

# Database Configuration
DATABASE_FILE = 'hospital_data.db'

# Web Interface
WEB_HOST = 'localhost'
WEB_PORT = 5050
DEBUG_MODE = True