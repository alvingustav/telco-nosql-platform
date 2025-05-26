import os
from typing import Dict, Any

# Database Configuration Constants
CASSANDRA_CONFIG = {
    'hosts': ['127.0.0.1'],
    'port': 9042,
    'keyspace': 'telco_cdr',
    'replication_factor': 1
}

MONGODB_CONFIG = {
    'uri': 'mongodb://localhost:27017/',
    'database': 'telco_customers',
    'connection_timeout': 30000
}

# Application Configuration
APP_CONFIG = {
    'debug': True,
    'host': '0.0.0.0',
    'port': 5000,
    'secret_key': 'telco-nosql-platform-secret-key-2025'
}

# Data Export Configuration
DATA_EXPORT_CONFIG = {
    'export_directory': 'telco_data_export',
    'batch_size': 5000,
    'max_records_per_file': 100000
}

# Performance Testing Configuration
PERFORMANCE_CONFIG = {
    'test_iterations': 5,
    'warmup_queries': 2,
    'timeout_seconds': 30
}
