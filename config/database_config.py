import os

# Cassandra Configuration
CASSANDRA_CONFIG = {
    'hosts': [os.getenv("CASSANDRA_HOST", "cassandra")],
    'port': int(os.getenv("CASSANDRA_PORT", 9042)),
    'keyspace': os.getenv("CASSANDRA_KEYSPACE", "telco_cdr"),
    'replication_factor': int(os.getenv("CASSANDRA_REPLICATION_FACTOR", 1))
}

# MongoDB Configuration
MONGODB_CONFIG = {
    'uri': os.getenv("MONGO_URI", "mongodb://mongodb:27017/"),
    'database': os.getenv("MONGO_DB", "telco_customers"),
    'connection_timeout': int(os.getenv("MONGO_TIMEOUT", 30000))
}

# App Configuration
APP_CONFIG = {
    'debug': os.getenv("FLASK_DEBUG", "1") == "1",
    'host': os.getenv("FLASK_HOST", "0.0.0.0"),
    'port': int(os.getenv("FLASK_PORT", 5000)),
    'secret_key': os.getenv("FLASK_SECRET_KEY", "telco-nosql-platform-secret-key-2025")
}

# Data Export Configuration
DATA_EXPORT_CONFIG = {
    'export_directory': os.getenv("EXPORT_DIR", "telco_data_export"),
    'batch_size': int(os.getenv("EXPORT_BATCH_SIZE", 5000)),
    'max_records_per_file': int(os.getenv("EXPORT_MAX_RECORDS", 100000))
}

# Performance Testing Configuration
PERFORMANCE_CONFIG = {
    'test_iterations': int(os.getenv("PERF_ITER", 5)),
    'warmup_queries': int(os.getenv("PERF_WARMUP", 2)),
    'timeout_seconds': int(os.getenv("PERF_TIMEOUT", 30))
}
