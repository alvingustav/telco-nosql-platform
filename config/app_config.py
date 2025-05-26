import os
from typing import Dict, Any

# Application Configuration
APP_CONFIG = {
    'debug': os.getenv('DEBUG', 'True').lower() == 'true',
    'host': os.getenv('HOST', '0.0.0.0'),
    'port': int(os.getenv('PORT', 5000)),
    'secret_key': os.getenv('SECRET_KEY', 'telco-nosql-platform-secret-key-2025'),
    'environment': os.getenv('ENVIRONMENT', 'development')
}

# Logging Configuration
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'default': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        },
        'detailed': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',
            'formatter': 'default',
            'stream': 'ext://sys.stdout'
        },
        'file': {
            'class': 'logging.FileHandler',
            'level': 'DEBUG',
            'formatter': 'detailed',
            'filename': 'logs/telco_platform.log',
            'mode': 'a'
        }
    },
    'loggers': {
        '': {
            'level': 'DEBUG',
            'handlers': ['console', 'file']
        },
        'cassandra': {
            'level': 'WARNING',
            'handlers': ['console', 'file'],
            'propagate': False
        },
        'pymongo': {
            'level': 'WARNING', 
            'handlers': ['console', 'file'],
            'propagate': False
        }
    }
}

# Security Configuration
SECURITY_CONFIG = {
    'session_timeout': 3600,  # 1 hour
    'max_login_attempts': 5,
    'password_min_length': 8,
    'cors_origins': ['http://localhost:3000', 'http://127.0.0.1:3000'],
    'rate_limit': {
        'requests_per_minute': 60,
        'requests_per_hour': 1000
    }
}

# WebSocket Configuration
WEBSOCKET_CONFIG = {
    'ping_timeout': 60,
    'ping_interval': 25,
    'cors_allowed_origins': "*",
    'async_mode': 'threading'
}

# Cache Configuration
CACHE_CONFIG = {
    'type': 'simple',
    'default_timeout': 300,  # 5 minutes
    'key_prefix': 'telco_platform_'
}
