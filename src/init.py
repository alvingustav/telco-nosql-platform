"""
Telco NoSQL Platform - Main Package
"""

__version__ = "1.0.0"
__author__ = "ROBD Project Team"
__description__ = "Telco NoSQL Platform with Cassandra and MongoDB"

# Package imports
from . import database
from . import data_generation
from . import web_app
from . import utils

__all__ = [
    'database',
    'data_generation', 
    'web_app',
    'utils'
]
