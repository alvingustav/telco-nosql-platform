"""
Route modules for Telco NoSQL Platform web application
"""

from . import dashboard
from . import database_management
from . import query_interface
from . import analytics

__all__ = [
    'dashboard',
    'database_management',
    'query_interface', 
    'analytics'
]
