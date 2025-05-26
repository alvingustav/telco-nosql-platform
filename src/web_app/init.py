"""
Web application module for Telco NoSQL Platform
"""

from .app import app, socketio

__all__ = [
    'app',
    'socketio'
]
