from flask import Blueprint, render_template, jsonify, request
from datetime import datetime, timedelta
import logging

dashboard_bp = Blueprint('dashboard', __name__)
logger = logging.getLogger(__name__)

@dashboard_bp.route('/')
def index():
    """Main dashboard page"""
    try:
        # Get system status
        system_status = get_system_status()
        
        # Get recent activity
        recent_activity = get_recent_activity()
        
        # Get performance metrics
        performance_metrics = get_performance_metrics()
        
        return render_template('dashboard.html',
                             system_status=system_status,
                             recent_activity=recent_activity,
                             performance_metrics=performance_metrics)
    except Exception as e:
        logger.error(f"Dashboard error: {e}")
        return render_template('error.html', error=str(e)), 500

@dashboard_bp.route('/api/system-status')
def api_system_status():
    """API endpoint for system status"""
    try:
        status = get_system_status()
        return jsonify({
            'status': 'success',
            'data': status
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@dashboard_bp.route('/api/performance-summary')
def api_performance_summary():
    """API endpoint for performance summary"""
    try:
        summary = get_performance_summary()
        return jsonify({
            'status': 'success',
            'data': summary
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

def get_system_status():
    """Get current system status"""
    from src.web_app.app import cassandra_manager, mongo_manager
    
    return {
        'cassandra': {
            'connected': cassandra_manager.session is not None if cassandra_manager else False,
            'status': 'online' if cassandra_manager and cassandra_manager.session else 'offline'
        },
        'mongodb': {
            'connected': mongo_manager.db is not None if mongo_manager else False,
            'status': 'online' if mongo_manager and mongo_manager.db else 'offline'
        },
        'timestamp': datetime.now().isoformat(),
        'uptime': calculate_uptime()
    }

def get_recent_activity():
    """Get recent system activity"""
    return [
        {
            'timestamp': datetime.now() - timedelta(minutes=5),
            'action': 'Query executed',
            'details': 'Call analytics query completed',
            'status': 'success'
        },
        {
            'timestamp': datetime.now() - timedelta(minutes=15),
            'action': 'Data loaded',
            'details': '10,000 CDR records loaded',
            'status': 'success'
        },
        {
            'timestamp': datetime.now() - timedelta(minutes=30),
            'action': 'Index created',
            'details': 'Cassandra indexes created',
            'status': 'success'
        }
    ]

def get_performance_metrics():
    """Get performance metrics"""
    return {
        'avg_query_time': 0.245,
        'total_queries': 1247,
        'cache_hit_rate': 85.6,
        'index_efficiency': 92.3
    }

def get_performance_summary():
    """Get performance summary for API"""
    return {
        'queries_today': 156,
        'avg_response_time': 0.234,
        'error_rate': 0.02,
        'throughput': 45.6
    }

def calculate_uptime():
    """Calculate system uptime"""
    # Simple implementation - in production, track actual start time
    return "2h 45m"
