from flask import Blueprint, render_template, jsonify, request
from datetime import datetime, timedelta
import logging

analytics_bp = Blueprint('analytics', __name__)
logger = logging.getLogger(__name__)

@analytics_bp.route('/analytics')
def analytics():
    """Analytics dashboard page"""
    try:
        return render_template('analytics.html')
    except Exception as e:
        logger.error(f"Analytics error: {e}")
        return render_template('error.html', error=str(e)), 500

@analytics_bp.route('/performance-comparison')
def performance_comparison():
    """Performance comparison page"""
    try:
        return render_template('performance_comparison.html')
    except Exception as e:
        logger.error(f"Performance comparison error: {e}")
        return render_template('error.html', error=str(e)), 500

@analytics_bp.route('/api/analytics/call-volume')
def api_call_volume():
    """Get call volume analytics"""
    try:
        # Get call volume data for charts
        data = get_call_volume_data()
        
        return jsonify({
            'status': 'success',
            'data': data
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@analytics_bp.route('/api/analytics/customer-segments')
def api_customer_segments():
    """Get customer segmentation data"""
    try:
        data = get_customer_segment_data()
        
        return jsonify({
            'status': 'success',
            'data': data
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@analytics_bp.route('/api/analytics/revenue-trends')
def api_revenue_trends():
    """Get revenue trend data"""
    try:
        data = get_revenue_trend_data()
        
        return jsonify({
            'status': 'success',
            'data': data
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@analytics_bp.route('/api/performance/comparison', methods=['POST'])
def api_performance_comparison():
    """Run performance comparison"""
    try:
        from src.web_app.app import query_aggregator
        
        if not query_aggregator:
            return jsonify({
                'status': 'error',
                'message': 'Query aggregator not initialized'
            }), 400
        
        results = query_aggregator.performance_comparison()
        
        return jsonify({
            'status': 'success',
            'results': results
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

def get_call_volume_data():
    """Get call volume analytics data"""
    # Sample data - in production, query from database
    return {
        'labels': ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun'],
        'datasets': [
            {
                'label': 'Voice Calls',
                'data': [12000, 15000, 13000, 17000, 16000, 18000],
                'backgroundColor': 'rgba(54, 162, 235, 0.6)'
            },
            {
                'label': 'Video Calls',
                'data': [8000, 9000, 7500, 11000, 12000, 13500],
                'backgroundColor': 'rgba(255, 99, 132, 0.6)'
            }
        ]
    }

def get_customer_segment_data():
    """Get customer segmentation data"""
    return {
        'labels': ['Basic', 'Premium', 'Enterprise'],
        'datasets': [{
            'data': [45, 35, 20],
            'backgroundColor': [
                'rgba(255, 99, 132, 0.6)',
                'rgba(54, 162, 235, 0.6)',
                'rgba(255, 205, 86, 0.6)'
            ]
        }]
    }

def get_revenue_trend_data():
    """Get revenue trend data"""
    return {
        'labels': ['Q1', 'Q2', 'Q3', 'Q4'],
        'datasets': [{
            'label': 'Revenue (Million IDR)',
            'data': [1250, 1380, 1420, 1650],
            'borderColor': 'rgba(75, 192, 192, 1)',
            'backgroundColor': 'rgba(75, 192, 192, 0.2)',
            'fill': True
        }]
    }
