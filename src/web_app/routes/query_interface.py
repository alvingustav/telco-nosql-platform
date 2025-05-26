from flask import Blueprint, render_template, jsonify, request
from datetime import datetime, timedelta
import logging

query_bp = Blueprint('query', __name__)
logger = logging.getLogger(__name__)

@query_bp.route('/query-interface')
def query_interface():
    """Query interface page"""
    try:
        return render_template('query_interface.html')
    except Exception as e:
        logger.error(f"Query interface error: {e}")
        return render_template('error.html', error=str(e)), 500

@query_bp.route('/api/query/execute', methods=['POST'])
def api_execute_query():
    """Execute query via API"""
    try:
        query_type = request.json.get('query_type')
        parameters = request.json.get('parameters', {})
        
        from src.web_app.app import query_aggregator
        
        if not query_aggregator:
            return jsonify({
                'status': 'error',
                'message': 'Query aggregator not initialized'
            }), 400
        
        # Execute based on query type
        if query_type == 'call_analytics':
            result = execute_call_analytics_query(parameters)
        elif query_type == 'customer_insights':
            result = execute_customer_insights_query(parameters)
        elif query_type == 'combined_behavior':
            result = execute_combined_query(parameters)
        else:
            return jsonify({
                'status': 'error',
                'message': 'Invalid query type'
            }), 400
        
        return jsonify({
            'status': 'success',
            'result': result
        })
        
    except Exception as e:
        logger.error(f"Query execution error: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@query_bp.route('/api/query/templates')
def api_query_templates():
    """Get query templates"""
    templates = {
        'call_analytics': {
            'name': 'Call Analytics',
            'description': 'Analyze call volume and patterns',
            'parameters': [
                {'name': 'start_date', 'type': 'date', 'required': True},
                {'name': 'end_date', 'type': 'date', 'required': True},
                {'name': 'call_type', 'type': 'select', 'options': ['voice', 'video', 'conference'], 'required': False}
            ]
        },
        'customer_insights': {
            'name': 'Customer Insights',
            'description': 'Customer segmentation analysis',
            'parameters': [
                {'name': 'segment', 'type': 'select', 'options': ['basic', 'premium', 'enterprise'], 'required': False},
                {'name': 'plan_type', 'type': 'select', 'options': ['prepaid', 'postpaid'], 'required': False}
            ]
        },
        'combined_behavior': {
            'name': 'Combined Customer Behavior',
            'description': 'Combined analysis from both databases',
            'parameters': [
                {'name': 'month', 'type': 'month', 'required': True},
                {'name': 'limit', 'type': 'number', 'default': 50, 'required': False}
            ]
        }
    }
    
    return jsonify({
        'status': 'success',
        'templates': templates
    })

@query_bp.route('/api/query/history')
def api_query_history():
    """Get query execution history"""
    try:
        from src.web_app.app import performance_monitor
        
        history = performance_monitor.get_recent_queries(20)
        
        return jsonify({
            'status': 'success',
            'history': history
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

def execute_call_analytics_query(parameters):
    """Execute call analytics query"""
    from src.web_app.app import query_aggregator
    
    start_date = datetime.fromisoformat(parameters.get('start_date'))
    end_date = datetime.fromisoformat(parameters.get('end_date'))
    call_type = parameters.get('call_type')
    
    return query_aggregator.query_db1_call_analytics(start_date, end_date, call_type)

def execute_customer_insights_query(parameters):
    """Execute customer insights query"""
    from src.web_app.app import query_aggregator
    
    segment = parameters.get('segment')
    plan_type = parameters.get('plan_type')
    
    return query_aggregator.query_db2_customer_insights(segment, plan_type)

def execute_combined_query(parameters):
    """Execute combined query"""
    from src.web_app.app import query_aggregator
    
    month = parameters.get('month')
    limit = parameters.get('limit', 50)
    
    return query_aggregator.query_combined_customer_behavior(month, limit)
