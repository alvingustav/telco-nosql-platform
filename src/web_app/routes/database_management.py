from flask import Blueprint, render_template, jsonify, request
import logging

database_bp = Blueprint('database', __name__)
logger = logging.getLogger(__name__)

@database_bp.route('/database-status')
def database_status():
    """Database status page"""
    try:
        cassandra_status = get_cassandra_status()
        mongodb_status = get_mongodb_status()
        
        return render_template('database_status.html',
                             cassandra_status=cassandra_status,
                             mongodb_status=mongodb_status)
    except Exception as e:
        logger.error(f"Database status error: {e}")
        return render_template('error.html', error=str(e)), 500

@database_bp.route('/api/database/cassandra/status')
def api_cassandra_status():
    """API endpoint for Cassandra status"""
    try:
        status = get_cassandra_status()
        return jsonify({
            'status': 'success',
            'data': status
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@database_bp.route('/api/database/mongodb/status')
def api_mongodb_status():
    """API endpoint for MongoDB status"""
    try:
        status = get_mongodb_status()
        return jsonify({
            'status': 'success',
            'data': status
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@database_bp.route('/api/database/setup', methods=['POST'])
def api_setup_databases():
    """Setup databases via API"""
    try:
        from src.web_app.app import setup_databases
        result = setup_databases()
        return jsonify(result)
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@database_bp.route('/api/database/indexes/create', methods=['POST'])
def api_create_indexes():
    """Create database indexes"""
    try:
        database = request.json.get('database', 'both')
        from src.web_app.app import create_indexes
        result = create_indexes(database)
        return jsonify(result)
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@database_bp.route('/api/database/indexes/drop', methods=['POST'])
def api_drop_indexes():
    """Drop database indexes"""
    try:
        database = request.json.get('database', 'both')
        from src.web_app.app import drop_indexes
        result = drop_indexes(database)
        return jsonify(result)
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

def get_cassandra_status():
    """Get Cassandra database status"""
    from src.web_app.app import cassandra_manager
    
    if not cassandra_manager:
        return {
            'connected': False,
            'keyspace': None,
            'tables': [],
            'record_counts': {}
        }
    
    try:
        tables = ['call_records', 'sms_records', 'data_usage']
        record_counts = {}
        
        for table in tables:
            try:
                count = cassandra_manager.get_table_count(table)
                record_counts[table] = count
            except:
                record_counts[table] = 0
        
        return {
            'connected': True,
            'keyspace': cassandra_manager.keyspace,
            'tables': tables,
            'record_counts': record_counts,
            'indexes': get_cassandra_indexes()
        }
    except Exception as e:
        logger.error(f"Cassandra status error: {e}")
        return {
            'connected': False,
            'error': str(e)
        }

def get_mongodb_status():
    """Get MongoDB database status"""
    from src.web_app.app import mongo_manager
    
    if not mongo_manager:
        return {
            'connected': False,
            'database': None,
            'collections': [],
            'record_counts': {}
        }
    
    try:
        collections = ['customers', 'subscriptions', 'billing', 'customer_support']
        record_counts = {}
        
        for collection in collections:
            try:
                count = mongo_manager.get_collection_count(collection)
                record_counts[collection] = count
            except:
                record_counts[collection] = 0
        
        return {
            'connected': True,
            'database': mongo_manager.database_name,
            'collections': collections,
            'record_counts': record_counts,
            'indexes': get_mongodb_indexes()
        }
    except Exception as e:
        logger.error(f"MongoDB status error: {e}")
        return {
            'connected': False,
            'error': str(e)
        }

def get_cassandra_indexes():
    """Get Cassandra index information"""
    return [
        'call_records_caller_idx',
        'call_records_start_time_idx',
        'sms_records_sender_idx',
        'data_usage_customer_idx'
    ]

def get_mongodb_indexes():
    """Get MongoDB index information"""
    return [
        'customers_customer_id_idx',
        'customers_phone_number_idx',
        'subscriptions_customer_id_idx',
        'billing_customer_id_idx'
    ]
