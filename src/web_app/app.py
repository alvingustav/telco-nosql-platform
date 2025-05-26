from flask import Flask, render_template, request, jsonify, flash, redirect, url_for
from flask_socketio import SocketIO, emit
import asyncio
import threading
import time
import logging
from datetime import datetime, timedelta
import json
import os

from src.database.cassandra_manager import CassandraManager
from src.database.mongodb_manager import MongoManager
from src.database.query_aggregator import QueryAggregator
from src.data_generation.data_loader import TelcoDataLoader
from src.utils.performance_monitor import PerformanceMonitor
from config.database_config import CASSANDRA_CONFIG, MONGODB_CONFIG, APP_CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

app = Flask(__name__)
app.secret_key = APP_CONFIG['secret_key']
socketio = SocketIO(app, cors_allowed_origins="*")

# Global instances
cassandra_manager = None
mongo_manager = None
query_aggregator = None
performance_monitor = PerformanceMonitor()

@app.route('/')
def dashboard():
    """Main dashboard with system overview"""
    system_status = get_system_status()
    recent_queries = get_recent_queries()
    performance_metrics = performance_monitor.get_latest_metrics()
    data_summary = get_data_summary()
    
    return render_template('dashboard.html', 
                         system_status=system_status,
                         recent_queries=recent_queries,
                         performance_metrics=performance_metrics,
                         data_summary=data_summary)

@app.route('/database-status')
def database_status():
    """Database connection and schema status"""
    cassandra_status = check_cassandra_status()
    mongodb_status = check_mongodb_status()
    
    return render_template('database_status.html',
                         cassandra_status=cassandra_status,
                         mongodb_status=mongodb_status)

@app.route('/query-interface')
def query_interface():
    """Interactive query interface"""
    return render_template('query_interface.html')

@app.route('/analytics')
def analytics():
    """Analytics dashboard with visualizations"""
    return render_template('analytics.html')

@app.route('/performance-comparison')
def performance_comparison():
    """Performance comparison with and without indexes"""
    return render_template('performance_comparison.html')

# API Routes
@app.route('/api/setup-databases', methods=['POST'])
def setup_databases():
    """Setup database connections and schemas"""
    try:
        global cassandra_manager, mongo_manager, query_aggregator
        
        emit_progress("Initializing database connections...", 10)
        
        # Initialize Cassandra
        cassandra_manager = CassandraManager(**CASSANDRA_CONFIG)
        if not cassandra_manager.connect():
            raise Exception("Failed to connect to Cassandra")
        
        emit_progress("Setting up Cassandra keyspace and tables...", 30)
        cassandra_manager.setup_keyspace()
        cassandra_manager.create_tables()
        
        emit_progress("Initializing MongoDB connection...", 50)
        
        # Initialize MongoDB
        mongo_manager = MongoManager(**MONGODB_CONFIG)
        if not mongo_manager.connect():
            raise Exception("Failed to connect to MongoDB")
        
        emit_progress("Setting up MongoDB collections and indexes...", 70)
        mongo_manager.create_collections_and_indexes()
        
        emit_progress("Initializing Query Aggregator...", 90)
        
        # Initialize query aggregator
        query_aggregator = QueryAggregator(cassandra_manager, mongo_manager)
        
        emit_progress("Database setup completed!", 100)
        
        return jsonify({
            'status': 'success',
            'message': 'Databases setup successfully',
            'details': {
                'cassandra_keyspace': CASSANDRA_CONFIG['keyspace'],
                'mongodb_database': MONGODB_CONFIG['database']
            }
        })
        
    except Exception as e:
        app.logger.error(f"Database setup failed: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/api/load-existing-data', methods=['POST'])
def load_existing_data():
    """Load data from existing JSON files"""
    try:
        data_directory = request.json.get('data_directory', 'telco_data_export')
        
        # Check if databases are initialized
        if not cassandra_manager or not mongo_manager:
            return jsonify({
                'status': 'error',
                'message': 'Databases not initialized. Please setup databases first.'
            }), 400
        
        # Initialize data loader
        data_loader = TelcoDataLoader(data_directory)
        
        # Verify data directory first
        emit_progress("Verifying data directory...", 5)
        verification = data_loader.verify_data_directory()
        missing_files = [f for f, exists in verification.items() if not exists]
        
        if missing_files:
            return jsonify({
                'status': 'error',
                'message': f'Missing required files: {", ".join(missing_files)}',
                'verification': verification
            }), 400
        
        # Load data to Cassandra
        emit_progress("Loading data to Cassandra...", 20)
        cassandra_results = data_loader.load_cassandra_data(cassandra_manager)
        
        emit_progress("Loading data to MongoDB...", 60)
        # Load data to MongoDB
        mongodb_results = data_loader.load_mongodb_data(mongo_manager)
        
        emit_progress("Data loading completed!", 100)
        
                total_records = sum(cassandra_results.values()) + sum(mongodb_results.values())
        
        return jsonify({
            'status': 'success',
            'message': f'Successfully loaded {total_records:,} records from existing data',
            'cassandra_results': cassandra_results,
            'mongodb_results': mongodb_results,
            'total_records': total_records
        })
        
    except Exception as e:
        app.logger.error(f"Data loading failed: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/api/verify-data-directory', methods=['POST'])
def verify_data_directory():
    """Verify data directory and get summary"""
    try:
        data_directory = request.json.get('data_directory', 'telco_data_export')
        data_loader = TelcoDataLoader(data_directory)
        
        summary = data_loader.get_data_summary()
        
        return jsonify({
            'status': 'success',
            'summary': summary
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/api/execute-query', methods=['POST'])
def execute_query():
    """Execute queries and return results"""
    try:
        query_type = request.json.get('query_type')
        parameters = request.json.get('parameters', {})
        
        if not query_aggregator:
            return jsonify({
                'status': 'error',
                'message': 'Query aggregator not initialized'
            }), 400
        
        start_time = time.time()
        
        if query_type == 'call_analytics':
            start_date = datetime.fromisoformat(parameters.get('start_date'))
            end_date = datetime.fromisoformat(parameters.get('end_date'))
            call_type = parameters.get('call_type')
            
            result = query_aggregator.query_db1_call_analytics(start_date, end_date, call_type)
            
        elif query_type == 'customer_insights':
            segment = parameters.get('segment')
            plan_type = parameters.get('plan_type')
            
            result = query_aggregator.query_db2_customer_insights(segment, plan_type)
            
        elif query_type == 'combined_behavior':
            month = parameters.get('month')
            limit = parameters.get('limit', 50)
            
            result = query_aggregator.query_combined_customer_behavior(month, limit)
            
        else:
            return jsonify({
                'status': 'error',
                'message': 'Invalid query type'
            }), 400
        
        execution_time = time.time() - start_time
        
        # Log performance
        performance_monitor.log_query_performance(query_type, execution_time)
        
        return jsonify({
            'status': 'success',
            'result': result,
            'execution_time': execution_time
        })
        
    except Exception as e:
        app.logger.error(f"Query execution failed: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/api/performance-test', methods=['POST'])
def performance_test():
    """Run performance tests with and without indexes"""
    try:
        if not query_aggregator:
            return jsonify({
                'status': 'error',
                'message': 'Database not initialized'
            }), 400
        
        emit_progress("Starting performance comparison...", 10)
        
        # Run performance comparison
        results = query_aggregator.performance_comparison()
        
        emit_progress("Performance comparison completed!", 100)
        
        return jsonify({
            'status': 'success',
            'results': results
        })
        
    except Exception as e:
        app.logger.error(f"Performance test failed: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/api/create-indexes', methods=['POST'])
def create_indexes():
    """Create indexes on databases"""
    try:
        database = request.json.get('database', 'both')
        
        if database == 'cassandra' and cassandra_manager:
            cassandra_manager.create_indexes()
        elif database == 'mongodb' and mongo_manager:
            mongo_manager.create_collections_and_indexes()
        elif database == 'both':
            if cassandra_manager:
                cassandra_manager.create_indexes()
            if mongo_manager:
                mongo_manager.create_collections_and_indexes()
        
        return jsonify({
            'status': 'success',
            'message': f'Indexes created for {database}'
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/api/drop-indexes', methods=['POST'])
def drop_indexes():
    """Drop indexes from databases"""
    try:
        database = request.json.get('database', 'both')
        
        if database == 'cassandra' and cassandra_manager:
            cassandra_manager.drop_indexes()
        elif database == 'mongodb' and mongo_manager:
            for collection in ['customers', 'subscriptions', 'billing', 'customer_support']:
                mongo_manager.drop_indexes(collection)
        elif database == 'both':
            if cassandra_manager:
                cassandra_manager.drop_indexes()
            if mongo_manager:
                for collection in ['customers', 'subscriptions', 'billing', 'customer_support']:
                    mongo_manager.drop_indexes(collection)
        
        return jsonify({
            'status': 'success',
            'message': f'Indexes dropped for {database}'
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

# WebSocket events for real-time updates
@socketio.on('connect')
def handle_connect():
    emit('connected', {'message': 'Connected to Telco NoSQL Platform'})

@socketio.on('request_system_status')
def handle_system_status():
    status = get_system_status()
    emit('system_status_update', status)

def emit_progress(message, progress):
    """Emit progress updates via WebSocket"""
    socketio.emit('loading_progress', {
        'message': message,
        'progress': progress
    })

def get_system_status():
    """Get current system status"""
    return {
        'cassandra_connected': cassandra_manager.session is not None if cassandra_manager else False,
        'mongodb_connected': mongo_manager.db is not None if mongo_manager else False,
        'timestamp': datetime.now().isoformat(),
        'uptime': get_uptime()
    }

def check_cassandra_status():
    """Check Cassandra connection and schema status"""
    if not cassandra_manager:
        return {'connected': False, 'tables': [], 'indexes': []}
    
    try:
        # Get table counts
        table_counts = {}
        for table in ['call_records', 'sms_records', 'data_usage']:
            table_counts[table] = cassandra_manager.get_table_count(table)
        
        return {
            'connected': True,
            'keyspace': cassandra_manager.keyspace,
            'tables': ['call_records', 'sms_records', 'data_usage'],
            'table_counts': table_counts,
            'indexes': ['caller_id_idx', 'call_start_time_idx', 'sender_id_idx']
        }
    except Exception as e:
        return {'connected': False, 'error': str(e)}

def check_mongodb_status():
    """Check MongoDB connection and collections status"""
    if not mongo_manager:
        return {'connected': False, 'collections': [], 'indexes': []}
    
    try:
        # Get collection counts
        collection_counts = {}
        for collection in ['customers', 'subscriptions', 'billing', 'customer_support']:
            collection_counts[collection] = mongo_manager.get_collection_count(collection)
        
        return {
            'connected': True,
            'database': mongo_manager.database_name,
            'collections': ['customers', 'subscriptions', 'billing', 'customer_support'],
            'collection_counts': collection_counts,
            'indexes': ['customer_id_idx', 'phone_number_idx', 'registration_date_idx']
        }
    except Exception as e:
        return {'connected': False, 'error': str(e)}

def get_recent_queries():
    """Get recent query history"""
    return performance_monitor.get_recent_queries()

def get_data_summary():
    """Get data summary from export directory"""
    try:
        data_loader = TelcoDataLoader()
        return data_loader.get_data_summary()
    except:
        return {}

def get_uptime():
    """Calculate application uptime"""
    # Simple uptime calculation
    return "Running"

if __name__ == '__main__':
    import eventlet
    eventlet.monkey_patch()
    socketio.run(app, debug=APP_CONFIG['debug'], host=APP_CONFIG['host'], port=APP_CONFIG['port'])
