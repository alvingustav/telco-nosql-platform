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
        
        total_records = sum(cassandra_results.values()) +
