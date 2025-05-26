#!/usr/bin/env python3
"""
Database setup script for Telco NoSQL Platform
Usage: python scripts/setup_databases.py
"""

import sys
import os
import argparse
import logging

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from database.cassandra_manager import CassandraManager
from database.mongodb_manager import MongoManager
from config.database_config import CASSANDRA_CONFIG, MONGODB_CONFIG

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def setup_cassandra():
    """Setup Cassandra database"""
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Setting up Cassandra...")
        
        # Initialize Cassandra manager
        cassandra_manager = CassandraManager(**CASSANDRA_CONFIG)
        
        # Connect to Cassandra
        if not cassandra_manager.connect():
            raise Exception("Failed to connect to Cassandra")
        
        # Setup keyspace
        logger.info("Creating keyspace...")
        cassandra_manager.setup_keyspace()
        
        # Create tables
        logger.info("Creating tables...")
        cassandra_manager.create_tables()
        
        # Create indexes
        logger.info("Creating indexes...")
        cassandra_manager.create_indexes()
        
        logger.info("✅ Cassandra setup completed successfully")
        return cassandra_manager
        
    except Exception as e:
        logger.error(f"❌ Cassandra setup failed: {e}")
        return None

def setup_mongodb():
    """Setup MongoDB database"""
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Setting up MongoDB...")
        
        # Initialize MongoDB manager
        mongo_manager = MongoManager(**MONGODB_CONFIG)
        
        # Connect to MongoDB
        if not mongo_manager.connect():
            raise Exception("Failed to connect to MongoDB")
        
        # Create collections and indexes
        logger.info("Creating collections and indexes...")
        mongo_manager.create_collections_and_indexes()
        
        logger.info("✅ MongoDB setup completed successfully")
        return mongo_manager
        
    except Exception as e:
        logger.error(f"❌ MongoDB setup failed: {e}")
        return None

def verify_setup(cassandra_manager, mongo_manager):
    """Verify database setup"""
    logger = logging.getLogger(__name__)
    
    logger.info("Verifying database setup...")
    
    # Verify Cassandra
    if cassandra_manager:
        try:
            # Test query
            result = cassandra_manager.session.execute("SELECT COUNT(*) FROM call_records")
            logger.info("✅ Cassandra verification passed")
        except Exception as e:
            logger.warning(f"⚠️ Cassandra verification failed: {e}")
    
    # Verify MongoDB
    if mongo_manager:
        try:
            # Test collection access
            count = mongo_manager.get_collection_count('customers')
            logger.info("✅ MongoDB verification passed")
        except Exception as e:
            logger.warning(f"⚠️ MongoDB verification failed: {e}")

def main():
    parser = argparse.ArgumentParser(description='Setup Telco NoSQL Platform databases')
    parser.add_argument('--cassandra-only', action='store_true', help='Setup only Cassandra')
    parser.add_argument('--
