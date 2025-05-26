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
    parser.add_argument('--mongodb-only', action='store_true', help='Setup only MongoDB')
    parser.add_argument('--verify-only', action='store_true', help='Only verify setup')
    parser.add_argument('--drop-existing', action='store_true', help='Drop existing databases')
    
    args = parser.parse_args()
    
    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("🚀 Starting Telco NoSQL Platform Database Setup")
    logger.info("=" * 60)
    
    cassandra_manager = None
    mongo_manager = None
    
    try:
        if args.verify_only:
            logger.info("Verification mode - checking existing setup...")
            # Quick verification without setup
            if not args.mongodb_only:
                cassandra_manager = CassandraManager(**CASSANDRA_CONFIG)
                if cassandra_manager.connect():
                    logger.info("✅ Cassandra connection verified")
                else:
                    logger.error("❌ Cassandra connection failed")
            
            if not args.cassandra_only:
                mongo_manager = MongoManager(**MONGODB_CONFIG)
                if mongo_manager.connect():
                    logger.info("✅ MongoDB connection verified")
                else:
                    logger.error("❌ MongoDB connection failed")
            
            return 0
        
        # Drop existing databases if requested
        if args.drop_existing:
            logger.warning("⚠️ Dropping existing databases...")
            if not args.mongodb_only:
                try:
                    cassandra_manager = CassandraManager(**CASSANDRA_CONFIG)
                    if cassandra_manager.connect():
                        cassandra_manager.session.execute(f"DROP KEYSPACE IF EXISTS {CASSANDRA_CONFIG['keyspace']}")
                        logger.info("✅ Cassandra keyspace dropped")
                except Exception as e:
                    logger.warning(f"⚠️ Could not drop Cassandra keyspace: {e}")
            
            if not args.cassandra_only:
                try:
                    mongo_manager = MongoManager(**MONGODB_CONFIG)
                    if mongo_manager.connect():
                        mongo_manager.client.drop_database(MONGODB_CONFIG['database'])
                        logger.info("✅ MongoDB database dropped")
                except Exception as e:
                    logger.warning(f"⚠️ Could not drop MongoDB database: {e}")
        
        # Setup databases
        if not args.mongodb_only:
            cassandra_manager = setup_cassandra()
        
        if not args.cassandra_only:
            mongo_manager = setup_mongodb()
        
        # Verify setup
        if cassandra_manager or mongo_manager:
            verify_setup(cassandra_manager, mongo_manager)
            logger.info("✅ Database setup completed successfully!")
            logger.info("=" * 60)
            
            # Print connection details
            if cassandra_manager:
                logger.info(f"📊 Cassandra: {CASSANDRA_CONFIG['hosts'][0]}:{CASSANDRA_CONFIG['port']}")
                logger.info(f"🔑 Keyspace: {CASSANDRA_CONFIG['keyspace']}")
            
            if mongo_manager:
                logger.info(f"📊 MongoDB: {MONGODB_CONFIG['uri']}")
                logger.info(f"🔑 Database: {MONGODB_CONFIG['database']}")
            
            logger.info("=" * 60)
            logger.info("🎉 Ready to load data and run queries!")
            return 0
        else:
            logger.error("❌ Database setup failed")
            return 1
            
    except KeyboardInterrupt:
        logger.info("\n⚠️ Setup interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"💥 Unexpected error: {e}")
        return 1
    finally:
        # Cleanup connections
        if cassandra_manager and cassandra_manager.cluster:
            cassandra_manager.close()
        if mongo_manager and mongo_manager.client:
            mongo_manager.close()

if __name__ == '__main__':
    sys.exit(main())
