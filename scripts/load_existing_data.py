#!/usr/bin/env python3
"""
Data loading script for Telco NoSQL Platform
Usage: python scripts/load_existing_data.py --data-dir telco_data_export
"""

import sys
import os
import argparse
import logging
import time
from datetime import datetime

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from database.cassandra_manager import CassandraManager
from database.mongodb_manager import MongoManager
from data_generation.data_loader import TelcoDataLoader
from config.database_config import CASSANDRA_CONFIG, MONGODB_CONFIG

def setup_logging(verbose=False):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def load_data_with_progress(data_loader, cassandra_manager, mongo_manager, batch_mode=False):
    """Load data with progress tracking"""
    logger = logging.getLogger(__name__)
    
    start_time = time.time()
    results = {
        'cassandra': {},
        'mongodb': {},
        'total_time': 0,
        'total_records': 0
    }
    
    try:
        # Verify data directory first
        logger.info("📋 Verifying data directory...")
        summary = data_loader.get_data_summary()
        
        logger.info(f"📁 Data directory: {summary['directory']}")
        logger.info(f"📊 Files found: {summary['files_found']}/{summary['total_files']}")
        
        missing_files = [f for f, exists in summary['file_status'].items() if not exists]
        if missing_files:
            logger.error(f"❌ Missing required files: {missing_files}")
            return None
        
        # Display estimated records
        total_estimated = sum(summary['estimated_records'].values())
        logger.info(f"📈 Estimated total records: {total_estimated:,}")
        
        if not batch_mode:
            response = input("Continue with data loading? (y/N): ")
            if response.lower() != 'y':
                logger.info("Data loading cancelled by user")
                return None
        
        # Load Cassandra data
        logger.info("🔄 Loading data to Cassandra...")
        cassandra_start = time.time()
        
        if cassandra_manager:
            cassandra_results = data_loader.load_cassandra_data(cassandra_manager)
            results['cassandra'] = cassandra_results
            cassandra_time = time.time() - cassandra_start
            
            total_cassandra = sum(cassandra_results.values())
            logger.info(f"✅ Cassandra loading completed in {cassandra_time:.2f}s")
            logger.info(f"📊 Total Cassandra records: {total_cassandra:,}")
        else:
            logger.warning("⚠️ Cassandra manager not available")
        
        # Load MongoDB data
        logger.info("🔄 Loading data to MongoDB...")
        mongodb_start = time.time()
        
        if mongo_manager:
            mongodb_results = data_loader.load_mongodb_data(mongo_manager)
            results['mongodb'] = mongodb_results
            mongodb_time = time.time() - mongodb_start
            
            total_mongodb = sum(mongodb_results.values())
            logger.info(f"✅ MongoDB loading completed in {mongodb_time:.2f}s")
            logger.info(f"📊 Total MongoDB records: {total_mongodb:,}")
        else:
            logger.warning("⚠️ MongoDB manager not available")
        
        # Calculate totals
        total_time = time.time() - start_time
        total_records = sum(results['cassandra'].values()) + sum(results['mongodb'].values())
        
        results['total_time'] = total_time
        results['total_records'] = total_records
        
        # Performance metrics
        records_per_second = total_records / total_time if total_time > 0 else 0
        
        logger.info("=" * 60)
        logger.info("🎉 DATA LOADING COMPLETED")
        logger.info("=" * 60)
        logger.info(f"📊 Total records loaded: {total_records:,}")
        logger.info(f"⏱️  Total time: {total_time:.2f} seconds")
        logger.info(f"🚀 Loading rate: {records_per_second:.2f} records/second")
        logger.info("=" * 60)
        
        return results
        
    except Exception as e:
        logger.error(f"💥 Data loading failed: {e}")
        return None

def verify_loaded_data(cassandra_manager, mongo_manager):
    """Verify that data was loaded correctly"""
    logger = logging.getLogger(__name__)
    
    logger.info("🔍 Verifying loaded data...")
    verification_results = {}
    
    # Verify Cassandra data
    if cassandra_manager:
        try:
            cassandra_counts = {}
            tables = ['call_records', 'sms_records', 'data_usage']
            
            for table in tables:
                count = cassandra_manager.get_table_count(table)
                cassandra_counts[table] = count
                logger.info(f"✅ {table}: {count:,} records")
            
            verification_results['cassandra'] = cassandra_counts
        except Exception as e:
            logger.error(f"❌ Cassandra verification failed: {e}")
            verification_results['cassandra'] = {'error': str(e)}
    
    # Verify MongoDB data
    if mongo_manager:
        try:
            mongodb_counts = {}
            collections = ['customers', 'subscriptions', 'billing', 'customer_support']
            
            for collection in collections:
                count = mongo_manager.get_collection_count(collection)
                mongodb_counts[collection] = count
                logger.info(f"✅ {collection}: {count:,} documents")
            
            verification_results['mongodb'] = mongodb_counts
        except Exception as e:
            logger.error(f"❌ MongoDB verification failed: {e}")
            verification_results['mongodb'] = {'error': str(e)}
    
    return verification_results

def create_sample_queries(cassandra_manager, mongo_manager):
    """Create and test sample queries"""
    logger = logging.getLogger(__name__)
    
    logger.info("🧪 Testing sample queries...")
    
    # Test Cassandra query
    if cassandra_manager:
        try:
            query = "SELECT COUNT(*) FROM call_records WHERE call_type = 'voice' ALLOW FILTERING"
            start_time = time.time()
            result = cassandra_manager.execute_query(query)
            query_time = time.time() - start_time
            
            if result:
                count = result[0]['count']
                logger.info(f"✅ Cassandra test query: {count:,} voice calls ({query_time:.3f}s)")
            else:
                logger.warning("⚠️ Cassandra test query returned no results")
                
        except Exception as e:
            logger.error(f"❌ Cassandra test query failed: {e}")
    
    # Test MongoDB query
    if mongo_manager:
        try:
            start_time = time.time()
            pipeline = [
                {"$match": {"customer_segment": "premium"}},
                {"$count": "total"}
            ]
            result = list(mongo_manager.execute_aggregation('customers', pipeline))
            query_time = time.time() - start_time
            
            if result:
                count = result[0]['total']
                logger.info(f"✅ MongoDB test query: {count:,} premium customers ({query_time:.3f}s)")
            else:
                logger.warning("⚠️ MongoDB test query returned no results")
                
        except Exception as e:
            logger.error(f"❌ MongoDB test query failed: {e}")

def main():
    parser = argparse.ArgumentParser(description='Load existing JSON data into Telco platform')
    parser.add_argument('--data-dir', default='telco_data_export', 
                       help='Directory containing JSON data files')
    parser.add_argument('--cassandra-only', action='store_true',
                       help='Load only to Cassandra')
    parser.add_argument('--mongodb-only', action='store_true',
                       help='Load only to MongoDB')
    parser.add_argument('--verify-only', action='store_true',
                       help='Only verify existing data')
    parser.add_argument('--batch', action='store_true',
                       help='Run in batch mode (no user prompts)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    parser.add_argument('--test-queries', action='store_true',
                       help='Run test queries after loading')
    
    args = parser.parse_args()
    
    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)
    
    logger.info("🚀 Starting Telco Data Loading Process")
    logger.info("=" * 60)
    
    try:
        # Initialize data loader
        data_loader = TelcoDataLoader(args.data_dir)
        
        # Initialize database managers
        cassandra_manager = None
        mongo_manager = None
        
        if not args.mongodb_only:
            logger.info("🔌 Connecting to Cassandra...")
            cassandra_manager = CassandraManager(**CASSANDRA_CONFIG)
            if not cassandra_manager.connect():
                logger.error("❌ Failed to connect to Cassandra")
                if not args.cassandra_only:
                    return 1
                cassandra_manager = None
        
        if not args.cassandra_only:
            logger.info("🔌 Connecting to MongoDB...")
            mongo_manager = MongoManager(**MONGODB_CONFIG)
            if not mongo_manager.connect():
                logger.error("❌ Failed to connect to MongoDB")
                if not args.mongodb_only:
                    return 1
                mongo_manager = None
        
        if not cassandra_manager and not mongo_manager:
            logger.error("❌ No database connections available")
            return 1
        
        if args.verify_only:
            # Only verify existing data
            verification_results = verify_loaded_data(cassandra_manager, mongo_manager)
            logger.info("✅ Data verification completed")
            return 0
        
        # Load data
        results = load_data_with_progress(
            data_loader, 
            cassandra_manager, 
            mongo_manager, 
            args.batch
        )
        
        if not results:
            logger.error("❌ Data loading failed")
            return 1
        
        # Verify loaded data
        verify_loaded_data(cassandra_manager, mongo_manager)
        
        # Run test queries if requested
        if args.test_queries:
            create_sample_queries(cassandra_manager, mongo_manager)
        
        logger.info("🎉 Data loading process completed successfully!")
        return 0
        
    except KeyboardInterrupt:
        logger.info("\n⚠️ Data loading interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"💥 Unexpected error: {e}")
        return 1
    finally:
        # Cleanup connections
        if cassandra_manager:
            cassandra_manager.close()
        if mongo_manager:
            mongo_manager.close()

if __name__ == '__main__':
    sys.exit(main())
