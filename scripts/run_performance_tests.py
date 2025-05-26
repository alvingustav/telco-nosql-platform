#!/usr/bin/env python3
"""
Performance testing script for Telco NoSQL Platform
Usage: python scripts/run_performance_tests.py --iterations 5
"""

import sys
import os
import argparse
import logging
import time
import json
import statistics
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from database.cassandra_manager import CassandraManager
from database.mongodb_manager import MongoManager
from database.query_aggregator import QueryAggregator
from utils.performance_monitor import PerformanceMonitor
from config.database_config import CASSANDRA_CONFIG, MONGODB_CONFIG, PERFORMANCE_CONFIG

def setup_logging(verbose=False):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

class PerformanceTester:
    def __init__(self, cassandra_manager, mongo_manager, iterations=5):
        self.cassandra_manager = cassandra_manager
        self.mongo_manager = mongo_manager
        self.query_aggregator = QueryAggregator(cassandra_manager, mongo_manager)
        self.iterations = iterations
        self.logger = logging.getLogger(__name__)
        self.performance_monitor = PerformanceMonitor()
        
        # Test parameters
        self.test_params = {
            'call_analytics': {
                'start_date': datetime.now() - timedelta(days=30),
                'end_date': datetime.now(),
                'call_type': 'voice'
            },
            'customer_insights': {
                'segment': 'premium',
                'plan_type': 'postpaid'
            },
            'combined_behavior': {
                'month': '2024-01',
                'limit': 50
            }
        }
    
    def run_single_query_test(self, query_type, with_index=True):
        """Run a single query test"""
        try:
            start_time = time.time()
            
            if query_type == 'call_analytics':
                result = self.query_aggregator.query_db1_call_analytics(
                    self.test_params['call_analytics']['start_date'],
                    self.test_params['call_analytics']['end_date'],
                    self.test_params['call_analytics']['call_type']
                )
            elif query_type == 'customer_insights':
                result = self.query_aggregator.query_db2_customer_insights(
                    self.test_params['customer_insights']['segment'],
                    self.test_params['customer_insights']['plan_type']
                )
            elif query_type == 'combined_behavior':
                result = self.query_aggregator.query_combined_customer_behavior(
                    self.test_params['combined_behavior']['month'],
                    self.test_params['combined_behavior']['limit']
                )
            else:
                raise ValueError(f"Unknown query type: {query_type}")
            
            execution_time = time.time() - start_time
            
            # Log performance
            self.performance_monitor.log_query_performance(
                query_type, execution_time, with_index
            )
            
            return {
                'success': True,
                'execution_time': execution_time,
                'result_count': result.get('record_count', 0),
                'error': None
            }
            
        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.error(f"Query {query_type} failed: {e}")
            
            return {
                'success': False,
                'execution_time': execution_time,
                'result_count': 0,
                'error': str(e)
            }
    
    def run_query_iterations(self, query_type, with_index=True, warmup=True):
        """Run multiple iterations of a query"""
        self.logger.info(f"🔄 Testing {query_type} ({'with' if with_index else 'without'} index)")
        
        results = []
        
        # Warmup queries
        if warmup:
            self.logger.debug("Warming up...")
            for _ in range(PERFORMANCE_CONFIG['warmup_queries']):
                self.run_single_query_test(query_type, with_index)
        
        # Actual test iterations
        for i in range(self.iterations):
            self.logger.debug(f"Iteration {i+1}/{self.iterations}")
            result = self.run_single_query_test(query_type, with_index)
            results.append(result)
            
            # Small delay between iterations
            time.sleep(0.1)
        
        return results
    
    def analyze_results(self, results):
        """Analyze query results and calculate statistics"""
        if not results:
            return {}
        
        successful_results = [r for r in results if r['success']]
        
        if not successful_results:
            return {
                'success_rate': 0,
                'total_iterations': len(results),
                'successful_iterations': 0
            }
        
        execution_times = [r['execution_time'] for r in successful_results]
        
        analysis = {
            'success_rate': len(successful_results) / len(results) * 100,
            'total_iterations': len(results),
            'successful_iterations': len(successful_results),
            'min_time': min(execution_times),
            'max_time': max(execution_times),
            'mean_time': statistics.mean(execution_times),
            'median_time': statistics.median(execution_times),
            'stdev_time': statistics.stdev(execution_times) if len(execution_times) > 1 else 0,
            'total_time': sum(execution_times),
            'avg_result_count': statistics.mean([r['result_count'] for r in successful_results])
        }
        
        return analysis
    
    def run_index_comparison_test(self, query_type):
        """Run comparison test with and without indexes"""
        self.logger.info(f"🧪 Running index comparison for {query_type}")
        
        # Test without indexes
        self.logger.info("📊 Testing without indexes...")
        self.drop_indexes()
        time.sleep(2)  # Allow time for index drops to take effect
        
        results_without_index = self.run_query_iterations(query_type, with_index=False)
        analysis_without = self.analyze_results(results_without_index)
        
        # Test with indexes
        self.logger.info("📊 Testing with indexes...")
        self.create_indexes()
        time.sleep(3)  # Allow time for indexes to be built
        
        results_with_index = self.run_query_iterations(query_type, with_index=True)
        analysis_with = self.analyze_results(results_with_index)
        
        # Calculate improvement
        improvement = {}
        if analysis_without.get('mean_time', 0) > 0 and analysis_with.get('mean_time', 0) > 0:
            improvement_percent = (
                (analysis_without['mean_time'] - analysis_with['mean_time']) / 
                analysis_without['mean_time']
            ) * 100
            
            improvement = {
                'improvement_percent': improvement_percent,
                'time_saved': analysis_without['mean_time'] - analysis_with['mean_time'],
                'speedup_factor': analysis_without['mean_time'] / analysis_with['mean_time']
            }
        
        return {
            'query_type': query_type,
            'without_index': analysis_without,
            'with_index': analysis_with,
            'improvement': improvement
        }
    
    def run_concurrent_test(self, query_type, num_threads=5):
        """Run concurrent query test"""
        self.logger.info(f"🚀 Running concurrent test for {query_type} with {num_threads} threads")
        
        start_time = time.time()
        results = []
        
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            # Submit concurrent queries
            futures = [
                executor.submit(self.run_single_query_test, query_type, True)
                for _ in range(num_threads * 2)  # Run 2 queries per thread
            ]
            
            # Collect results
            for future in as_completed(futures):
                try:
                    result = future.result(timeout=PERFORMANCE_CONFIG['timeout_seconds'])
                    results.append(result)
                except Exception as e:
                    self.logger.error(f"Concurrent query failed: {e}")
                    results.append({
                        'success': False,
                        'execution_time': 0,
                        'result_count': 0,
                        'error': str(e)
                    })
        
        total_time = time.time() - start_time
        analysis = self.analyze_results(results)
        analysis['total_concurrent_time'] = total_time
        analysis['queries_per_second'] = len(results) / total_time if total_time > 0 else 0
        
        return analysis
    
    def create_indexes(self):
        """Create indexes on both databases"""
        try:
            if self.cassandra_manager:
                self.cassandra_manager.create_indexes()
            if self.mongo_manager:
                self.mongo_manager.create_collections_and_indexes()
            self.logger.debug("✅ Indexes created")
        except Exception as e:
            self.logger.error(f"❌ Failed to create indexes: {e}")
    
    def drop_indexes(self):
        """Drop indexes from both databases"""
        try:
            if self.cassandra_manager:
                self.cassandra_manager.drop_indexes()
            if self.mongo_manager:
                collections = ['customers', 'subscriptions', 'billing', 'customer_support']
                for collection in collections:
                    self.mongo_manager.drop_indexes(collection)
            self.logger.debug("✅ Indexes dropped")
        except Exception as e:
            self.logger.error(f"❌ Failed to drop indexes: {e}")
    
    def run_comprehensive_test(self):
        """Run comprehensive performance test suite"""
        self.logger.info("🚀 Starting comprehensive performance test suite")
        self.logger.info("=" * 60)
        
        test_results = {
            'test_start_time': datetime.now().isoformat(),
            'test_parameters': {
                'iterations': self.iterations,
                'warmup_queries': PERFORMANCE_CONFIG['warmup_queries'],
                'timeout_seconds': PERFORMANCE_CONFIG['timeout_seconds']
            },
            'query_tests': {},
            'concurrent_tests': {},
            'summary': {}
        }
        
        query_types = ['call_analytics', 'customer_insights', 'combined_behavior']
        
        # Run index comparison tests
        for query_type in query_types:
            try:
                self.logger.info(f"🧪 Testing {query_type}...")
                result = self.run_index_comparison_test(query_type)
                test_results['query_tests'][query_type] = result
                
                # Log results
                if result['improvement']:
                    improvement = result['improvement']['improvement_percent']
                    self.logger.info(f"✅ {query_type}: {improvement:.2f}% improvement with indexes")
                else:
                    self.logger.warning(f"⚠️ {query_type}: Could not calculate improvement")
                    
            except Exception as e:
                self.logger.error(f"❌ {query_type} test failed: {e}")
                test_results['query_tests'][query_type] = {'error': str(e)}
        
        # Run concurrent tests
        self.logger.info("🚀 Running concurrent performance tests...")
        for query_type in query_types:
            try:
                result = self.run_concurrent_test(query_type)
                test_results['concurrent_tests'][query_type] = result
                
                qps = result.get('queries_per_second', 0)
                self.logger.info(f"✅ {query_type} concurrent: {qps:.2f} queries/second")
                
            except Exception as e:
                self.logger.error(f"❌ {query_type} concurrent test failed: {e}")
                test_results['concurrent_tests'][query_type] = {'error': str(e)}
        
        # Calculate summary statistics
        test_results['summary'] = self.calculate_test_summary(test_results)
        test_results['test_end_time'] = datetime.now().isoformat()
        
        return test_results
    
    def calculate_test_summary(self, test_results):
        """Calculate summary statistics from test results"""
        summary = {
            'total_queries_tested': len(test_results['query_tests']),
            'successful_tests': 0,
            'average_improvement': 0,
            'best_improvement': 0,
            'worst_improvement': 0,
            'total_test_time': 0
        }
        
        improvements = []
        
        for query_type, result in test_results['query_tests'].items():
            if 'error' not in result and result.get('improvement'):
                summary['successful_tests'] += 1
                improvement = result['improvement']['improvement_percent']
                improvements.append(improvement)
        
        if improvements:
            summary['average_improvement'] = statistics.mean(improvements)
            summary['best_improvement'] = max(improvements)
            summary['worst_improvement'] = min(improvements)
        
        return summary

def save_results(results, output_file):
    """Save test results to JSON file"""
    try:
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        logging.getLogger(__name__).info(f"📄 Results saved to {output_file}")
    except Exception as e:
        logging.getLogger(__name__).error(f"❌ Failed to save results: {e}")

def print_summary_report(results):
    """Print a summary report of test results"""
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 60)
    logger.info("📊 PERFORMANCE TEST SUMMARY REPORT")
    logger.info("=" * 60)
    
    summary = results.get('summary', {})
    
    logger.info(f"🧪 Total queries tested: {summary.get('total_queries_tested', 0)}")
    logger.info(f"✅ Successful tests: {summary.get('successful_tests', 0)}")
    logger.info(f"📈 Average improvement: {summary.get('average_improvement', 0):.2f}%")
    logger.info(f"🏆 Best improvement: {summary.get('best_improvement', 0):.2f}%")
    logger.info(f"📉 Worst improvement: {summary.get('worst_improvement', 0):.2f}%")
    
    logger.info("\n📋 Detailed Results:")
    for query_type, result in results.get('query_tests', {}).items():
        if 'error' in result:
            logger.info(f"❌ {query_type}: {result['error']}")
        elif result.get('improvement'):
            improvement = result['improvement']['improvement_percent']
            speedup = result['improvement']['speedup_factor']
            logger.info(f"✅ {query_type}: {improvement:.2f}% improvement ({speedup:.2f}x speedup)")
        else:
            logger.info(f"⚠️ {query_type}: No improvement data")
    
    logger.info("=" * 60)

def main():
    parser = argparse.ArgumentParser(description='Run performance tests for Telco platform')
    parser.add_argument('--iterations', type=int, default=PERFORMANCE_CONFIG['test_iterations'],
                       help='Number of test iterations per query')
    parser.add_argument('--output', '-o', default='performance_results.json',
                       help='Output file for test results')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    parser.add_argument('--concurrent-threads', type=int, default=5,
                       help='Number of concurrent threads for concurrent tests')
    parser.add_argument('--query-type', choices=['call_analytics', 'customer_insights', 'combined_behavior'],
                       help='Run test for specific query type only')
    parser.add_argument('--no-warmup', action='store_true',
                       help='Skip warmup queries')
    
    args = parser.parse_args()
    
    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)
    
    logger.info("🚀 Starting Telco Performance Testing")
    logger.info("=" * 60)
    
    try:
        # Initialize database managers
        logger.info("🔌 Connecting to databases...")
        
        cassandra_manager = CassandraManager(**CASSANDRA_CONFIG)
        if not cassandra_manager.connect():
            logger.error("❌ Failed to connect to Cassandra")
            return 1
        
        mongo_manager = MongoManager(**MONGODB_CONFIG)
        if not mongo_manager.connect():
            logger.error("❌ Failed to connect to MongoDB")
            return 1
        
        # Initialize performance tester
        tester = PerformanceTester(cassandra_manager, mongo_manager, args.iterations)
        
        # Run tests
        if args.query_type:
            # Run single query type test
            logger.info(f"🧪 Running test for {args.query_type} only")
            result = tester.run_index_comparison_test(args.query_type)
            results = {
                'test_start_time': datetime.now().isoformat(),
                'query_tests': {args.query_type: result},
                'test_end_time': datetime.now().isoformat()
            }
        else:
            # Run comprehensive test suite
            results = tester.run_comprehensive_test()
        
        # Save and display results
        save_results(results, args.output)
        print_summary_report(results)
        
        logger.info("🎉 Performance testing completed successfully!")
        return 0
        
    except KeyboardInterrupt:
        logger.info("\n⚠️ Performance testing interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"💥 Unexpected error: {e}")
        return 1
    finally:
        # Cleanup connections
        try:
            if 'cassandra_manager' in locals():
                cassandra_manager.close()
            if 'mongo_manager' in locals():
                mongo_manager.close()
        except:
            pass

if __name__ == '__main__':
    sys.exit(main())
