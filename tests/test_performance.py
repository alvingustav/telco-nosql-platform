import pytest
import unittest
from unittest.mock import Mock, patch
import time
from datetime import datetime, timedelta

from src.utils.performance_monitor import PerformanceMonitor
from src.database.query_aggregator import QueryAggregator

class TestPerformanceMonitor(unittest.TestCase):
    
    def setUp(self):
        self.performance_monitor = PerformanceMonitor()
    
    def test_log_query_performance(self):
        """Test logging query performance"""
        query_type = 'test_query'
        execution_time = 0.5
        
        self.performance_monitor.log_query_performance(query_type, execution_time)
        
        # Check that query was logged
        recent_queries = self.performance_monitor.get_recent_queries(1)
        self.assertEqual(len(recent_queries), 1)
        
        logged_query = recent_queries[0]
        self.assertEqual(logged_query['type'], query_type)
        self.assertEqual(logged_query['duration'], execution_time * 1000)  # Should be in ms
    
    def test_get_latest_metrics(self):
        """Test getting latest performance metrics"""
        # Log some test queries
        for i in range(5):
            self.performance_monitor.log_query_performance(
                'test_query', 
                0.1 + (i * 0.05)
            )
        
        metrics = self.performance_monitor.get_latest_metrics()
        
        self.assertIn('test_query', metrics)
        query_metrics = metrics['test_query']
        
        self.assertIn('average_duration', query_metrics)
        self.assertIn('total_queries', query_metrics)
        self.assertEqual(query_metrics['total_queries'], 5)
    
    def test_performance_comparison(self):
        """Test performance comparison functionality"""
        query_type = 'test_query'
        
        # Log queries without index
        for i in range(3):
            self.performance_monitor.log_query_performance(
                query_type, 
                1.0,  # Slower without index
                with_index=False
            )
        
        # Log queries with index
        for i in range(3):
            self.performance_monitor.log_query_performance(
                query_type, 
                0.3,  # Faster with index
                with_index=True
            )
        
        comparison = self.performance_monitor.get_performance_comparison(query_type)
        
        self.assertIn('with_index', comparison)
        self.assertIn('without_index', comparison)
        self.assertIn('improvement_percent', comparison)
        
        # Should show significant improvement
        self.assertGreater(comparison['improvement_percent'], 50)

class TestQueryPerformance(unittest.TestCase):
    
    def setUp(self):
        self.cassandra_manager = Mock()
        self.mongo_manager = Mock()
        self.query_aggregator = QueryAggregator(
            self.cassandra_manager,
            self.mongo_manager
        )
    
    def test_query_execution_time_measurement(self):
        """Test that query execution time is properly measured"""
        # Mock slow query
        def slow_query(*args, **kwargs):
            time.sleep(0.1)  # Simulate 100ms query
            return [{'result': 'test'}]
        
        self.cassandra_manager.execute_query.side_effect = slow_query
        
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
        
        result = self.query_aggregator.query_db1_call_analytics(start_date, end_date)
        
        # Check that execution time is reasonable
        self.assertIn('execution_time', result)
        self.assertGreater(result['execution_time'], 0.05)  # At least 50ms
        self.assertLess(result['execution_time'], 1.0)      # Less than 1 second
    
    def test_index_performance_impact(self):
        """Test performance impact of indexes"""
        # Mock different response times for with/without indexes
        def mock_query_with_index(*args, **kwargs):
            time.sleep(0.05)  # Fast with index
            return [{'result': 'test'}]
        
        def mock_query_without_index(*args, **kwargs):
            time.sleep(0.2)   # Slow without index
            return [{'result': 'test'}]
        
        # Test without index
        self.cassandra_manager.execute_query.side_effect = mock_query_without_index
        start_time = time.time()
        result_no_index = self.query_aggregator.query_db1_call_analytics(
            datetime.now() - timedelta(days=30),
            datetime.now()
        )
        time_no_index = time.time() - start_time
        
        # Test with index
        self.cassandra_manager.execute_query.side_effect = mock_query_with_index
        start_time = time.time()
        result_with_index = self.query_aggregator.query_db1_call_analytics(
            datetime.now() - timedelta(days=30),
            datetime.now()
        )
        time_with_index = time.time() - start_time
        
        # Index should provide performance improvement
        self.assertLess(time_with_index, time_no_index)
        
        # Calculate improvement percentage
        improvement = ((time_no_index - time_with_index) / time_no_index) * 100
        self.assertGreater(improvement, 50)  # Should be at least 50% improvement
    
    def test_concurrent_query_performance(self):
        """Test performance under concurrent load"""
        import threading
        
        results = []
        
        def execute_query():
            start_time = time.time()
            result = self.query_aggregator.query_db1_call_analytics(
                datetime.now() - timedelta(days=30),
                datetime.now()
            )
            execution_time = time.time() - start_time
            results.append(execution_time)
        
        # Mock quick response
        self.cassandra_manager.execute_query.return_value = [{'result': 'test'}]
        
        # Execute multiple queries concurrently
        threads = []
        for i in range(5):
            thread = threading.Thread(target=execute_query)
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Check that all queries completed
        self.assertEqual(len(results), 5)
        
        # Check that performance didn't degrade significantly
        avg_time = sum(results) / len(results)
        max_time = max(results)
        
        # All queries should complete within reasonable time
        self.assertLess(avg_time, 1.0)
        self.assertLess(max_time, 2.0)

if __name__ == '__main__':
    unittest.main()
