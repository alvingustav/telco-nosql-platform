import pytest
import unittest
from unittest.mock import Mock, patch
from datetime import datetime, timedelta

from src.database.query_aggregator import QueryAggregator

class TestQueries(unittest.TestCase):
    
    def setUp(self):
        self.cassandra_manager = Mock()
        self.mongo_manager = Mock()
        self.query_aggregator = QueryAggregator(
            self.cassandra_manager,
            self.mongo_manager
        )
    
    def test_call_analytics_query_performance(self):
        """Test call analytics query performance"""
        # Mock query execution
        self.cassandra_manager.execute_query.return_value = [
            {'call_type': 'voice', 'call_count': 1000, 'avg_duration': 120}
        ]
        
        start_time = datetime.now()
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
        
        result = self.query_aggregator.query_db1_call_analytics(start_date, end_date)
        
        # Check that execution time is recorded
        self.assertIn('execution_time', result)
        self.assertIsInstance(result['execution_time'], float)
        self.assertGreater(result['execution_time'], 0)
    
    def test_customer_insights_query_with_filters(self):
        """Test customer insights query with filters"""
        self.mongo_manager.execute_aggregation.return_value = [
            {
                '_id': {'segment': 'premium'},
                'customer_count': 100,
                'avg_monthly_fee': 250000
            }
        ]
        
        # Test with segment filter
        result = self.query_aggregator.query_db2_customer_insights(segment='premium')
        
        self.assertIn('results', result)
        self.assertEqual(result['query_type'], 'DB2_ONLY')
        
        # Test with plan type filter
        result = self.query_aggregator.query_db2_customer_insights(plan_type='postpaid')
        
        self.assertIn('results', result)
    
    def test_combined_query_data_integration(self):
        """Test combined query data integration"""
        # Mock Cassandra data
        cassandra_data = [
            {
                'caller_id': 'CUST_000001',
                'total_calls': 45,
                'total_duration': 2700,
                'total_cost': 135.50
            },
            {
                'caller_id': 'CUST_000002',
                'total_calls': 32,
                'total_duration': 1920,
                'total_cost': 96.00
            }
        ]
        
        # Mock MongoDB data
        mongodb_data = [
            {
                'customer_id': 'CUST_000001',
                'personal_info': {'first_name': 'Ahmad', 'last_name': 'Wijaya'},
                'customer_segment': 'premium',
                'subscription': {'plan_type': 'postpaid', 'monthly_fee': 200000}
            },
            {
                'customer_id': 'CUST_000002',
                'personal_info': {'first_name': 'Siti', 'last_name': 'Nurhaliza'},
                'customer_segment': 'basic',
                'subscription': {'plan_type': 'prepaid', 'monthly_fee': 75000}
            }
        ]
        
        self.cassandra_manager.execute_query.return_value = cassandra_data
        self.mongo_manager.execute_aggregation.return_value = mongodb_data
        
        result = self.query_aggregator.query_combined_customer_behavior('2024-01')
        
        # Check that data is properly combined
        self.assertEqual(result['query_type'], 'COMBINED')
        self.assertIn('results', result)
        
        # Check that results contain data from both databases
        if result['results']:
            first_result = result['results'][0]
            self.assertIn('customer_id', first_result)
            self.assertIn('total_calls', first_result)  # From Cassandra
            self.assertIn('customer_segment', first_result)  # From MongoDB
    
    def test_query_error_handling(self):
        """Test query error handling"""
        # Mock database error
        self.cassandra_manager.execute_query.side_effect = Exception("Database connection error")
        
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
        
        result = self.query_aggregator.query_db1_call_analytics(start_date, end_date)
        
        # Check that error is handled gracefully
        self.assertEqual(result['query_type'], 'DB1_ONLY')
        self.assertIn('error', result)
        self.assertIn('execution_time', result)
    
    def test_performance_comparison(self):
        """Test performance comparison functionality"""
        # Mock performance comparison
        with patch('time.sleep'):  # Speed up the test
            result = self.query_aggregator.performance_comparison()
        
        self.assertIn('individual_queries', result)
        self.assertIn('summary', result)
        
        # Check that all three query types are tested
        individual_results = result['individual_queries']
        expected_queries = ['query1_call_analytics', 'query2_customer_insights', 'query3_combined']
        
        for query_name in expected_queries:
            self.assertIn(query_name, individual_results)
            query_result = individual_results[query_name]
            self.assertIn('without_index', query_result)
            self.assertIn('with_index', query_result)
            self.assertIn('improvement_percent', query_result)

if __name__ == '__main__':
    unittest.main()
