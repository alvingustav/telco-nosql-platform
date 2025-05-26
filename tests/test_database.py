import pytest
import unittest
from unittest.mock import Mock, patch
from datetime import datetime, timedelta

from src.database.cassandra_manager import CassandraManager
from src.database.mongodb_manager import MongoManager
from src.database.query_aggregator import QueryAggregator

class TestCassandraManager(unittest.TestCase):
    
    def setUp(self):
        self.cassandra_manager = CassandraManager()
    
    def test_connection(self):
        """Test Cassandra connection"""
        # Mock the connection
        with patch('cassandra.cluster.Cluster') as mock_cluster:
            mock_session = Mock()
            mock_cluster.return_value.connect.return_value = mock_session
            
            result = self.cassandra_manager.connect()
            self.assertTrue(result)
            self.assertIsNotNone(self.cassandra_manager.session)
    
    def test_keyspace_setup(self):
        """Test keyspace creation"""
        with patch.object(self.cassandra_manager, 'session') as mock_session:
            self.cassandra_manager.setup_keyspace()
            mock_session.execute.assert_called()
    
    def test_table_creation(self):
        """Test table creation"""
        with patch.object(self.cassandra_manager, 'session') as mock_session:
            self.cassandra_manager.create_tables()
            # Should call execute multiple times for different tables
            self.assertGreater(mock_session.execute.call_count, 0)

class TestMongoManager(unittest.TestCase):
    
    def setUp(self):
        self.mongo_manager = MongoManager()
    
    def test_connection(self):
        """Test MongoDB connection"""
        with patch('pymongo.MongoClient') as mock_client:
            mock_db = Mock()
            mock_client.return_value.__getitem__.return_value = mock_db
            mock_client.return_value.admin.command.return_value = {'ok': 1}
            
            result = self.mongo_manager.connect()
            self.assertTrue(result)
            self.assertIsNotNone(self.mongo_manager.db)
    
    def test_collection_setup(self):
        """Test collection and index creation"""
        with patch.object(self.mongo_manager, 'db') as mock_db:
            mock_collection = Mock()
            mock_db.__getitem__.return_value = mock_collection
            
            self.mongo_manager.create_collections_and_indexes()
            mock_collection.create_index.assert_called()

class TestQueryAggregator(unittest.TestCase):
    
    def setUp(self):
        self.cassandra_manager = Mock()
        self.mongo_manager = Mock()
        self.query_aggregator = QueryAggregator(
            self.cassandra_manager, 
            self.mongo_manager
        )
    
    def test_db1_query(self):
        """Test DB1 only query"""
        # Mock Cassandra query result
        self.cassandra_manager.execute_query.return_value = [
            {
                'call_type': 'voice',
                'network_type': '4G',
                'call_count': 1000,
                'avg_duration': 120.5,
                'total_cost': 5000.0
            }
        ]
        
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
        
        result = self.query_aggregator.query_db1_call_analytics(start_date, end_date)
        
        self.assertEqual(result['query_type'], 'DB1_ONLY')
        self.assertEqual(result['database'], 'Cassandra')
        self.assertIn('results', result)
    
    def test_db2_query(self):
        """Test DB2 only query"""
        # Mock MongoDB aggregation result
        self.mongo_manager.execute_aggregation.return_value = [
            {
                '_id': {'segment': 'premium', 'plan_type': 'postpaid'},
                'customer_count': 500,
                'avg_monthly_fee': 200000,
                'total_revenue': 100000000
            }
        ]
        
        result = self.query_aggregator.query_db2_customer_insights()
        
        self.assertEqual(result['query_type'], 'DB2_ONLY')
        self.assertEqual(result['database'], 'MongoDB')
        self.assertIn('results', result)
    
    def test_combined_query(self):
        """Test combined query"""
        # Mock both database responses
        self.cassandra_manager.execute_query.return_value = [
            {
                'caller_id': 'CUST_000001',
                'total_calls': 50,
                'total_duration': 3000,
                'total_cost': 150.0
            }
        ]
        
        self.mongo_manager.execute_aggregation.return_value = [
            {
                'customer_id': 'CUST_000001',
                'personal_info': {'first_name': 'John', 'last_name': 'Doe'},
                'customer_segment': 'premium',
                'subscription': {'plan_type': 'postpaid', 'monthly_fee': 200000}
            }
        ]
        
        result = self.query_aggregator.query_combined_customer_behavior('2024-01')
        
        self.assertEqual(result['query_type'], 'COMBINED')
        self.assertIn('Cassandra', result['databases'])
        self.assertIn('MongoDB', result['databases'])

if __name__ == '__main__':
    unittest.main()
