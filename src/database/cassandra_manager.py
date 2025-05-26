from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy
import logging
import time
from typing import List, Dict, Any, Optional
from datetime import datetime

class CassandraManager:
    def __init__(self, hosts=['127.0.0.1'], port=9042, keyspace='telco_cdr', replication_factor=1):
        self.hosts = hosts
        self.port = port
        self.keyspace = keyspace
        self.replication_factor = replication_factor
        self.cluster = None
        self.session = None
        self.logger = logging.getLogger(__name__)
        
    def connect(self) -> bool:
        """Establish connection to Cassandra cluster"""
        try:
            self.logger.info("🔄 Connecting to Cassandra...")
            
            # Configure cluster with load balancing policy
            self.cluster = Cluster(
                self.hosts, 
                port=self.port,
                load_balancing_policy=DCAwareRoundRobinPolicy()
            )
            
            self.session = self.cluster.connect()
            self.logger.info("✅ Connected to Cassandra successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to connect to Cassandra: {e}")
            return False
    
    def setup_keyspace(self):
        """Create keyspace with proper replication strategy"""
        if not self.session:
            if not self.connect():
                raise Exception("Cannot connect to Cassandra")
        
        create_keyspace_query = f"""
        CREATE KEYSPACE IF NOT EXISTS {self.keyspace}
        WITH replication = {{
            'class': 'SimpleStrategy', 
            'replication_factor': {self.replication_factor}
        }}
        """
        
        try:
            self.session.execute(create_keyspace_query)
            self.session.set_keyspace(self.keyspace)
            self.logger.info(f"✅ Keyspace {self.keyspace} created/selected")
        except Exception as e:
            self.logger.error(f"❌ Keyspace setup failed: {e}")
            raise
    
    def create_tables(self):
        """Create all required tables for telco data"""
        
        # Call Detail Records table
        create_cdr_table = """
        CREATE TABLE IF NOT EXISTS call_records (
            call_id UUID PRIMARY KEY,
            caller_id TEXT,
            callee_id TEXT,
            call_start_time TIMESTAMP,
            call_end_time TIMESTAMP,
            duration_seconds INT,
            call_type TEXT,
            location_cell_id TEXT,
            location_lat DOUBLE,
            location_lon DOUBLE,
            cost_amount DECIMAL,
            network_type TEXT,
            quality_score INT,
            created_at TIMESTAMP
        )
        """
        
        # SMS Records table
        create_sms_table = """
        CREATE TABLE IF NOT EXISTS sms_records (
            sms_id UUID PRIMARY KEY,
            sender_id TEXT,
            receiver_id TEXT,
            message_length INT,
            sent_time TIMESTAMP,
            delivery_status TEXT,
            cost_amount DECIMAL,
            network_type TEXT,
            created_at TIMESTAMP
        )
        """
        
        # Data Usage table
        create_data_table = """
        CREATE TABLE IF NOT EXISTS data_usage (
            usage_id UUID PRIMARY KEY,
            customer_id TEXT,
            session_start TIMESTAMP,
            session_end TIMESTAMP,
            data_consumed_mb BIGINT,
            app_category TEXT,
            network_type TEXT,
            cost_amount DECIMAL,
            created_at TIMESTAMP
        )
        """
        
        tables = [
            ('call_records', create_cdr_table),
            ('sms_records', create_sms_table),
            ('data_usage', create_data_table)
        ]
        
        for table_name, table_query in tables:
            try:
                self.session.execute(table_query)
                self.logger.info(f"✅ Table {table_name} created successfully")
            except Exception as e:
                self.logger.error(f"❌ Failed to create table {table_name}: {e}")
                raise
    
    def create_indexes(self):
        """Create indexes for query optimization"""
        indexes = [
            # Call records indexes
            "CREATE INDEX IF NOT EXISTS call_records_caller_idx ON call_records (caller_id)",
            "CREATE INDEX IF NOT EXISTS call_records_start_time_idx ON call_records (call_start_time)",
            "CREATE INDEX IF NOT EXISTS call_records_type_idx ON call_records (call_type)",
            "CREATE INDEX IF NOT EXISTS call_records_network_idx ON call_records (network_type)",
            
            # SMS records indexes
            "CREATE INDEX IF NOT EXISTS sms_records_sender_idx ON sms_records (sender_id)",
            "CREATE INDEX IF NOT EXISTS sms_records_sent_time_idx ON sms_records (sent_time)",
            "CREATE INDEX IF NOT EXISTS sms_records_network_idx ON sms_records (network_type)",
            
            # Data usage indexes
            "CREATE INDEX IF NOT EXISTS data_usage_customer_idx ON data_usage (customer_id)",
            "CREATE INDEX IF NOT EXISTS data_usage_start_time_idx ON data_usage (session_start)",
            "CREATE INDEX IF NOT EXISTS data_usage_category_idx ON data_usage (app_category)"
        ]
        
        for index_query in indexes:
            try:
                self.session.execute(index_query)
                self.logger.info(f"✅ Index created: {index_query.split()[-1]}")
            except Exception as e:
                self.logger.warning(f"⚠️ Index creation failed: {e}")
    
    def drop_indexes(self):
        """Drop all indexes for performance comparison"""
        indexes_to_drop = [
            "DROP INDEX IF EXISTS call_records_caller_idx",
            "DROP INDEX IF EXISTS call_records_start_time_idx", 
            "DROP INDEX IF EXISTS call_records_type_idx",
            "DROP INDEX IF EXISTS call_records_network_idx",
            "DROP INDEX IF EXISTS sms_records_sender_idx",
            "DROP INDEX IF EXISTS sms_records_sent_time_idx",
            "DROP INDEX IF EXISTS sms_records_network_idx",
            "DROP INDEX IF EXISTS data_usage_customer_idx",
            "DROP INDEX IF EXISTS data_usage_start_time_idx",
            "DROP INDEX IF EXISTS data_usage_category_idx"
        ]
        
        for drop_query in indexes_to_drop:
            try:
                self.session.execute(drop_query)
                self.logger.info(f"✅ Index dropped: {drop_query.split()[-1]}")
            except Exception as e:
                self.logger.warning(f"⚠️ Index drop failed: {e}")
    
    def insert_batch_data(self, table_name: str, data: List[Dict], batch_size: int = 1000):
        """Insert data in batches for better performance"""
        if not data:
            return 0
        
        # Prepare insert statements based on table
        if table_name == 'call_records':
            insert_query = """
            INSERT INTO call_records (
                call_id, caller_id, callee_id, call_start_time, call_end_time,
                duration_seconds, call_type, location_cell_id, location_lat,
                location_lon, cost_amount, network_type, quality_score, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        elif table_name == 'sms_records':
            insert_query = """
            INSERT INTO sms_records (
                sms_id, sender_id, receiver_id, message_length, sent_time,
                delivery_status, cost_amount, network_type, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        elif table_name == 'data_usage':
            insert_query = """
            INSERT INTO data_usage (
                usage_id, customer_id, session_start, session_end,
                data_consumed_mb, app_category, network_type, cost_amount, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        else:
            raise ValueError(f"Unknown table: {table_name}")
        
        prepared = self.session.prepare(insert_query)
        inserted_count = 0
        
        for i in range(0, len(data), batch_size):
            batch = data[i:i+batch_size]
            
            for record in batch:
                try:
                    if table_name == 'call_records':
                        self.session.execute(prepared, [
                            record['call_id'], record['caller_id'], record['callee_id'],
                            record['call_start_time'], record['call_end_time'],
                            record['duration_seconds'], record['call_type'],
                            record['location_cell_id'], record['location_lat'],
                            record['location_lon'], record['cost_amount'],
                            record['network_type'], record['quality_score'],
                            record['created_at']
                        ])
                    elif table_name == 'sms_records':
                        self.session.execute(prepared, [
                            record['sms_id'], record['sender_id'], record['receiver_id'],
                            record['message_length'], record['sent_time'],
                            record['delivery_status'], record['cost_amount'],
                            record['network_type'], record['created_at']
                        ])
                    elif table_name == 'data_usage':
                        self.session.execute(prepared, [
                            record['usage_id'], record['customer_id'],
                            record['session_start'], record['session_end'],
                            record['data_consumed_mb'], record['app_category'],
                            record['network_type'], record['cost_amount'],
                            record['created_at']
                        ])
                    
                    inserted_count += 1
                    
                except Exception as e:
                    self.logger.error(f"❌ Failed to insert record: {e}")
            
            self.logger.info(f"Inserted {min(i+batch_size, len(data))}/{len(data)} records to {table_name}")
        
        return inserted_count
    
    def execute_query(self, query: str, parameters: List = None) -> List[Dict]:
        """Execute a query and return results"""
        try:
            if parameters:
                result = self.session.execute(query, parameters)
            else:
                result = self.session.execute(query)
            
            # Convert result to list of dictionaries
            columns = result.column_names if hasattr(result, 'column_names') else []
            return [dict(zip(columns, row)) for row in result]
            
        except Exception as e:
            self.logger.error(f"❌ Query execution failed: {e}")
            raise
    
    def get_table_count(self, table_name: str) -> int:
        """Get record count for a table"""
        try:
            result = self.session.execute(f"SELECT COUNT(*) FROM {table_name}")
            return result.one()[0]
        except Exception as e:
            self.logger.error(f"❌ Failed to get count for {table_name}: {e}")
            return 0
    
    def close(self):
        """Close database connections"""
        if self.cluster:
            self.cluster.shutdown()
            self.logger.info("✅ Cassandra connection closed")
