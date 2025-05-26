from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

class MongoManager:
    def __init__(self, uri='mongodb://localhost:27017/', database='telco_customers', connection_timeout=30000):
        self.uri = uri
        self.database_name = database
        self.connection_timeout = connection_timeout
        self.client = None
        self.db = None
        self.logger = logging.getLogger(__name__)
    
    def connect(self) -> bool:
        """Establish connection to MongoDB"""
        try:
            self.logger.info("🔄 Connecting to MongoDB...")
            
            self.client = MongoClient(
                self.uri, 
                serverSelectionTimeoutMS=self.connection_timeout
            )
            
            # Test connection
            self.client.admin.command('ping')
            self.db = self.client[self.database_name]
            
            self.logger.info("✅ Connected to MongoDB successfully")
            return True
            
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            self.logger.error(f"❌ Failed to connect to MongoDB: {e}")
            return False
        except Exception as e:
            self.logger.error(f"❌ Unexpected error connecting to MongoDB: {e}")
            return False
    
    def create_collections_and_indexes(self):
        """Create collections and indexes for optimal performance"""
        if not self.db:
            if not self.connect():
                raise Exception("Cannot connect to MongoDB")
        
        collections_config = {
            'customers': [
                [("customer_id", ASCENDING)],
                [("phone_number", ASCENDING)],
                [("registration_date", ASCENDING)],
                [("status", ASCENDING)],
                [("location.city", ASCENDING)],
                [("customer_segment", ASCENDING)]
            ],
            'subscriptions': [
                [("customer_id", ASCENDING)],
                [("plan_type", ASCENDING)],
                [("start_date", ASCENDING)],
                [("status", ASCENDING)],
                [("monthly_fee", ASCENDING)]
            ],
            'billing': [
                [("customer_id", ASCENDING)],
                [("billing_month", ASCENDING)],
                [("payment_status", ASCENDING)],
                [("payment_date", ASCENDING)],
                [("amount", ASCENDING)]
            ],
            'customer_support': [
                [("customer_id", ASCENDING)],
                [("ticket_date", ASCENDING)],
                [("status", ASCENDING)],
                [("issue_type", ASCENDING)],
                [("priority", ASCENDING)]
            ]
        }
        
        for collection_name, indexes in collections_config.items():
            try:
                collection = self.db[collection_name]
                
                # Create indexes
                for index_spec in indexes:
                    try:
                        collection.create_index(index_spec)
                        self.logger.info(f"✅ Index created on {collection_name}: {index_spec}")
                    except Exception as e:
                        self.logger.warning(f"⚠️ Index creation failed on {collection_name}: {e}")
                
                self.logger.info(f"✅ Collection {collection_name} setup completed")
                
            except Exception as e:
                self.logger.error(f"❌ Failed to setup collection {collection_name}: {e}")
                raise
    
    def drop_indexes(self, collection_name: str):
        """Drop all indexes except _id for performance comparison"""
        try:
            collection = self.db[collection_name]
            indexes = collection.list_indexes()
            
            for index in indexes:
                index_name = index['name']
                if index_name != '_id_':  # Don't drop the default _id index
                    collection.drop_index(index_name)
                    self.logger.info(f"✅ Dropped index {index_name} from {collection_name}")
                    
        except Exception as e:
            self.logger.error(f"❌ Failed to drop indexes from {collection_name}: {e}")
    
    def insert_batch_data(self, collection_name: str, data: List[Dict], batch_size: int = 5000) -> int:
        """Insert data in batches for better performance"""
        if not data:
            return 0
        
        collection = self.db[collection_name]
        inserted_count = 0
        
        for i in range(0, len(data), batch_size):
            batch = data[i:i+batch_size]
            
            try:
                result = collection.insert_many(batch, ordered=False)
                inserted_count += len(result.inserted_ids)
                self.logger.info(f"Inserted {min(i+batch_size, len(data))}/{len(data)} records to {collection_name}")
                
            except Exception as e:
                self.logger.error(f"❌ Failed to insert batch to {collection_name}: {e}")
        
        return inserted_count
    
    def execute_aggregation(self, collection_name: str, pipeline: List[Dict]) -> List[Dict]:
        """Execute aggregation pipeline"""
        try:
            collection = self.db[collection_name]
            result = list(collection.aggregate(pipeline))
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Aggregation failed on {collection_name}: {e}")
            raise
    
    def find_documents(self, collection_name: str, query: Dict = None, projection: Dict = None, 
                      sort: List = None, limit: int = None) -> List[Dict]:
        """Find documents with optional query, projection, sort, and limit"""
        try:
            collection = self.db[collection_name]
            cursor = collection.find(query or {}, projection)
            
            if sort:
                cursor = cursor.sort(sort)
            if limit:
                cursor = cursor.limit(limit)
            
            return list(cursor)
            
        except Exception as e:
            self.logger.error(f"❌ Find operation failed on {collection_name}: {e}")
            raise
    
    def get_collection_count(self, collection_name: str, query: Dict = None) -> int:
        """Get document count for a collection"""
        try:
            collection = self.db[collection_name]
            return collection.count_documents(query or {})
        except Exception as e:
            self.logger.error(f"❌ Failed to get count for {collection_name}: {e}")
            return 0
    
    def get_collection_stats(self, collection_name: str) -> Dict:
        """Get collection statistics"""
        try:
            stats = self.db.command("collStats", collection_name)
            return {
                'count': stats.get('count', 0),
                'size': stats.get('size', 0),
                'avgObjSize': stats.get('avgObjSize', 0),
                'indexSizes': stats.get('indexSizes', {})
            }
        except Exception as e:
            self.logger.error(f"❌ Failed to get stats for {collection_name}: {e}")
            return {}
    
    def close(self):
        """Close database connection"""
        if self.client:
            self.client.close()
            self.logger.info("✅ MongoDB connection closed")
