import json
import os
from datetime import datetime
from typing import Dict, List, Any, Optional
import time
import logging
from uuid import UUID

class TelcoDataLoader:
    def __init__(self, data_directory='telco_data_export'):
        self.data_directory = data_directory
        self.logger = logging.getLogger(__name__)
        
    def load_json_file(self, filename: str) -> List[Dict]:
        """Load data from JSON file with error handling"""
        filepath = os.path.join(self.data_directory, filename)
        
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File not found: {filepath}")
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            self.logger.info(f"Successfully loaded {len(data)} records from {filename}")
            return data
            
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in {filename}: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error loading {filename}: {e}")
            raise
    
    def convert_datetime_strings(self, data: List[Dict]) -> List[Dict]:
        """Convert ISO datetime strings back to datetime objects"""
        datetime_fields = [
            'call_start_time', 'call_end_time', 'created_at', 'sent_time',
            'session_start', 'session_end', 'registration_date', 'start_date',
            'end_date', 'payment_date', 'ticket_date', 'resolution_date',
            'updated_at'
        ]
        
        converted_data = []
        for record in data:
            converted_record = {}
            for key, value in record.items():
                if key in datetime_fields and isinstance(value, str):
                    try:
                        # Handle different datetime formats
                        if 'T' in value:
                            # ISO format with T separator
                            if value.endswith('Z'):
                                value = value[:-1] + '+00:00'
                            converted_record[key] = datetime.fromisoformat(value)
                        else:
                            # Keep as string if not proper ISO format
                            converted_record[key] = value
                    except ValueError:
                        # If conversion fails, keep original value
                        converted_record[key] = value
                elif key == 'call_id' or key == 'sms_id' or key == 'usage_id':
                    # Convert UUID strings to UUID objects for Cassandra
                    try:
                        converted_record[key] = UUID(value) if isinstance(value, str) else value
                    except ValueError:
                        converted_record[key] = value
                else:
                    converted_record[key] = value
            converted_data.append(converted_record)
        
        return converted_data
    
    def validate_cassandra_data(self, data: List[Dict], table_name: str) -> List[Dict]:
        """Validate and clean data for Cassandra tables"""
        valid_data = []
        
        required_fields = {
            'call_records': ['call_id', 'caller_id', 'callee_id', 'call_start_time', 'call_end_time'],
            'sms_records': ['sms_id', 'sender_id', 'receiver_id', 'sent_time'],
            'data_usage': ['usage_id', 'customer_id', 'session_start', 'session_end']
        }
        
        required = required_fields.get(table_name, [])
        
        for record in data:
            # Check required fields
            if all(field in record and record[field] is not None for field in required):
                # Additional validation
                if table_name == 'call_records':
                    if isinstance(record.get('duration_seconds'), (int, float)) and record['duration_seconds'] >= 0:
                        valid_data.append(record)
                elif table_name == 'sms_records':
                    if isinstance(record.get('message_length'), (int, float)) and record['message_length'] > 0:
                        valid_data.append(record)
                elif table_name == 'data_usage':
                    if isinstance(record.get('data_consumed_mb'), (int, float)) and record['data_consumed_mb'] > 0:
                        valid_data.append(record)
                else:
                    valid_data.append(record)
        
        self.logger.info(f"Validated {len(valid_data)}/{len(data)} records for {table_name}")
        return valid_data
    
    def validate_mongodb_data(self, data: List[Dict], collection_name: str) -> List[Dict]:
        """Validate and clean data for MongoDB collections"""
        valid_data = []
        
        required_fields = {
            'customers': ['customer_id', 'personal_info'],
            'subscriptions': ['customer_id', 'plan_type'],
            'billing': ['customer_id', 'billing_month'],
            'customer_support': ['customer_id', 'ticket_id']
        }
        
        required = required_fields.get(collection_name, [])
        
        for record in data:
            # Check required fields
            if all(field in record and record[field] is not None for field in required):
                # Additional validation for specific collections
                if collection_name == 'customers':
                    if isinstance(record.get('personal_info'), dict):
                        valid_data.append(record)
                elif collection_name == 'billing':
                    if isinstance(record.get('amount'), (int, float)) and record['amount'] > 0:
                        valid_data.append(record)
                else:
                    valid_data.append(record)
        
        self.logger.info(f"Validated {len(valid_data)}/{len(data)} records for {collection_name}")
        return valid_data
    
    def load_cassandra_data(self, cassandra_manager) -> Dict[str, int]:
        """Load all Cassandra data from JSON files"""
        results = {}
        
        cassandra_files = {
            'cdr_data.json': 'call_records',
            'sms_data.json': 'sms_records',
            'data_usage.json': 'data_usage'
        }
        
        for filename, table_name in cassandra_files.items():
            try:
                self.logger.info(f"Loading {filename} to {table_name}...")
                
                # Load and convert data
                raw_data = self.load_json_file(filename)
                converted_data = self.convert_datetime_strings(raw_data)
                validated_data = self.validate_cassandra_data(converted_data, table_name)
                
                # Insert data
                inserted_count = cassandra_manager.insert_batch_data(table_name, validated_data)
                results[table_name] = inserted_count
                
                self.logger.info(f"✅ Successfully loaded {inserted_count} records to {table_name}")
                
            except Exception as e:
                self.logger.error(f"❌ Failed to load {filename}: {e}")
                results[table_name] = 0
        
        return results
    
    def load_mongodb_data(self, mongo_manager) -> Dict[str, int]:
        """Load all MongoDB data from JSON files"""
        results = {}
        
        mongodb_files = {
            'customers.json': 'customers',
            'subscriptions.json': 'subscriptions',
            'billing_records.json': 'billing',
            'support_tickets.json': 'customer_support'
        }
        
        for filename, collection_name in mongodb_files.items():
            try:
                self.logger.info(f"Loading {filename} to {collection_name}...")
                
                # Load and convert data
                raw_data = self.load_json_file(filename)
                converted_data = self.convert_datetime_strings(raw_data)
                validated_data = self.validate_mongodb_data(converted_data, collection_name)
                
                # Insert data
                inserted_count = mongo_manager.insert_batch_data(collection_name, validated_data)
                results[collection_name] = inserted_count
                
                self.logger.info(f"✅ Successfully loaded {inserted_count} records to {collection_name}")
                
            except Exception as e:
                self.logger.error(f"❌ Failed to load {filename}: {e}")
                results[collection_name] = 0
        
        return results
    
    def verify_data_directory(self) -> Dict[str, bool]:
        """Verify that all required JSON files exist"""
        required_files = [
            'cdr_data.json',
            'sms_data.json', 
            'data_usage.json',
            'customers.json',
            'subscriptions.json',
            'billing_records.json',
            'support_tickets.json'
        ]
        
        verification = {}
        for filename in required_files:
            filepath = os.path.join(self.data_directory, filename)
            verification[filename] = os.path.exists(filepath)
        
        return verification
    
    def get_data_summary(self) -> Dict[str, Any]:
        """Get comprehensive summary of data in the export directory"""
        verification = self.verify_data_directory()
        summary = {
            'directory': self.data_directory,
            'files_found': sum(verification.values()),
            'total_files': len(verification),
            'file_status': verification,
            'estimated_records': {},
            'file_sizes': {},
            'data_quality': {}
        }
        
        # Get detailed information for existing files
        for filename, exists in verification.items():
            if exists:
                try:
                    filepath = os.path.join(self.data_directory, filename)
                    
                    # Get file size
                    file_size = os.path.getsize(filepath)
                    summary['file_sizes'][filename] = file_size
                    
                    # Get record count and basic validation
                    data = self.load_json_file(filename)
                    summary['estimated_records'][filename] = len(data)
                    
                    # Basic data quality check
                    if data:
                        sample_record = data[0]
                        summary['data_quality'][filename] = {
                            'has_data': len(data) > 0,
                            'sample_fields': list(sample_record.keys()) if isinstance(sample_record, dict) else [],
                            'field_count': len(sample_record) if isinstance(sample_record, dict) else 0
                        }
                    
                except Exception as e:
                    summary['estimated_records'][filename] = f'Error: {str(e)}'
                    summary['data_quality'][filename] = {'error': str(e)}
        
        return summary
    
    def create_data_backup(self, backup_directory: str = None) -> bool:
        """Create backup of current data directory"""
        if not backup_directory:
            backup_directory = f"{self.data_directory}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        try:
            import shutil
            shutil.copytree(self.data_directory, backup_directory)
            self.logger.info(f"✅ Data backup created: {backup_directory}")
            return True
        except Exception as e:
            self.logger.error(f"❌ Failed to create backup: {e}")
            return False
