import json
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import uuid

class DataValidator:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def validate_cassandra_record(self, record: Dict, table_name: str) -> bool:
        """Validate a single Cassandra record"""
        try:
            if table_name == 'call_records':
                required_fields = ['call_id', 'caller_id', 'callee_id', 'call_start_time', 'call_end_time']
                return all(field in record for field in required_fields)
            
            elif table_name == 'sms_records':
                required_fields = ['sms_id', 'sender_id', 'receiver_id', 'sent_time']
                return all(field in record for field in required_fields)
            
            elif table_name == 'data_usage':
                required_fields = ['usage_id', 'customer_id', 'session_start', 'session_end']
                return all(field in record for field in required_fields)
            
            return False
            
        except Exception as e:
            self.logger.error(f"Validation error: {e}")
            return False
    
    def validate_mongodb_record(self, record: Dict, collection_name: str) -> bool:
        """Validate a single MongoDB record"""
        try:
            if collection_name == 'customers':
                return 'customer_id' in record and 'personal_info' in record
            
            elif collection_name == 'subscriptions':
                return 'customer_id' in record and 'plan_type' in record
            
            elif collection_name == 'billing':
                return 'customer_id' in record and 'billing_month' in record
            
            elif collection_name == 'customer_support':
                return 'customer_id' in record and 'ticket_id' in record
            
            return False
            
        except Exception as e:
            self.logger.error(f"Validation error: {e}")
            return False
    
    def validate_json_file(self, filepath: str) -> Dict[str, Any]:
        """Validate JSON file format and content"""
        validation_result = {
            'valid': False,
            'record_count': 0,
            'errors': [],
            'warnings': []
        }
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if not isinstance(data, list):
                validation_result['errors'].append("JSON must contain an array of records")
                return validation_result
            
            validation_result['record_count'] = len(data)
            
            if len(data) == 0:
                validation_result['warnings'].append("File contains no records")
            
            validation_result['valid'] = True
            
        except json.JSONDecodeError as e:
            validation_result['errors'].append(f"Invalid JSON format: {e}")
        except FileNotFoundError:
            validation_result['errors'].append("File not found")
        except Exception as e:
            validation_result['errors'].append(f"Unexpected error: {e}")
        
        return validation_result
