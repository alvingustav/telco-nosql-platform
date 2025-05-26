import time
import json
from datetime import datetime, timedelta
from collections import defaultdict, deque
import threading

class PerformanceMonitor:
    def __init__(self):
        self.query_history = deque(maxlen=1000)  # Keep last 1000 queries
        self.performance_metrics = defaultdict(list)
        self.lock = threading.Lock()
    
    def log_query_performance(self, query_type, execution_time, with_index=True):
        """Log query performance metrics"""
        with self.lock:
            timestamp = datetime.now()
            
            query_record = {
                'timestamp': timestamp.isoformat(),
                'type': query_type,
                'duration': round(execution_time * 1000, 2),  # Convert to ms
                'with_index': with_index,
                'status': 'success'
            }
            
            self.query_history.append(query_record)
            self.performance_metrics[query_type].append({
                'timestamp': timestamp,
                'duration': execution_time,
                'with_index': with_index
            })
    
    def get_recent_queries(self, limit=10):
        """Get recent query history"""
        with self.lock:
            return list(self.query_history)[-limit:]
    
    def get_latest_metrics(self):
        """Get latest performance metrics"""
        with self.lock:
            metrics = {}
            for query_type, records in self.performance_metrics.items():
                if records:
                    recent_records = records[-10:]  # Last 10 records
                    avg_duration = sum(r['duration'] for r in recent_records) / len(recent_records)
                    metrics[query_type] = {
                        'average_duration': round(avg_duration * 1000, 2),  # ms
                        'total_queries': len(records),
                        'last_execution': recent_records[-1]['timestamp'].isoformat()
                    }
            return metrics
    
    def get_performance_comparison(self, query_type):
        """Get performance comparison for a specific query type"""
        with self.lock:
            records = self.performance_metrics.get(query_type, [])
            
            with_index = [r for r in records if r['with_index']]
            without_index = [r for r in records if not r['with_index']]
            
            comparison = {}
            
            if with_index:
                comparison['with_index'] = {
                    'average': sum(r['duration'] for r in with_index) / len(with_index),
                    'count': len(with_index)
                }
            
            if without_index:
                comparison['without_index'] = {
                    'average': sum(r['duration'] for r in without_index) / len(without_index),
                    'count': len(without_index)
                }
            
            if 'with_index' in comparison and 'without_index' in comparison:
                improvement = ((comparison['without_index']['average'] - 
                              comparison['with_index']['average']) / 
                              comparison['without_index']['average']) * 100
                comparison['improvement_percent'] = round(improvement, 2)
            
            return comparison
