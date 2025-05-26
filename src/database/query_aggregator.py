import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from .cassandra_manager import CassandraManager
from .mongodb_manager import MongoManager

class QueryAggregator:
    def __init__(self, cassandra_manager: CassandraManager, mongo_manager: MongoManager):
        self.cassandra = cassandra_manager
        self.mongo = mongo_manager
        self.logger = logging.getLogger(__name__)
    
    def query_db1_call_analytics(self, start_date: datetime, end_date: datetime, 
                                call_type: Optional[str] = None) -> Dict[str, Any]:
        """
        Query 1: Analisis volume panggilan dari Cassandra (DB1)
        Menganalisis volume panggilan berdasarkan tipe dan jaringan
        """
        self.logger.info(f"🔍 Query DB1: Analyzing call volume from {start_date} to {end_date}")
        start_time = time.time()
        
        try:
            # Build query with optional call type filter
            base_query = """
            SELECT call_type, network_type, COUNT(*) as call_count,
                   AVG(duration_seconds) as avg_duration,
                   SUM(cost_amount) as total_cost
            FROM call_records
            WHERE call_start_time >= ? AND call_start_time <= ?
            """
            
            parameters = [start_date, end_date]
            
            if call_type:
                base_query += " AND call_type = ?"
                parameters.append(call_type)
            
            base_query += " GROUP BY call_type, network_type ALLOW FILTERING"
            
            # Execute query
            results = self.cassandra.execute_query(base_query, parameters)
            
            # Process results
            processed_results = []
            total_calls = 0
            total_revenue = 0
            
            for row in results:
                call_count = int(row.get('call_count', 0))
                total_cost = float(row.get('total_cost', 0))
                
                processed_results.append({
                    'call_type': row.get('call_type'),
                    'network_type': row.get('network_type'),
                    'call_count': call_count,
                    'avg_duration': round(float(row.get('avg_duration', 0)), 2),
                    'total_cost': round(total_cost, 2)
                })
                
                total_calls += call_count
                total_revenue += total_cost
            
            execution_time = time.time() - start_time
            
            return {
                'query_type': 'DB1_ONLY',
                'database': 'Cassandra',
                'table': 'call_records',
                'results': processed_results,
                'summary': {
                    'total_calls': total_calls,
                    'total_revenue': round(total_revenue, 2),
                    'period': f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
                },
                'execution_time': execution_time,
                'record_count': len(processed_results)
            }
            
        except Exception as e:
            self.logger.error(f"❌ Query DB1 execution error: {e}")
            return {
                'query_type': 'DB1_ONLY',
                'database': 'Cassandra',
                'error': str(e),
                'execution_time': time.time() - start_time
            }
    
    def query_db2_customer_insights(self, segment: Optional[str] = None, 
                                   plan_type: Optional[str] = None) -> Dict[str, Any]:
        """
        Query 2: Analisis segmentasi pelanggan dari MongoDB (DB2)
        Menganalisis profil pelanggan berdasarkan segmen dan tipe paket
        """
        self.logger.info(f"🔍 Query DB2: Customer segmentation analysis")
        start_time = time.time()
        
        try:
            # Build aggregation pipeline
            pipeline = [
                {
                    "$lookup": {
                        "from": "subscriptions",
                        "localField": "customer_id",
                        "foreignField": "customer_id",
                        "as": "subscription"
                    }
                },
                {"$unwind": "$subscription"},
                {
                    "$lookup": {
                        "from": "billing",
                        "localField": "customer_id",
                        "foreignField": "customer_id",
                        "as": "billing_history"
                    }
                }
            ]
            
            # Add filters if specified
            match_conditions = {}
            if segment:
                match_conditions["customer_segment"] = segment
            if plan_type:
                match_conditions["subscription.plan_type"] = plan_type
            
            if match_conditions:
                pipeline.insert(-1, {"$match": match_conditions})
            
            # Group and aggregate
            pipeline.extend([
                {
                    "$group": {
                        "_id": {
                            "segment": "$customer_segment",
                            "plan_type": "$subscription.plan_type",
                            "city": "$location.city"
                        },
                        "customer_count": {"$sum": 1},
                        "avg_monthly_fee": {"$avg": "$subscription.monthly_fee"},
                        "avg_credit_score": {"$avg": "$credit_score"},
                        "total_revenue": {"$sum": "$subscription.monthly_fee"}
                    }
                },
                {"$sort": {"customer_count": -1}}
            ])
            
            # Execute aggregation
            results = self.mongo.execute_aggregation('customers', pipeline)
            
            # Process results
            processed_results = []
            total_customers = 0
            total_revenue = 0
            
            for row in results:
                customer_count = row.get('customer_count', 0)
                revenue = row.get('total_revenue', 0)
                
                processed_results.append({
                    'segment': row['_id'].get('segment'),
                    'plan_type': row['_id'].get('plan_type'),
                    'city': row['_id'].get('city'),
                    'customer_count': customer_count,
                    'avg_monthly_fee': round(row.get('avg_monthly_fee', 0), 2),
                    'avg_credit_score': round(row.get('avg_credit_score', 0), 2),
                    'total_revenue': round(revenue, 2)
                })
                
                total_customers += customer_count
                total_revenue += revenue
            
            execution_time = time.time() - start_time
            
            return {
                'query_type': 'DB2_ONLY',
                'database': 'MongoDB',
                'collections': ['customers', 'subscriptions', 'billing'],
                'results': processed_results,
                'summary': {
                    'total_customers': total_customers,
                    'total_revenue': round(total_revenue, 2),
                    'segments_analyzed': len(set(r['segment'] for r in processed_results))
                },
                'execution_time': execution_time,
                'record_count': len(processed_results)
            }
            
        except Exception as e:
            self.logger.error(f"❌ Query DB2 execution error: {e}")
            return {
                'query_type': 'DB2_ONLY',
                'database': 'MongoDB',
                'error': str(e),
                'execution_time': time.time() - start_time
            }
    
    def query_combined_customer_behavior(self, month: str, limit: int = 50) -> Dict[str, Any]:
        """
        Query 3: Analisis gabungan customer behavior dari kedua DB
        Menggabungkan data aktivitas panggilan (Cassandra) dengan profil pelanggan (MongoDB)
        """
        self.logger.info(f"🔍 Query Combined: Customer behavior analysis for {month}")
        start_time = time.time()
        
        try:
            # Step 1: Get call activity from Cassandra
            self.logger.info("Step 1: Getting call activity from Cassandra...")
            
            # Parse month to get date range
            year, month_num = map(int, month.split('-'))
            start_date = datetime(year, month_num, 1)
            if month_num == 12:
                end_date = datetime(year + 1, 1, 1)
            else:
                end_date = datetime(year, month_num + 1, 1)
            
            call_query = """
            SELECT caller_id, COUNT(*) as total_calls,
                   SUM(duration_seconds) as total_duration,
                   SUM(cost_amount) as total_cost
            FROM call_records
            WHERE call_start_time >= ? AND call_start_time < ?
            GROUP BY caller_id
            ORDER BY total_calls DESC
            LIMIT ?
            ALLOW FILTERING
            """
            
            call_results = self.cassandra.execute_query(
                call_query, [start_date, end_date, limit * 2]  # Get more records for filtering
            )
            
            # Convert to dictionary for easier lookup
            call_activity = {}
            for row in call_results:
                caller_id = row.get('caller_id')
                if caller_id:
                    call_activity[caller_id] = {
                        'total_calls': int(row.get('total_calls', 0)),
                        'total_duration': int(row.get('total_duration', 0)),
                        'total_cost': float(row.get('total_cost', 0))
                    }
            
            # Step 2: Get customer profiles from MongoDB
            self.logger.info("Step 2: Getting customer profiles from MongoDB...")
            
            customer_ids = list(call_activity.keys())
            
            # Get customer profiles with subscription and billing info
            pipeline = [
                {"$match": {"customer_id": {"$in": customer_ids}}},
                {
                    "$lookup": {
                        "from": "subscriptions",
                        "localField": "customer_id",
                        "foreignField": "customer_id",
                        "as": "subscription"
                    }
                },
                {"$unwind": "$subscription"},
                {
                    "$lookup": {
                        "from": "billing",
                        "localField": "customer_id",
                        "foreignField": "customer_id",
                        "as": "billing_history"
                    }
                },
                {
                    "$project": {
                        "customer_id": 1,
                        "personal_info.first_name": 1,
                        "personal_info.last_name": 1,
                        "customer_segment": 1,
                        "location.city": 1,
                        "subscription.plan_type": 1,
                        "subscription.monthly_fee": 1,
                        "status": 1,
                        "billing_count": {"$size": "$billing_history"}
                    }
                }
            ]
            
            customer_profiles = self.mongo.execute_aggregation('customers', pipeline)
            
            # Step 3: Combine results
            self.logger.info("Step 3: Combining results from both databases...")
            combined_results = []
            
            for customer in customer_profiles:
                customer_id = customer.get('customer_id')
                if customer_id in call_activity:
                    call_data = call_activity[customer_id]
                    
                    # Calculate usage efficiency
                    monthly_fee = customer.get('subscription', {}).get('monthly_fee', 1)
                    usage_efficiency = round((call_data['total_cost'] / monthly_fee) * 100, 2) if monthly_fee > 0 else 0
                    
                    combined_result = {
                        'customer_id': customer_id,
                        'name': f"{customer.get('personal_info', {}).get('first_name', '')} {customer.get('personal_info', {}).get('last_name', '')}".strip(),
                        'segment': customer.get('customer_segment'),
                        'plan_type': customer.get('subscription', {}).get('plan_type'),
                        'city': customer.get('location', {}).get('city'),
                        'monthly_fee': monthly_fee,
                        'status': customer.get('status'),
                        'total_calls': call_data['total_calls'],
                        'total_call_duration': call_data['total_duration'],
                        'total_call_cost': round(call_data['total_cost'], 2),
                        'usage_efficiency': usage_efficiency,
                        'billing_records': customer.get('billing_count', 0)
                    }
                    
                    combined_results.append(combined_result)
            
            # Sort by total calls (descending) and limit results
            combined_results.sort(key=lambda x: x['total_calls'], reverse=True)
            combined_results = combined_results[:limit]
            
            execution_time = time.time() - start_time
            
            # Calculate summary statistics
            total_calls = sum(r['total_calls'] for r in combined_results)
            total_revenue = sum(r['total_call_cost'] for r in combined_results)
            avg_efficiency = sum(r['usage_efficiency'] for r in combined_results) / len(combined_results) if combined_results else 0
            
            return {
                'query_type': 'COMBINED',
                'databases': ['Cassandra', 'MongoDB'],
                'tables_collections': ['call_records', 'customers', 'subscriptions', 'billing'],
                'results': combined_results,
                'summary': {
                    'total_calls': total_calls,
                    'total_revenue': round(total_revenue, 2),
                    'avg_usage_efficiency': round(avg_efficiency, 2),
                    'month_analyzed': month
                },
                'execution_time': execution_time,
                'record_count': len(combined_results)
            }
            
        except Exception as e:
            self.logger.error(f"❌ Combined query execution error: {e}")
            return {
                'query_type': 'COMBINED',
                'databases': ['Cassandra', 'MongoDB'],
                'error': str(e),
                'execution_time': time.time() - start_time
            }
    
    def performance_comparison(self) -> Dict[str, Any]:
        """
        Compare query performance with and without indexes
        Menjalankan benchmark untuk mengukur improvement dari indexing
        """
        self.logger.info("🔍 Running performance comparison...")
        results = {}
        
        # Test parameters
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
        test_month = "2024-01"
        
        # Test Query 1: Call Analytics (DB1)
        self.logger.info("Testing Query 1: Call Analytics (Cassandra)")
        
        # Test without indexes
        self.cassandra.drop_indexes()
        time.sleep(1)  # Allow time for index drops to take effect
        
        start_time = time.time()
        result1_no_idx = self.query_db1_call_analytics(start_date, end_date)
        no_index_time_q1 = time.time() - start_time
        
        # Test with indexes
        self.cassandra.create_indexes()
        time.sleep(2)  # Allow time for indexes to be built
        
        start_time = time.time()
        result1_with_idx = self.query_db1_call_analytics(start_date, end_date)
        with_index_time_q1 = time.time() - start_time
        
        results['query1_call_analytics'] = {
            'without_index': round(no_index_time_q1, 4),
            'with_index': round(with_index_time_q1, 4),
            'improvement_percent': round(((no_index_time_q1 - with_index_time_q1) / no_index_time_q1) * 100, 2) if no_index_time_q1 > 0 else 0,
            'records_processed': result1_with_idx.get('record_count', 0)
        }
        
        # Test Query 2: Customer Insights (DB2)
        self.logger.info("Testing Query 2: Customer Insights (MongoDB)")
        
        # Test without indexes
        for collection in ['customers', 'subscriptions', 'billing']:
            self.mongo.drop_indexes(collection)
        time.sleep(1)
        
        start_time = time.time()
        result2_no_idx = self.query_db2_customer_insights()
        no_index_time_q2 = time.time() - start_time
        
        # Test with indexes
        self.mongo.create_collections_and_indexes()
        time.sleep(2)
        
        start_time = time.time()
        result2_with_idx = self.query_db2_customer_insights()
        with_index_time_q2 = time.time() - start_time
        
        results['query2_customer_insights'] = {
            'without_index': round(no_index_time_q2, 4),
            'with_index': round(with_index_time_q2, 4),
            'improvement_percent': round(((no_index_time_q2 - with_index_time_q2) / no_index_time_q2) * 100, 2) if no_index_time_q2 > 0 else 0,
            'records_processed': result2_with_idx.get('record_count', 0)
        }
        
        # Test Query 3: Combined Analysis
        self.logger.info("Testing Query 3: Combined Customer Behavior")
        
        # Test without indexes (indexes already dropped above)
        start_time = time.time()
        result3_no_idx = self.query_combined_customer_behavior(test_month, 25)
        no_index_time_q3 = time.time() - start_time
        
        # Test with indexes (indexes already created above)
        start_time = time.time()
        result3_with_idx = self.query_combined_customer_behavior(test_month, 25)
        with_index_time_q3 = time.time() - start_time
        
        results['query3_combined'] = {
            'without_index': round(no_index_time_q3, 4),
            'with_index': round(with_index_time_q3, 4),
            'improvement_percent': round(((no_index_time_q3 - with_index_time_q3) / no_index_time_q3) * 100, 2) if no_index_time_q3 > 0 else 0,
            'records_processed': result3_with_idx.get('record_count', 0)
        }
        
        # Calculate overall statistics
        improvements = [r['improvement_percent'] for r in results.values() if r['improvement_percent'] > 0]
        avg_improvement = sum(improvements) / len(improvements) if improvements else 0
        
        total_time_without = sum(r['without_index'] for r in results.values())
        total_time_with = sum(r['with_index'] for r in results.values())
        
        self.logger.info(f"✅ Performance comparison completed. Average improvement: {avg_improvement:.2f}%")
        
        return {
            'individual_queries': results,
            'summary': {
                'average_improvement': round(avg_improvement, 2),
                'total_time_without_indexes': round(total_time_without, 4),
                'total_time_with_indexes': round(total_time_with, 4),
                'total_time_saved': round(total_time_without - total_time_with, 4),
                'best_improvement': max(improvements) if improvements else 0,
                'worst_improvement': min(improvements) if improvements else 0,
                'queries_tested': len(results)
            },
            'recommendations': self._generate_performance_recommendations(results)
        }
    
    def _generate_performance_recommendations(self, results: Dict) -> List[str]:
        """Generate performance optimization recommendations"""
        recommendations = []
        
        for query_name, metrics in results.items():
            improvement = metrics.get('improvement_percent', 0)
            
            if improvement > 50:
                recommendations.append(f"✅ {query_name}: Excellent index performance ({improvement:.1f}% improvement)")
            elif improvement > 20:
                recommendations.append(f"⚠️ {query_name}: Good index performance ({improvement:.1f}% improvement)")
            elif improvement > 0:
                recommendations.append(f"🔍 {query_name}: Minimal index benefit ({improvement:.1f}% improvement) - consider query optimization")
            else:
                recommendations.append(f"❌ {query_name}: No improvement detected - review index strategy")
        
        # General recommendations
        avg_improvement = sum(r.get('improvement_percent', 0) for r in results.values()) / len(results)
        
        if avg_improvement > 40:
            recommendations.append("🎉 Overall: Excellent indexing strategy - maintain current approach")
        elif avg_improvement > 20:
            recommendations.append("👍 Overall: Good indexing strategy - consider additional optimizations")
        else:
            recommendations.append("🔧 Overall: Review indexing strategy and query patterns")
        
        return recommendations
