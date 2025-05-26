import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import json
import uuid
from typing import List, Dict, Any

class TelcoDataGenerator:
    def __init__(self):
        self.fake = Faker('id_ID')  # Indonesian locale
        self.call_types = ['voice', 'video', 'conference']
        self.network_types = ['2G', '3G', '4G', '5G']
        self.plan_types = ['prepaid', 'postpaid']
        self.cities = ['Jakarta', 'Surabaya', 'Bandung', 'Medan', 'Semarang', 'Makassar', 'Palembang']
        self.app_categories = ['social_media', 'streaming', 'gaming', 'browsing', 'messaging', 'email']
    
    def generate_cdr_data(self, num_records=100000):
        """Generate Call Detail Records for Cassandra"""
        print(f"🔄 Generating {num_records} CDR records...")
        cdr_data = []
        customer_ids = [f"CUST_{str(i).zfill(6)}" for i in range(1, 50001)]
        
        for i in range(num_records):
            call_start = self.fake.date_time_between(start_date='-1y', end_date='now')
            duration = random.randint(10, 3600)  # 10 seconds to 1 hour
            call_end = call_start + timedelta(seconds=duration)
            
            record = {
                'call_id': str(uuid.uuid4()),
                'caller_id': random.choice(customer_ids),
                'callee_id': random.choice(customer_ids),
                'call_start_time': call_start,
                'call_end_time': call_end,
                'duration_seconds': duration,
                'call_type': random.choice(self.call_types),
                'location_cell_id': f"CELL_{random.randint(1000, 9999)}",
                'location_lat': float(self.fake.latitude()),
                'location_lon': float(self.fake.longitude()),
                'cost_amount': round(random.uniform(0.1, 10.0), 2),
                'network_type': random.choice(self.network_types),
                'quality_score': random.randint(1, 5),
                'created_at': datetime.now()
            }
            
            cdr_data.append(record)
            
            if i % 10000 == 0 and i > 0:
                print(f"Generated {i} CDR records...")
        
        print(f"✅ Generated {len(cdr_data)} CDR records")
        return cdr_data
    
    def generate_sms_data(self, num_records=50000):
        """Generate SMS records for Cassandra"""
        print(f"🔄 Generating {num_records} SMS records...")
        sms_data = []
        customer_ids = [f"CUST_{str(i).zfill(6)}" for i in range(1, 50001)]
        delivery_statuses = ['delivered', 'pending', 'failed']
        
        for i in range(num_records):
            record = {
                'sms_id': str(uuid.uuid4()),
                'sender_id': random.choice(customer_ids),
                'receiver_id': random.choice(customer_ids),
                'message_length': random.randint(1, 160),
                'sent_time': self.fake.date_time_between(start_date='-1y', end_date='now'),
                'delivery_status': random.choice(delivery_statuses),
                'cost_amount': round(random.uniform(0.05, 0.5), 2),
                'network_type': random.choice(self.network_types),
                'created_at': datetime.now()
            }
            
            sms_data.append(record)
            
            if i % 10000 == 0 and i > 0:
                print(f"Generated {i} SMS records...")
        
        print(f"✅ Generated {len(sms_data)} SMS records")
        return sms_data
    
    def generate_data_usage(self, num_records=75000):
        """Generate data usage records for Cassandra"""
        print(f"🔄 Generating {num_records} data usage records...")
        data_usage = []
        customer_ids = [f"CUST_{str(i).zfill(6)}" for i in range(1, 50001)]
        
        for i in range(num_records):
            session_start = self.fake.date_time_between(start_date='-1y', end_date='now')
            session_duration = random.randint(60, 7200)  # 1 minute to 2 hours
            session_end = session_start + timedelta(seconds=session_duration)
            
            record = {
                'usage_id': str(uuid.uuid4()),
                'customer_id': random.choice(customer_ids),
                'session_start': session_start,
                'session_end': session_end,
                'data_consumed_mb': random.randint(1, 1000),
                'app_category': random.choice(self.app_categories),
                'network_type': random.choice(self.network_types),
                'cost_amount': round(random.uniform(0.01, 5.0), 2),
                'created_at': datetime.now()
            }
            
            data_usage.append(record)
            
            if i % 10000 == 0 and i > 0:
                print(f"Generated {i} data usage records...")
        
        print(f"✅ Generated {len(data_usage)} data usage records")
        return data_usage
    
    def generate_customer_data(self, num_customers=50000):
        """Generate Customer data for MongoDB"""
        print(f"🔄 Generating {num_customers} customer records...")
        customers = []
        subscriptions = []
        billing_records = []
        support_tickets = []
        
        for i in range(1, num_customers + 1):
            customer_id = f"CUST_{str(i).zfill(6)}"
            
            # Customer document
            registration_date = self.fake.date_time_between(start_date='-2y', end_date='now')
            customer = {
                "customer_id": customer_id,
                "personal_info": {
                    "first_name": self.fake.first_name(),
                    "last_name": self.fake.last_name(),
                    "email": self.fake.email(),
                    "phone_number": self.fake.phone_number(),
                    "date_of_birth": self.fake.date_of_birth(minimum_age=18, maximum_age=80).isoformat(),
                    "gender": random.choice(['M', 'F']),
                    "id_number": self.fake.ssn()
                },
                "address": {
                    "street": self.fake.street_address(),
                    "city": random.choice(self.cities),
                    "province": self.fake.state(),
                    "postal_code": self.fake.postcode(),
                    "country": "Indonesia"
                },
                "location": {
                    "city": random.choice(self.cities),
                    "coordinates": [float(self.fake.longitude()), float(self.fake.latitude())]
                },
                "registration_date": registration_date,
                "status": random.choice(['active', 'inactive', 'suspended']),
                "credit_score": random.randint(300, 850),
                "customer_segment": random.choice(['basic', 'premium', 'enterprise']),
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
            
            customers.append(customer)
            
            # Subscription document
            subscription = {
                "customer_id": customer_id,
                "plan_type": random.choice(self.plan_types),
                "plan_name": random.choice(['Basic', 'Standard', 'Premium', 'Unlimited']),
                "monthly_fee": random.choice([50000, 100000, 150000, 200000, 300000]),
                "data_quota_gb": random.choice([5, 10, 25, 50, 100]),
                "voice_minutes": random.choice([300, 500, 1000, 2000, -1]),  # -1 for unlimited
                "sms_quota": random.choice([100, 500, 1000, -1]),
                "start_date": registration_date,
                "end_date": None if customer["status"] == "active" else self.fake.date_time_between(start_date=registration_date, end_date='now'),
                "status": customer["status"],
                "features": random.sample(['5G', 'international_roaming', 'hotspot', 'music_streaming', 'video_streaming'], k=random.randint(1, 3)),
                "created_at": datetime.now()
            }
            
            subscriptions.append(subscription)
            
            # Generate billing records for last 12 months
            for month in range(12):
                billing_date = datetime.now() - timedelta(days=30*month)
                billing = {
                    "customer_id": customer_id,
                    "billing_month": billing_date.strftime("%Y-%m"),
                    "amount": subscription["monthly_fee"] + random.randint(-10000, 50000),
                    "usage": {
                        "data_used_gb": random.uniform(0, subscription["data_quota_gb"]),
                        "voice_minutes_used": random.randint(0, subscription["voice_minutes"] if subscription["voice_minutes"] > 0 else 1000),
                        "sms_sent": random.randint(0, subscription["sms_quota"] if subscription["sms_quota"] > 0 else 500)
                    },
                    "payment_status": random.choice(['paid', 'pending', 'overdue']),
                    "payment_date": billing_date + timedelta(days=random.randint(1, 30)) if random.random() > 0.1 else None,
                    "created_at": billing_date
                }
                
                billing_records.append(billing)
            
            # Generate support tickets (some customers)
            if random.random() < 0.3:  # 30% of customers have support tickets
                for _ in range(random.randint(1, 3)):
                    ticket = {
                        "ticket_id": f"TKT_{random.randint(100000, 999999)}",
                        "customer_id": customer_id,
                        "issue_type": random.choice(['billing', 'technical', 'service', 'complaint']),
                        "priority": random.choice(['low', 'medium', 'high', 'urgent']),
                        "status": random.choice(['open', 'in_progress', 'resolved', 'closed']),
                        "description": self.fake.text(max_nb_chars=200),
                        "ticket_date": self.fake.date_time_between(start_date=registration_date, end_date='now'),
                        "resolution_date": self.fake.date_time_between(start_date=registration_date, end_date='now') if random.random() > 0.3 else None,
                        "agent_id": f"AGENT_{random.randint(1, 100)}",
                        "created_at": datetime.now()
                    }
                    
                    support_tickets.append(ticket)
            
            if i % 5000 == 0:
                print(f"Generated {i} customer records...")
        
        print(f"✅ Generated {len(customers)} customers, {len(subscriptions)} subscriptions, {len(billing_records)} billing records, {len(support_tickets)} support tickets")
        return customers, subscriptions, billing_records, support_tickets
