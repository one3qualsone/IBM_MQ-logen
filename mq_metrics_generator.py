#!/usr/bin/env python3
"""
IBM MQ Metrics Generator for Elastic Demo
Generates realistic MQ queue depth metrics with anomaly scenarios
"""

import json
import random
import time
import os
from datetime import datetime, timedelta
from typing import Dict, List
import requests
import math

class MQMetricsGenerator:
    def __init__(self, es_url: str, es_api_key: str, index_name: str):
        self.es_url = es_url.rstrip('/')
        self.es_api_key = es_api_key
        self.index_name = index_name
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'ApiKey {es_api_key}',
            'Content-Type': 'application/json'
        })
        
        # Disable SSL warnings if needed (for demo purposes)
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        # Queue definitions - realistic for a bank payments system
        self.queues = [
            {
                "name": "PAYMENT.REQUEST.IN",        # Generic payment request pattern
                "qmgr": "QMPAYMENTS01",
                "max_depth": 5000,
                "normal_depth_range": (100, 500),
                "normal_rate_range": (50, 100),
                "priority": "critical"
            },
            {
                "name": "PAYMENT.RESPONSE.OUT",      # Corresponding response queue
                "qmgr": "QMPAYMENTS01",
                "max_depth": 5000,
                "normal_depth_range": (80, 400),
                "normal_rate_range": (40, 90),
                "priority": "critical"
            },
            {
                "name": "SWIFT.MT.OUTBOUND",         # SWIFT is industry standard - keep as is
                "qmgr": "QMPAYMENTS01", 
                "max_depth": 3000,
                "normal_depth_range": (50, 300),
                "normal_rate_range": (20, 60),
                "priority": "high"
            },
            {
                "name": "PAYMENT.ERROR.DLQ",         # Dead letter queue pattern
                "qmgr": "QMPAYMENTS01",
                "max_depth": 1000,
                "normal_depth_range": (0, 50),
                "normal_rate_range": (1, 10),
                "priority": "medium"
            },
            {
                "name": "ISO20022.TRANSFORM.IN",     # ISO20022 is industry standard
                "qmgr": "QMPAYMENTS01",
                "max_depth": 2000,
                "normal_depth_range": (100, 600),
                "normal_rate_range": (30, 80),
                "priority": "high"
            }
        ]
        # State tracking for realistic metrics
        self.queue_state = {}
        self.cumulative_counters = {}
        for queue in self.queues:
            queue_key = f"{queue['qmgr']}:{queue['name']}"
            self.queue_state[queue_key] = {
                "current_depth": random.randint(*queue['normal_depth_range']),
                "scenario": "normal"
            }
            self.cumulative_counters[queue_key] = {
                "input_count": random.randint(1000000, 5000000),
                "output_count": random.randint(1000000, 5000000)
            }
    
    def test_connection(self) -> bool:
        """Test connection to Elasticsearch"""
        print("🔌 Testing connection to Elasticsearch...")
        try:
            response = self.session.get(f"{self.es_url}/")
            
            if response.status_code == 200:
                cluster_info = response.json()
                print(f"✅ Connected to Elasticsearch")
                print(f"   Cluster: {cluster_info.get('cluster_name', 'unknown')}")
                print(f"   Version: {cluster_info.get('version', {}).get('number', 'unknown')}")
                return True
            else:
                print(f"❌ Connection failed: HTTP {response.status_code}")
                print(f"   Response: {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ Connection error: {e}")
            return False
    
    def ensure_index_exists(self) -> bool:
        """Create index if it doesn't exist"""
        print(f"📊 Checking if index '{self.index_name}' exists...")
        
        try:
            # Check if index exists
            response = self.session.head(f"{self.es_url}/{self.index_name}")
            
            if response.status_code == 200:
                print(f"✅ Index '{self.index_name}' already exists")
                return True
            
            # For data streams (metrics-*, logs-*, traces-*), they auto-create
            # So we'll just let Elasticsearch handle it
            if self.index_name.startswith(('metrics-', 'logs-', 'traces-')):
                print(f"✅ Data stream '{self.index_name}' will auto-create on first write")
                return True
            
            # Create regular index with mapping
            print(f"📝 Creating index '{self.index_name}'...")
            
            index_body = {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 1
                },
                "mappings": {
                    "properties": {
                        "@timestamp": {"type": "date"},
                        "prometheus": {
                            "properties": {
                                "labels": {
                                    "properties": {
                                        "qmgr": {"type": "keyword"},
                                        "queue": {"type": "keyword"},
                                        "cluster": {"type": "keyword"},
                                        "priority": {"type": "keyword"}
                                    }
                                },
                                "metrics": {
                                    "properties": {
                                        "ibmmq_queue_depth": {"type": "long"},
                                        "ibmmq_queue_max_depth": {"type": "long"},
                                        "ibmmq_queue_input_count": {"type": "long"},
                                        "ibmmq_queue_output_count": {"type": "long"},
                                        "ibmmq_queue_input_rate": {"type": "long"},
                                        "ibmmq_queue_output_rate": {"type": "long"},
                                        "ibmmq_queue_oldest_message_age": {"type": "long"},
                                        "ibmmq_queue_utilisation_pct": {"type": "float"}
                                    }
                                }
                            }
                        },
                        "host": {
                            "properties": {
                                "name": {"type": "keyword"},
                                "hostname": {"type": "keyword"}
                            }
                        },
                        "event": {
                            "properties": {
                                "dataset": {"type": "keyword"},
                                "module": {"type": "keyword"},
                                "kind": {"type": "keyword"}
                            }
                        }
                    }
                }
            }
            
            response = self.session.put(
                f"{self.es_url}/{self.index_name}",
                json=index_body
            )
            
            if response.status_code in [200, 201]:
                print(f"✅ Index '{self.index_name}' created successfully")
                return True
            else:
                print(f"❌ Failed to create index: HTTP {response.status_code}")
                print(f"   Response: {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ Error checking/creating index: {e}")
            return False
    
    def get_weekly_pattern_multiplier(self, timestamp: datetime) -> float:
        """Generate realistic weekly patterns"""
        weekday = timestamp.weekday()  # 0=Monday, 6=Sunday
        
        if weekday == 0:  # Monday - busiest
            return 1.4
        elif weekday == 1:  # Tuesday
            return 1.3
        elif weekday == 2:  # Wednesday
            return 1.2
        elif weekday == 3:  # Thursday
            return 1.1
        elif weekday == 4:  # Friday - winding down
            return 0.9
        elif weekday == 5:  # Saturday - minimal
            return 0.3
        else:  # Sunday - minimal
            return 0.2
    
    def get_monthly_pattern_multiplier(self, timestamp: datetime) -> float:
        """Generate realistic monthly patterns (end-of-month processing spikes)"""
        day = timestamp.day
        
        # End of month (last 3 days) - payment processing rush
        if day >= 28:
            return 1.5
        # Start of month (first 3 days) - statement processing
        elif day <= 3:
            return 1.3
        # Mid-month (around 15th) - payroll processing
        elif 14 <= day <= 16:
            return 1.2
        else:
            return 1.0
    
    def generate_daily_pattern_multiplier(self, hour: int) -> float:
        """Generate realistic hourly patterns for UK banking hours"""
        if 0 <= hour < 6:
            return 0.3  # Night time - minimal activity
        elif 6 <= hour < 9:
            return 0.6  # Early morning ramp up
        elif 9 <= hour < 11:
            return 1.3  # Morning peak
        elif 11 <= hour < 14:
            return 1.0  # Lunch period
        elif 14 <= hour < 16:
            return 1.4  # Afternoon peak
        elif 16 <= hour < 18:
            return 0.9  # Wind down
        else:
            return 0.5  # Evening
    
    def determine_scenario(self, timestamp: datetime, start_time: datetime) -> str:
        """Determine scenario based on timeline with multiple realistic anomalies"""
        days_elapsed = (timestamp - start_time).days
        hour = timestamp.hour
        weekday = timestamp.weekday()
        
        # Subtle anomaly patterns throughout the period
        
        # Week 1: Normal baseline
        if days_elapsed < 7:
            return "normal"
        
        # Week 2: Introduce subtle slowdowns on Monday mornings (pattern 1)
        elif 7 <= days_elapsed < 14:
            if weekday == 0 and 9 <= hour < 11:  # Monday morning
                return "subtle_degradation"
            return "normal"
        
        # Week 3: Database connection pool issues (gradual degradation pattern)
        elif 14 <= days_elapsed < 21:
            if 14 <= hour < 17:  # Afternoon hours
                return "gradual_degradation"
            return "normal"
        
        # Week 4: Multiple small incidents
        elif 21 <= days_elapsed < 28:
            # Mini outage on day 24 at 14:30 for 8 minutes
            if days_elapsed == 24:
                mini_outage_start = start_time + timedelta(days=24, hours=14, minutes=30)
                mini_outage_end = mini_outage_start + timedelta(minutes=8)
                if mini_outage_start <= timestamp < mini_outage_end:
                    return "mini_outage"
            
            # End-of-month processing spike causing buildup
            if days_elapsed >= 27:
                return "critical_buildup"
            
            return "normal"
        
        # Week 5: Major incident (the 19-minute outage)
        elif 28 <= days_elapsed < 35:
            if days_elapsed == 30:  # Day 30
                outage_start = start_time + timedelta(days=30, hours=14, minutes=30)
                outage_end = outage_start + timedelta(minutes=19)
                
                if outage_start <= timestamp < outage_end:
                    return "queue_full"
                elif timestamp >= outage_end and timestamp < outage_end + timedelta(hours=2):
                    return "recovery"
            
            if days_elapsed == 31:  # Day after major incident
                return "recovery"
            
            return "normal"
        
        # Weeks 6-8: Pattern learning opportunities
        elif 35 <= days_elapsed < 56:
            # Recurring pattern: SWIFT processing slowdown every Wednesday 2-4pm
            if weekday == 2 and 14 <= hour < 16:
                return "swift_slowdown"
            
            # ISO20022 transformation queue buildup on Fridays
            if weekday == 4 and 10 <= hour < 15:
                return "iso_buildup"
            
            return "normal"
        
        # Weeks 9-12: Stable with occasional spikes
        elif 56 <= days_elapsed < 84:
            # Random mini-spikes during peak hours (predictable time, unpredictable day)
            if 9 <= hour < 11 or 14 <= hour < 16:
                if random.random() < 0.05:  # 5% chance during peak hours
                    return "spike"
            
            return "normal"
        
        # Week 13+: Return to baseline with learned patterns
        else:
            return "normal"
    
    def calculate_queue_depth(self, queue: Dict, scenario: str, timestamp: datetime) -> Dict:
        """Calculate realistic queue depth based on scenario and patterns"""
        queue_key = f"{queue['qmgr']}:{queue['name']}"
        current_state = self.queue_state[queue_key]
        
        # Combine all pattern multipliers
        hour_multiplier = self.generate_daily_pattern_multiplier(timestamp.hour)
        weekly_multiplier = self.get_weekly_pattern_multiplier(timestamp)
        monthly_multiplier = self.get_monthly_pattern_multiplier(timestamp)
        
        combined_multiplier = hour_multiplier * weekly_multiplier * monthly_multiplier
        
        # Base values
        min_depth, max_depth = queue['normal_depth_range']
        min_rate, max_rate = queue['normal_rate_range']
        
        # Scenario-specific behaviour
        if scenario == "normal":
            target_depth = int(random.uniform(min_depth, max_depth) * combined_multiplier)
            input_rate = int(random.uniform(min_rate, max_rate) * combined_multiplier)
            output_rate = input_rate + random.randint(-5, 5)
        
        elif scenario == "subtle_degradation":
            # Very subtle - 10% slower processing
            target_depth = int(current_state['current_depth'] * 1.02)
            input_rate = int(random.uniform(min_rate, max_rate) * combined_multiplier)
            output_rate = int(input_rate * 0.9)
        
        elif scenario == "gradual_degradation":
            # Gradual buildup - 5% growth per interval
            target_depth = int(current_state['current_depth'] * 1.05)
            target_depth = min(target_depth, queue['max_depth'] - 500)
            input_rate = int(random.uniform(min_rate, max_rate) * combined_multiplier)
            output_rate = int(input_rate * 0.7)
        
        elif scenario == "critical_buildup":
            # Severe processing issues - 10% growth
            target_depth = int(current_state['current_depth'] * 1.1)
            target_depth = min(target_depth, int(queue['max_depth'] * 0.95))
            input_rate = int(random.uniform(min_rate, max_rate) * combined_multiplier)
            output_rate = int(input_rate * 0.4)
        
        elif scenario == "queue_full":
            # Complete stall
            target_depth = queue['max_depth']
            input_rate = 0
            output_rate = 0
        
        elif scenario == "mini_outage":
            # Partial stall - some processing continues
            target_depth = int(queue['max_depth'] * 0.8)
            input_rate = int(random.uniform(min_rate, max_rate) * combined_multiplier * 0.3)
            output_rate = int(input_rate * 0.2)
        
        elif scenario == "spike":
            # Sudden traffic spike
            target_depth = int(max_depth * 2 * combined_multiplier)
            target_depth = min(target_depth, int(queue['max_depth'] * 0.8))
            input_rate = int(max_rate * 2.5)
            output_rate = int(max_rate * 1.2)
        
        elif scenario == "swift_slowdown":
            # Specific to SWIFT queue
            if queue['name'] == "SWIFT.OUTBOUND":
                target_depth = int(current_state['current_depth'] * 1.08)
                target_depth = min(target_depth, int(queue['max_depth'] * 0.7))
                input_rate = int(random.uniform(min_rate, max_rate) * combined_multiplier)
                output_rate = int(input_rate * 0.5)
            else:
                target_depth = int(random.uniform(min_depth, max_depth) * combined_multiplier)
                input_rate = int(random.uniform(min_rate, max_rate) * combined_multiplier)
                output_rate = input_rate + random.randint(-5, 5)
        
        elif scenario == "iso_buildup":
            # Specific to ISO20022 transformation queue
            if queue['name'] == "ISO20022.TRANSFORM":
                target_depth = int(current_state['current_depth'] * 1.07)
                target_depth = min(target_depth, int(queue['max_depth'] * 0.8))
                input_rate = int(random.uniform(min_rate, max_rate) * combined_multiplier * 1.5)
                output_rate = int(input_rate * 0.8)
            else:
                target_depth = int(random.uniform(min_depth, max_depth) * combined_multiplier)
                input_rate = int(random.uniform(min_rate, max_rate) * combined_multiplier)
                output_rate = input_rate + random.randint(-5, 5)
        
        else:  # recovery
            # Catching up - 15% reduction
            target_depth = int(current_state['current_depth'] * 0.85)
            target_depth = max(target_depth, int((min_depth + max_depth) / 2))
            input_rate = int(random.uniform(min_rate, max_rate) * combined_multiplier)
            output_rate = int(input_rate * 1.3)
        
        # Smooth transitions
        current_depth = current_state['current_depth']
        new_depth = int(current_depth + (target_depth - current_depth) * 0.3)
        new_depth = max(0, min(new_depth, queue['max_depth']))
        
        # Update state
        self.queue_state[queue_key]['current_depth'] = new_depth
        
        # Calculate oldest message age
        if new_depth == 0:
            oldest_message_age = 0
        else:
            age_factor = new_depth / queue['max_depth']
            oldest_message_age = int(30 + (age_factor * 600))
        
        # Update cumulative counters
        counters = self.cumulative_counters[queue_key]
        counters['input_count'] += max(0, input_rate)
        counters['output_count'] += max(0, output_rate)
        
        utilisation_pct = round((new_depth / queue['max_depth']) * 100, 2)
        
        return {
            "queue_depth": new_depth,
            "input_rate": input_rate,
            "output_rate": output_rate,
            "oldest_message_age": oldest_message_age,
            "utilisation_pct": utilisation_pct,
            "input_count_cumulative": counters['input_count'],
            "output_count_cumulative": counters['output_count']
        }
    
    def generate_document(self, queue: Dict, scenario: str, timestamp: datetime) -> Dict:
        """Generate a complete Elasticsearch document"""
        metrics = self.calculate_queue_depth(queue, scenario, timestamp)
        
        doc = {
            "@timestamp": timestamp.isoformat() + "Z",
            "metricset": {
                "name": "collector",
                "module": "prometheus"
            },
            "service": {
                "type": "prometheus",
                "address": "mqserver01:9157"
            },
            "prometheus": {
                "labels": {
                    "qmgr": queue['qmgr'],
                    "queue": queue['name'],
                    "cluster": "PAYMENTS_CLUSTER",
                    "priority": queue['priority']
                },
                "metrics": {
                    "ibmmq_queue_depth": metrics['queue_depth'],
                    "ibmmq_queue_max_depth": queue['max_depth'],
                    "ibmmq_queue_input_count": metrics['input_count_cumulative'],
                    "ibmmq_queue_output_count": metrics['output_count_cumulative'],
                    "ibmmq_queue_input_rate": metrics['input_rate'],
                    "ibmmq_queue_output_rate": metrics['output_rate'],
                    "ibmmq_queue_oldest_message_age": metrics['oldest_message_age'],
                    "ibmmq_queue_utilisation_pct": metrics['utilisation_pct']
                }
            },
            "host": {
                "name": "mqprod01.bank.local",
                "hostname": "mqprod01"
            },
            "event": {
                "dataset": "prometheus.collector",
                "module": "prometheus",
                "kind": "metric"
            },
            "agent": {
                "type": "metricbeat",
                "version": "8.11.0"
            }
        }
        
        return doc
    
    def bulk_index_documents(self, documents: List[Dict]) -> bool:
        """Index documents using Elasticsearch bulk API"""
        if not documents:
            return True
        
        # Build bulk request body
        bulk_body = []
        for doc in documents:
            # Use 'create' op_type for data streams (metrics-*, logs-*, traces-*)
            bulk_body.append(json.dumps({"create": {"_index": self.index_name}}))
            # Document
            bulk_body.append(json.dumps(doc))
        
        bulk_data = '\n'.join(bulk_body) + '\n'
        
        try:
            response = self.session.post(
                f"{self.es_url}/_bulk",
                headers={"Content-Type": "application/x-ndjson"},
                data=bulk_data
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('errors'):
                    # Print detailed error information
                    print(f"\n❌ BULK INDEX ERRORS:")
                    for item in result.get('items', []):
                        if 'create' in item and 'error' in item['create']:
                            error = item['create']['error']
                            print(f"   Error type: {error.get('type')}")
                            print(f"   Reason: {error.get('reason')}")
                            if 'caused_by' in error:
                                print(f"   Caused by: {error['caused_by']}")
                    print()
                    return False
                return True
            else:
                print(f"\n❌ BULK INDEX FAILED:")
                print(f"   HTTP Status: {response.status_code}")
                print(f"   Response: {response.text[:500]}")
                print()
                return False
                
        except Exception as e:
            print(f"\n❌ EXCEPTION DURING BULK INDEX:")
            print(f"   {type(e).__name__}: {e}")
            print()
            return False
    
    def run_continuous(self, interval_seconds: int = 60, scenario: str = "normal"):
        """Run continuous data generation"""
        print(f"🚀 Starting continuous MQ metrics generation")
        print(f"📊 Index: {self.index_name}")
        print(f"📡 Elasticsearch: {self.es_url}")
        print(f"⏱️  Interval: {interval_seconds} seconds")
        print(f"🎬 Scenario: {scenario}")
        print(f"📝 Queues: {len(self.queues)}")
        print(f"\nPress Ctrl+C to stop\n")
        
        iteration = 0
        try:
            while True:
                iteration += 1
                timestamp = datetime.utcnow()
                
                documents = []
                for queue in self.queues:
                    doc = self.generate_document(queue, scenario, timestamp)
                    documents.append(doc)
                
                success = self.bulk_index_documents(documents)
                
                if success:
                    depths = [self.queue_state[f"{q['qmgr']}:{q['name']}"]['current_depth'] 
                             for q in self.queues]
                    avg_depth = sum(depths) / len(depths)
                    max_depth = max(depths)
                    critical_queues = sum(1 for d in depths if d > 4000)
                    
                    print(f"✅ [{iteration:04d}] {timestamp.strftime('%Y-%m-%d %H:%M:%S')} | "
                          f"Docs: {len(documents)} | "
                          f"Avg depth: {avg_depth:.0f} | "
                          f"Max depth: {max_depth} | "
                          f"Critical: {critical_queues} | "
                          f"Scenario: {scenario}")
                else:
                    print(f"❌ [{iteration:04d}] Failed to index documents")
                
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            print(f"\n\n🛑 Stopped after {iteration} iterations")
    
    def backfill_historical_data(self, days: int = 90, interval_minutes: int = 1):
        """Generate historical data for ML training"""
        print(f"📅 Generating {days} days of historical data")
        print(f"📊 Index: {self.index_name}")
        print(f"📡 Elasticsearch: {self.es_url}")
        print(f"⏱️  Interval: {interval_minutes} minute(s)")
        print(f"📝 Queues: {len(self.queues)}")
        print(f"📈 Expected total documents: ~{days * 24 * 60 * len(self.queues) // interval_minutes:,}")
        
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=days)
        current_time = start_time
        
        total_docs = 0
        batch_size = 100
        documents = []
        
        print(f"\n📖 Anomaly Pattern Timeline:")
        print(f"   Week 1:     Baseline (normal operations)")
        print(f"   Week 2:     Subtle Monday morning slowdowns")
        print(f"   Week 3:     Afternoon degradation pattern")
        print(f"   Week 4:     Mini-outage + end-of-month spike")
        print(f"   Week 5:     Major 19-minute outage on day 30")
        print(f"   Weeks 6-8:  Recurring Wed SWIFT slowdown, Fri ISO buildup")
        print(f"   Weeks 9-12: Random peak-hour spikes (ML training data)")
        print(f"   Week 13+:   Return to baseline\n")
        
        last_scenario = "normal"
        scenario_change_count = 0
        
        while current_time <= end_time:
            scenario = self.determine_scenario(current_time, start_time)
            
            # Track scenario changes for better logging
            if scenario != last_scenario:
                scenario_change_count += 1
                last_scenario = scenario
            
            for queue in self.queues:
                doc = self.generate_document(queue, scenario, current_time)
                documents.append(doc)
                total_docs += 1
            
            # Bulk index in batches
            if len(documents) >= batch_size:
                success = self.bulk_index_documents(documents)
                if success:
                    progress = ((current_time - start_time).total_seconds() / 
                               (end_time - start_time).total_seconds() * 100)
                    
                    # Show day of week and scenario for context
                    day_name = current_time.strftime('%a')
                    
                    print(f"📝 [{progress:5.1f}%] {total_docs:>7,} docs | "
                          f"{current_time.strftime('%Y-%m-%d %H:%M')} ({day_name}) | "
                          f"Scenario: {scenario:20s} | "
                          f"Changes: {scenario_change_count}")
                else:
                    print(f"❌ Failed batch at {current_time.strftime('%Y-%m-%d %H:%M')}")
                documents = []
            
            current_time += timedelta(minutes=interval_minutes)
        
        # Index remaining documents
        if documents:
            self.bulk_index_documents(documents)
        
        print(f"\n✅ Historical backfill complete!")
        print(f"   📊 Total documents: {total_docs:,}")
        print(f"   📅 Time range: {start_time.strftime('%Y-%m-%d')} to {end_time.strftime('%Y-%m-%d')}")
        print(f"   🔄 Scenario changes: {scenario_change_count}")
        print(f"   🎯 Ready for ML job training!")


def main():
    # Load configuration from environment variables
    es_url = os.getenv('ES_URL')
    es_api_key = os.getenv('ES_API_KEY')
    index_name = os.getenv('INDEX_NAME', 'metrics-mq-demo')
    mode = os.getenv('MODE', 'backfill')
    
    # Validate required config
    if not es_url:
        print("❌ ERROR: ES_URL environment variable is required")
        return 1
    
    if not es_api_key:
        print("❌ ERROR: ES_API_KEY environment variable is required")
        return 1
    
    print("=" * 60)
    print("IBM MQ Metrics Generator for Elastic")
    print("=" * 60)
    
    generator = MQMetricsGenerator(
        es_url=es_url,
        es_api_key=es_api_key,
        index_name=index_name
    )
    
    # Test connection first
    if not generator.test_connection():
        print("\n❌ Cannot proceed without valid connection")
        return 1
    
    # Ensure index exists
    if not generator.ensure_index_exists():
        print("\n❌ Cannot proceed without valid index")
        return 1
    
    print()  # Blank line before starting generation
    
    if mode == 'continuous':
        scenario = os.getenv('CONTINUOUS_SCENARIO', 'normal')
        interval = int(os.getenv('CONTINUOUS_INTERVAL_SECONDS', '60'))
        generator.run_continuous(
            interval_seconds=interval,
            scenario=scenario
        )
    else:  # backfill
        days = int(os.getenv('BACKFILL_DAYS', '90'))
        interval_minutes = int(os.getenv('BACKFILL_INTERVAL_MINUTES', '1'))
        generator.backfill_historical_data(
            days=days,
            interval_minutes=interval_minutes
        )
    
    return 0


if __name__ == '__main__':
    exit(main())