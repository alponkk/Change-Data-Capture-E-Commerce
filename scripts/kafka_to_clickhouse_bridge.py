#!/usr/bin/env python3
"""
Kafka to ClickHouse Bridge Script
=================================
This script consumes CDC data from Kafka topics and loads it into ClickHouse tables.
Uses confluent-kafka and clickhouse-connect for better Python 3.13 compatibility.
"""

import json
import time
import logging
from confluent_kafka import Consumer, KafkaError
import clickhouse_connect

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class KafkaClickHouseBridge:
    def __init__(self):
        # Kafka configuration
        self.kafka_config = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'clickhouse-bridge-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True
        }
        
        # ClickHouse configuration
        self.clickhouse_client = clickhouse_connect.get_client(
            host='localhost',
            port=8123,
            username='default',
            password='',
            database='default'
        )
        
        # Topic to table mapping
        self.topic_table_mapping = {
            'mongo.ecom.ecom.customers': 'mongo_ecom_customers',
            'mongo.ecom.ecom.products': 'mongo_ecom_products', 
            'mongo.ecom.ecom.orders': 'mongo_ecom_orders'
        }
        
    def create_tables(self):
        """Create ClickHouse tables if they don't exist"""
        tables = [
            """
            CREATE TABLE IF NOT EXISTS mongo_ecom_customers (
                raw_data String,
                _kafka_timestamp DateTime DEFAULT now(),
                _kafka_offset UInt64,
                _kafka_partition UInt32
            ) ENGINE = MergeTree()
            ORDER BY _kafka_timestamp
            """,
            """
            CREATE TABLE IF NOT EXISTS mongo_ecom_products (
                raw_data String,
                _kafka_timestamp DateTime DEFAULT now(),
                _kafka_offset UInt64,
                _kafka_partition UInt32
            ) ENGINE = MergeTree()
            ORDER BY _kafka_timestamp
            """,
            """
            CREATE TABLE IF NOT EXISTS mongo_ecom_orders (
                raw_data String,
                _kafka_timestamp DateTime DEFAULT now(),
                _kafka_offset UInt64,
                _kafka_partition UInt32
            ) ENGINE = MergeTree()
            ORDER BY _kafka_timestamp
            """
        ]
        
        for table_sql in tables:
            try:
                self.clickhouse_client.command(table_sql.strip())
                logger.info(f"✅ Table created/verified successfully")
            except Exception as e:
                logger.error(f"❌ Error creating table: {e}")
                
    def test_connections(self):
        """Test Kafka and ClickHouse connections"""
        try:
            # Test ClickHouse
            result = self.clickhouse_client.command("SELECT 1")
            logger.info("✅ ClickHouse connection successful")
            
            # Test Kafka (create consumer briefly)
            test_consumer = Consumer(self.kafka_config)
            topics = test_consumer.list_topics(timeout=10)
            test_consumer.close()
            logger.info(f"✅ Kafka connection successful. Available topics: {len(topics.topics)}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Connection test failed: {e}")
            return False
    
    def consume_and_load(self, max_messages=None, timeout_seconds=None, realtime=True):
        """Consume messages from Kafka and load into ClickHouse"""
        logger.info("🚀 Starting Kafka to ClickHouse bridge...")
        
        # Subscribe to all CDC topics
        topics = list(self.topic_table_mapping.keys())
        logger.info(f"📡 Subscribing to topics: {topics}")
        
        try:
            consumer = Consumer(self.kafka_config)
            consumer.subscribe(topics)
            
            message_count = 0
            batch_size = 100
            batch_data = {table: [] for table in self.topic_table_mapping.values()}
            start_time = time.time()
            
            if realtime:
                logger.info("⏳ Starting real-time streaming (runs continuously)...")
            else:
                logger.info(f"⏳ Batch mode - consuming messages (max: {max_messages}, timeout: {timeout_seconds}s)...")
            
            while True:
                msg = consumer.poll(1.0)
                
                if msg is None:
                    # In real-time mode, continue indefinitely; in batch mode, check timeout
                    if not realtime and timeout_seconds and time.time() - start_time > timeout_seconds:
                        logger.info(f"⏰ Timeout reached ({timeout_seconds}s)")
                        break
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.info(f"📄 End of partition reached for {msg.topic()}")
                        continue
                    else:
                        logger.error(f"❌ Kafka error: {msg.error()}")
                        continue
                
                try:
                    topic = msg.topic()
                    table_name = self.topic_table_mapping.get(topic)
                    
                    if not table_name:
                        logger.warning(f"⚠️ Unknown topic: {topic}")
                        continue
                    
                    # Get message data
                    message_value = msg.value().decode('utf-8') if msg.value() else ''
                    
                    # Skip delete events and empty messages
                    if not message_value or message_value.strip() == '':
                        continue
                        
                    # Parse JSON to check for delete events
                    try:
                        json_data = json.loads(message_value)
                        # Skip if this is a delete event
                        if json_data.get('__deleted') == True:
                            logger.debug(f"Skipping delete event for topic {topic}")
                            continue
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid JSON in message: {message_value[:100]}...")
                        continue
                    
                    # Prepare data for insertion
                    data_row = [
                        message_value,  # raw_data
                        int(time.time()),  # _kafka_timestamp
                        msg.offset(),  # _kafka_offset
                        msg.partition()  # _kafka_partition
                    ]
                    
                    batch_data[table_name].append(data_row)
                    message_count += 1
                    
                    # Insert in batches
                    if len(batch_data[table_name]) >= batch_size:
                        self._insert_batch(table_name, batch_data[table_name])
                        batch_data[table_name] = []
                    
                    # Periodic status in real-time mode
                    if realtime and message_count % 100 == 0 and message_count > 0:
                        elapsed = time.time() - start_time
                        rate = message_count / elapsed if elapsed > 0 else 0
                        logger.info(f"📊 Real-time: Processed {message_count} messages ({rate:.1f} msg/sec)")
                    
                    # In batch mode, check message limit; in real-time mode, continue indefinitely
                    if not realtime and max_messages and message_count >= max_messages:
                        logger.info(f"🎯 Reached max messages limit: {max_messages}")
                        break
                        
                except Exception as e:
                    logger.error(f"❌ Error processing message: {e}")
                    continue
            
            # Insert remaining batches
            for table_name, data in batch_data.items():
                if data:
                    self._insert_batch(table_name, data)
            
            consumer.close()
            logger.info(f"✅ Successfully processed {message_count} messages")
            
        except Exception as e:
            logger.error(f"❌ Error in consume_and_load: {e}")
    
    def _insert_batch(self, table_name, data):
        """Insert a batch of data into ClickHouse"""
        try:
            columns = ['raw_data', '_kafka_timestamp', '_kafka_offset', '_kafka_partition']
            self.clickhouse_client.insert(table_name, data, column_names=columns)
            logger.info(f"📊 Inserted {len(data)} records into {table_name}")
        except Exception as e:
            logger.error(f"❌ Error inserting into {table_name}: {e}")
    
    def clear_tables(self):
        """Clear all ClickHouse tables"""
        logger.info("🧹 Clearing ClickHouse tables...")
        
        for table_name in self.topic_table_mapping.values():
            try:
                self.clickhouse_client.command(f"TRUNCATE TABLE {table_name}")
                logger.info(f"✅ Cleared {table_name}")
            except Exception as e:
                logger.error(f"❌ Error clearing {table_name}: {e}")

    def show_stats(self):
        """Show table statistics"""
        logger.info("📈 Table Statistics:")
        logger.info("=" * 50)
        
        for table_name in self.topic_table_mapping.values():
            try:
                result = self.clickhouse_client.command(f"SELECT COUNT(*) FROM {table_name}")
                count = result if isinstance(result, int) else 0
                logger.info(f"📋 {table_name}: {count:,} records")
            except Exception as e:
                logger.error(f"❌ Error checking {table_name}: {e}")

def main():
    """Main execution function"""
    import sys
    
    bridge = KafkaClickHouseBridge()
    
    print("🔗 Kafka to ClickHouse Bridge")
    print("=" * 40)
    
    # Check for mode argument
    mode = "realtime"  # default to real-time
    reset_offset = False
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "--batch":
            mode = "batch"
        elif sys.argv[1] == "--realtime":
            mode = "realtime"
        elif sys.argv[1] == "--reset":
            mode = "batch"
            reset_offset = True
        elif sys.argv[1] in ["--help", "-h"]:
            print("Usage:")
            print("  python kafka_to_clickhouse_bridge.py --realtime  (default: runs continuously)")
            print("  python kafka_to_clickhouse_bridge.py --batch     (runs once and exits)")
            print("  python kafka_to_clickhouse_bridge.py --reset     (clears ClickHouse data and reads from beginning)")
            print("  python kafka_to_clickhouse_bridge.py --help      (show this help)")
            return
    
    # Test connections
    if not bridge.test_connections():
        print("❌ Connection tests failed. Exiting.")
        return
    
    # Create tables
    print("🏗️ Creating/verifying ClickHouse tables...")
    bridge.create_tables()
    
    # Handle reset mode
    if reset_offset:
        print("🧹 Clearing existing data...")
        bridge.clear_tables()
        # Use a unique consumer group to start from beginning
        bridge.kafka_config['group.id'] = f"clickhouse-bridge-reset-{int(time.time())}"
        print("🔄 Using fresh consumer group to read from beginning...")
    
    # Consume and load data
    if mode == "realtime":
        print("🚀 Starting real-time streaming pipeline...")
        print("💡 This will run continuously. Press Ctrl+C to stop.")
        try:
            bridge.consume_and_load(realtime=True)
        except KeyboardInterrupt:
            print("\n⏹️ Real-time streaming stopped by user.")
    else:
        if reset_offset:
            print("🚀 Starting fresh data load from beginning...")
            bridge.consume_and_load(max_messages=5000, timeout_seconds=120, realtime=False)
        else:
            print("🚀 Starting batch data transfer...")
            bridge.consume_and_load(max_messages=3500, timeout_seconds=60, realtime=False)
        print("✨ Batch process completed!")
    
    # Show final statistics
    bridge.show_stats()

if __name__ == "__main__":
    main() 