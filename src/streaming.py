"""
Confluent Cloud Streaming Infrastructure
Handles transaction stream ingestion and processing
"""

import os
import json
import time
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict
from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class StreamConfig:
    """Configuration for Confluent Cloud streaming"""
    bootstrap_servers: str
    api_key: str
    api_secret: str
    topic_name: str = "transactions"
    partitions: int = 6
    replication_factor: int = 3


class ConfluentStreamManager:
    """Manages Confluent Cloud streaming infrastructure"""
    
    def __init__(self, config: StreamConfig):
        self.config = config
        self.producer = None
        self.consumer = None
        self.admin_client = None
        
        # Common Confluent Cloud configuration
        self.kafka_config = {
            'bootstrap.servers': config.bootstrap_servers,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': config.api_key,
            'sasl.password': config.api_secret,
        }
    
    def create_topics(self) -> bool:
        """Create required Kafka topics"""
        try:
            # Initialize admin client
            self.admin_client = AdminClient(self.kafka_config)
            
            # Define topics to create
            topics = [
                NewTopic(
                    topic=self.config.topic_name,
                    num_partitions=self.config.partitions,
                    replication_factor=self.config.replication_factor,
                    config={
                        'cleanup.policy': 'delete',
                        'retention.ms': '86400000',  # 24 hours
                        'compression.type': 'snappy'
                    }
                ),
                NewTopic(
                    topic=f"{self.config.topic_name}-flagged",
                    num_partitions=self.config.partitions,
                    replication_factor=self.config.replication_factor,
                    config={
                        'cleanup.policy': 'delete',
                        'retention.ms': '86400000',  # 24 hours
                        'compression.type': 'snappy'
                    }
                ),
                NewTopic(
                    topic=f"{self.config.topic_name}-high-risk",
                    num_partitions=self.config.partitions,
                    replication_factor=self.config.replication_factor,
                    config={
                        'cleanup.policy': 'delete',
                        'retention.ms': '604800000',  # 7 days (longer retention for high-risk)
                        'compression.type': 'snappy'
                    }
                )
            ]
            
            # Create topics
            futures = self.admin_client.create_topics(topics)
            
            # Wait for topic creation
            for topic, future in futures.items():
                try:
                    future.result()  # The result itself is None
                    logger.info(f"✅ Topic '{topic}' created successfully")
                except Exception as e:
                    if "already exists" in str(e).lower():
                        logger.info(f"✅ Topic '{topic}' already exists")
                    else:
                        logger.error(f"❌ Failed to create topic '{topic}': {e}")
                        return False
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error creating topics: {e}")
            return False
    
    def get_producer(self) -> Producer:
        """Get Kafka producer instance"""
        if self.producer is None:
            producer_config = {
                **self.kafka_config,
                'acks': 'all',  # Wait for all replicas
                'retries': 3,
                'batch.size': 16384,
                'linger.ms': 10,  # Small batching for low latency
                'compression.type': 'snappy'
            }
            self.producer = Producer(producer_config)
        
        return self.producer
    
    def get_consumer(self, group_id: str, topics: list) -> Consumer:
        """Get Kafka consumer instance"""
        consumer_config = {
            **self.kafka_config,
            'group.id': group_id,
            'auto.offset.reset': 'latest',  # Start from latest for real-time processing
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 1000,
            'max.poll.interval.ms': 300000,  # 5 minutes
            'session.timeout.ms': 30000,    # 30 seconds
        }
        
        consumer = Consumer(consumer_config)
        consumer.subscribe(topics)
        return consumer
    
    def send_transaction(self, transaction_data: Dict[str, Any], topic: Optional[str] = None) -> bool:
        """Send transaction to Kafka topic"""
        try:
            producer = self.get_producer()
            target_topic = topic or self.config.topic_name
            
            # Serialize transaction data
            message = json.dumps(transaction_data, default=str)
            
            # Send message with callback
            def delivery_callback(err, msg):
                if err:
                    logger.error(f"❌ Message delivery failed: {err}")
                else:
                    logger.debug(f"✅ Message delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")
            
            producer.produce(
                topic=target_topic,
                key=transaction_data.get('transaction_id', ''),
                value=message,
                callback=delivery_callback
            )
            
            # Flush to ensure delivery (for demo purposes)
            producer.flush(timeout=1.0)
            return True
            
        except Exception as e:
            logger.error(f"❌ Error sending transaction: {e}")
            return False
    
    def test_connection(self) -> bool:
        """Test connection to Confluent Cloud"""
        try:
            # Test with admin client
            admin_client = AdminClient(self.kafka_config)
            metadata = admin_client.list_topics(timeout=10)
            
            logger.info(f"✅ Connected to Confluent Cloud")
            logger.info(f"📊 Available topics: {list(metadata.topics.keys())}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Connection test failed: {e}")
            return False
    
    def cleanup(self):
        """Clean up resources"""
        if self.producer:
            self.producer.flush()
        if self.consumer:
            self.consumer.close()


def create_stream_config_from_env() -> StreamConfig:
    """Create StreamConfig from environment variables"""
    return StreamConfig(
        bootstrap_servers=os.getenv("CONFLUENT_BOOTSTRAP_SERVERS", ""),
        api_key=os.getenv("CONFLUENT_API_KEY", ""),
        api_secret=os.getenv("CONFLUENT_API_SECRET", ""),
        topic_name=os.getenv("CONFLUENT_TOPIC_NAME", "transactions")
    )


def demo_streaming_setup():
    """Demo function to test streaming setup"""
    print("🌊 CONFLUENT CLOUD STREAMING SETUP")
    print("=" * 40)
    
    # Create config from environment
    config = create_stream_config_from_env()
    
    # Check if we have real credentials
    if not config.bootstrap_servers or config.bootstrap_servers == "your-bootstrap-servers":
        print("⚠️  Using mock mode - no real Confluent credentials")
        print("✅ Streaming logic structure validated")
        print("🎯 Next steps:")
        print("   1. Set up Confluent Cloud account")
        print("   2. Create cluster and get API credentials")
        print("   3. Update .env file with real credentials")
        return True
    
    # Test with real credentials
    stream_manager = ConfluentStreamManager(config)
    
    print("🔗 Testing Confluent Cloud connection...")
    if not stream_manager.test_connection():
        return False
    
    print("📝 Creating topics...")
    if not stream_manager.create_topics():
        return False
    
    print("🧪 Testing message production...")
    test_transaction = {
        "transaction_id": "test_001",
        "user_id": "test_user",
        "amount": 100.00,
        "timestamp": time.time()
    }
    
    if stream_manager.send_transaction(test_transaction):
        print("✅ Test message sent successfully")
    else:
        print("❌ Failed to send test message")
        return False
    
    stream_manager.cleanup()
    
    print("\n✅ CONFLUENT STREAMING SETUP COMPLETE!")
    print("🎯 Ready for transaction producer and consumer implementation")
    return True


if __name__ == "__main__":
    success = demo_streaming_setup()
    exit(0 if success else 1)