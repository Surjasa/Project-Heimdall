#!/usr/bin/env python3
"""
Real-Time Fraud Detection Consumer
The "glue" code that connects streaming to AI fraud detection
"""

import json
import os
import time
import logging
from typing import Dict, Any
from confluent_kafka import Consumer, Producer, KafkaError
from decimal import Decimal
from datetime import datetime

from fraud_detection import FraudDetectionService, Transaction, Location, UserContext
from streaming import create_stream_config_from_env

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FraudConsumer:
    """Real-time fraud detection consumer"""
    
    def __init__(self):
        self.fraud_service = FraudDetectionService()
        self.consumer = None
        self.producer = None
        self.running = False
        
        # Get stream configuration
        self.stream_config = create_stream_config_from_env()
        
        # Check if we have real credentials
        self.use_mock_mode = (
            not self.stream_config.bootstrap_servers or 
            self.stream_config.bootstrap_servers.startswith("your-")
        )
    
    def setup_kafka_clients(self):
        """Set up Kafka consumer and producer"""
        if self.use_mock_mode:
            logger.info("⚠️  Mock mode - no real Kafka clients")
            return True
        
        # Consumer configuration
        consumer_conf = {
            'bootstrap.servers': self.stream_config.bootstrap_servers,
            'sasl.mechanisms': 'PLAIN',
            'security.protocol': 'SASL_SSL',
            'sasl.username': self.stream_config.api_key,
            'sasl.password': self.stream_config.api_secret,
            'group.id': 'fraud-detection-service-v1',
            'auto.offset.reset': 'latest',  # Focus on real-time traffic
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 1000,
        }
        
        # Producer configuration (for alerts)
        producer_conf = {
            'bootstrap.servers': self.stream_config.bootstrap_servers,
            'sasl.mechanisms': 'PLAIN',
            'security.protocol': 'SASL_SSL',
            'sasl.username': self.stream_config.api_key,
            'sasl.password': self.stream_config.api_secret,
            'acks': 'all',
            'retries': 3,
        }
        
        try:
            self.consumer = Consumer(consumer_conf)
            self.producer = Producer(producer_conf)
            
            # Subscribe to transaction stream
            self.consumer.subscribe([self.stream_config.topic_name])
            
            logger.info("✅ Kafka clients configured successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to setup Kafka clients: {e}")
            return False
    
    def parse_transaction(self, transaction_data: Dict[str, Any]) -> Transaction:
        """Parse transaction data from Kafka message"""
        location_data = transaction_data.get('location', {})
        location = Location(
            country=location_data.get('country', 'Unknown'),
            city=location_data.get('city', 'Unknown'),
            coordinates=tuple(location_data.get('coordinates', [0.0, 0.0])),
            is_new_location=location_data.get('is_new_location', False),
            distance_from_home=location_data.get('distance_from_home', 0.0)
        )
        
        return Transaction(
            transaction_id=transaction_data.get('transaction_id', ''),
            user_id=transaction_data.get('user_id', ''),
            amount=Decimal(str(transaction_data.get('amount', 0))),
            currency=transaction_data.get('currency', 'USD'),
            merchant=transaction_data.get('merchant', ''),
            merchant_category=transaction_data.get('merchant_category', ''),
            location=location,
            timestamp=datetime.fromisoformat(transaction_data.get('timestamp', datetime.now().isoformat())),
            payment_method=transaction_data.get('payment_method', 'credit_card'),
            metadata=transaction_data.get('metadata', {})
        )
    
    def create_user_context(self, transaction: Transaction) -> UserContext:
        """Create user context for fraud analysis"""
        # In a real system, this would fetch from user database
        # For demo, create reasonable defaults
        return UserContext(
            user_id=transaction.user_id,
            home_location=Location(
                country="United States",
                city="San Francisco",
                coordinates=(37.7749, -122.4194),
                is_new_location=False,
                distance_from_home=0.0
            ),
            spending_patterns={
                "avg_transaction": 150.0,
                "max_transaction": 800.0,
                "common_categories": ["grocery", "gas", "restaurants"],
                "common_countries": ["United States"]
            },
            recent_transactions=[],
            risk_profile="low",
            phone_number="+1-555-0123"
        )
    
    def publish_fraud_alert(self, transaction_data: Dict[str, Any], analysis_result: Dict[str, Any]):
        """Publish high-risk transaction to fraud alerts topic"""
        if self.use_mock_mode:
            logger.info(f"🚨 MOCK ALERT: High Risk ({analysis_result.get('risk_score')}) detected!")
            return
        
        alert_payload = {
            "transaction": transaction_data,
            "analysis": {
                "risk_score": analysis_result.risk_score,
                "confidence": analysis_result.confidence,
                "risk_factors": analysis_result.risk_factors,
                "recommendation": analysis_result.recommendation,
                "model_used": analysis_result.model_used,
                "processing_time_ms": analysis_result.processing_time_ms
            },
            "status": "pending_voice_verification",
            "timestamp": datetime.now().isoformat()
        }
        
        try:
            self.producer.produce(
                f"{self.stream_config.topic_name}-high-risk",
                key=transaction_data.get('transaction_id'),
                value=json.dumps(alert_payload, default=str)
            )
            self.producer.flush()
            logger.info(f"🚨 ALERT: High Risk ({analysis_result.risk_score}) published to fraud_alerts!")
            
        except Exception as e:
            logger.error(f"❌ Failed to publish alert: {e}")
    
    def process_transaction(self, transaction_data: Dict[str, Any]):
        """Process a single transaction through fraud detection"""
        try:
            # Parse transaction
            transaction = self.parse_transaction(transaction_data)
            user_context = self.create_user_context(transaction)
            
            logger.info(f"📥 Processing Txn: {transaction.transaction_id} - ${transaction.amount}")
            
            # AI Analysis (The "Brain" call)
            # This triggers Datadog LLM tracing automatically
            start_time = time.time()
            analysis_result = self.fraud_service.analyze_transaction_realtime(transaction, user_context)
            processing_time = (time.time() - start_time) * 1000
            
            logger.info(f"🧠 AI Analysis: Risk={analysis_result.risk_score}/100, "
                       f"Confidence={analysis_result.confidence:.2f}, "
                       f"Time={processing_time:.0f}ms")
            
            # Action Gate: If Risk > 70, Alert!
            if analysis_result.risk_score > 70:
                self.publish_fraud_alert(transaction_data, analysis_result)
            else:
                logger.info(f"✅ Transaction approved (Risk: {analysis_result.risk_score}/100)")
            
            return analysis_result
            
        except Exception as e:
            logger.error(f"❌ Error processing transaction: {e}")
            return None
    
    def run_fraud_consumer(self):
        """Main consumer loop"""
        logger.info("🚀 Fraud Detection Consumer Starting...")
        
        if not self.setup_kafka_clients():
            if not self.use_mock_mode:
                logger.error("❌ Failed to setup Kafka clients")
                return False
        
        if self.use_mock_mode:
            logger.info("🧪 Running in mock mode - simulating transaction processing")
            self.run_mock_consumer()
            return True
        
        logger.info("🚀 Consumer Active: Listening for transactions...")
        self.running = True
        
        try:
            while self.running:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        continue
                
                # Extract and process transaction
                try:
                    transaction_data = json.loads(msg.value().decode('utf-8'))
                    self.process_transaction(transaction_data)
                    
                except json.JSONDecodeError as e:
                    logger.error(f"❌ Invalid JSON in message: {e}")
                except Exception as e:
                    logger.error(f"❌ Error processing message: {e}")
                    
        except KeyboardInterrupt:
            logger.info("⏹️  Consumer stopped by user")
        finally:
            if self.consumer:
                self.consumer.close()
            logger.info("✅ Consumer shutdown complete")
        
        return True
    
    def run_mock_consumer(self):
        """Run consumer in mock mode for testing"""
        logger.info("🧪 Mock consumer simulating transaction processing...")
        
        # Simulate some transactions
        mock_transactions = [
            {
                "transaction_id": "txn_mock_001",
                "user_id": "user_12345",
                "amount": 150.00,
                "currency": "USD",
                "merchant": "Grocery Store",
                "merchant_category": "grocery",
                "location": {
                    "country": "United States",
                    "city": "San Francisco",
                    "coordinates": [37.7749, -122.4194],
                    "is_new_location": False,
                    "distance_from_home": 5.0
                },
                "timestamp": datetime.now().isoformat(),
                "payment_method": "credit_card",
                "metadata": {"transaction_type": "normal"}
            },
            {
                "transaction_id": "txn_mock_002",
                "user_id": "user_12345",
                "amount": 2500.00,
                "currency": "USD",
                "merchant": "Electronics Store",
                "merchant_category": "electronics",
                "location": {
                    "country": "Romania",
                    "city": "Bucharest",
                    "coordinates": [44.4268, 26.1025],
                    "is_new_location": True,
                    "distance_from_home": 5000.0
                },
                "timestamp": datetime.now().isoformat(),
                "payment_method": "credit_card",
                "metadata": {"transaction_type": "suspicious"}
            }
        ]
        
        for transaction_data in mock_transactions:
            logger.info(f"🧪 Processing mock transaction: {transaction_data['transaction_id']}")
            result = self.process_transaction(transaction_data)
            time.sleep(2)  # Simulate processing delay
        
        logger.info("✅ Mock consumer simulation complete")
    
    def stop(self):
        """Stop the consumer"""
        self.running = False


def main():
    """Main function to run the fraud consumer"""
    consumer = FraudConsumer()
    return consumer.run_fraud_consumer()


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)