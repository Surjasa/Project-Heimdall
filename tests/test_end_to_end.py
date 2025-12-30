#!/usr/bin/env python3
"""
End-to-End Test: Traffic Generator → Consumer → AI Analysis
Demonstrates the complete fraud detection pipeline
"""

import os
import sys
import time
import threading
import signal
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add src to path
sys.path.append('src')

from consumer import FraudConsumer
from transaction_producer import TransactionProducer
from streaming import ConfluentStreamManager, create_stream_config_from_env

class EndToEndDemo:
    """Demonstrates the complete fraud detection pipeline"""
    
    def __init__(self):
        self.consumer = None
        self.producer = None
        self.running = False
    
    def setup(self):
        """Set up the demo components"""
        print("🚀 END-TO-END FRAUD DETECTION DEMO")
        print("=" * 45)
        
        # Create stream config
        stream_config = create_stream_config_from_env()
        
        # Check if we have real Confluent credentials
        if (stream_config.bootstrap_servers and 
            not stream_config.bootstrap_servers.startswith("your-")):
            print("🌊 Using real Confluent Cloud streaming")
            stream_manager = ConfluentStreamManager(stream_config)
        else:
            print("🧪 Using mock mode (no real Confluent credentials)")
            stream_manager = None
        
        # Create components
        self.consumer = FraudConsumer()
        self.producer = TransactionProducer(stream_manager)
        
        return True
    
    def run_demo(self, duration_seconds: int = 30):
        """Run the end-to-end demo"""
        if not self.setup():
            print("❌ Failed to setup demo")
            return False
        
        print(f"\n🎬 Starting {duration_seconds}-second demo...")
        print("📊 You should see:")
        print("   1. Transactions being generated")
        print("   2. Consumer processing each transaction")
        print("   3. AI analysis with risk scores")
        print("   4. High-risk alerts (when risk > 70%)")
        print("\nPress Ctrl+C to stop early...\n")
        
        # Set up signal handler for graceful shutdown
        def signal_handler(sig, frame):
            print("\n⏹️  Stopping demo...")
            self.stop_demo()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        
        self.running = True
        
        # Start consumer in background thread
        consumer_thread = threading.Thread(
            target=self.consumer.run_fraud_consumer,
            daemon=True
        )
        consumer_thread.start()
        
        # Give consumer time to start
        time.sleep(2)
        
        # Start producer with chaos mode
        print("🚛 Starting transaction generation...")
        self.producer.start_continuous_generation(
            rate_per_second=1.0,  # 1 transaction per second
            chaos_probability=0.4  # 40% chance of suspicious transactions
        )
        
        # Run for specified duration
        try:
            time.sleep(duration_seconds)
        except KeyboardInterrupt:
            pass
        
        self.stop_demo()
        return True
    
    def stop_demo(self):
        """Stop the demo"""
        self.running = False
        
        if self.producer:
            self.producer.stop_continuous_generation()
        
        if self.consumer:
            self.consumer.stop()
        
        print("✅ Demo stopped")
    
    def run_quick_demo(self):
        """Run a quick demo with mock data"""
        print("🧪 QUICK DEMO MODE")
        print("=" * 25)
        
        # Just run the consumer in mock mode
        consumer = FraudConsumer()
        consumer.run_fraud_consumer()
        
        print("\n✅ Quick demo complete!")
        print("🎯 This demonstrates:")
        print("   ✓ Transaction parsing and validation")
        print("   ✓ AI fraud analysis integration")
        print("   ✓ Risk scoring and alert logic")
        print("   ✓ End-to-end data flow")


def main():
    """Main demo function"""
    
    import argparse
    parser = argparse.ArgumentParser(description="End-to-end fraud detection demo")
    parser.add_argument("--duration", type=int, default=30, help="Demo duration in seconds (default: 30)")
    parser.add_argument("--quick", action="store_true", help="Run quick mock demo instead")
    
    args = parser.parse_args()
    
    demo = EndToEndDemo()
    
    if args.quick:
        demo.run_quick_demo()
        return 0
    
    # Validate duration
    if args.duration < 5 or args.duration > 300:
        print("❌ Duration must be between 5 and 300 seconds")
        return 1
    
    try:
        success = demo.run_demo(args.duration)
        return 0 if success else 1
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        return 1


if __name__ == "__main__":
    exit(main())