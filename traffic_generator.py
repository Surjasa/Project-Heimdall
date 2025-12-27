#!/usr/bin/env python3
"""
Traffic Generator - Continuous Transaction Generation with Chaos Mode
Generates realistic transaction load for testing and demonstration
"""

import os
import sys
import time
import signal
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add src to path
sys.path.append('src')

from transaction_producer import TransactionProducer
from streaming import ConfluentStreamManager, create_stream_config_from_env

class TrafficGenerator:
    """Manages continuous traffic generation with chaos engineering"""
    
    def __init__(self):
        self.producer = None
        self.stream_manager = None
        self.running = False
    
    def setup(self):
        """Set up streaming and producer"""
        # Create stream config
        stream_config = create_stream_config_from_env()
        
        # Check if we have real Confluent credentials
        if (stream_config.bootstrap_servers and 
            not stream_config.bootstrap_servers.startswith("your-")):
            
            print("🌊 Setting up Confluent Cloud streaming...")
            self.stream_manager = ConfluentStreamManager(stream_config)
            
            # Test connection
            if not self.stream_manager.test_connection():
                print("❌ Failed to connect to Confluent Cloud")
                return False
            
            # Create topics if needed
            if not self.stream_manager.create_topics():
                print("❌ Failed to create topics")
                return False
                
            print("✅ Confluent Cloud connected and ready")
        else:
            print("⚠️  No Confluent Cloud credentials - running in local mode")
        
        # Create transaction producer
        self.producer = TransactionProducer(self.stream_manager)
        return True
    
    def start_traffic(self, rate_per_second: float = 2.0, chaos_probability: float = 0.3):
        """Start generating traffic"""
        if not self.producer:
            print("❌ Producer not initialized")
            return False
        
        print(f"🚀 Starting traffic generation:")
        print(f"   📊 Rate: {rate_per_second} transactions/second")
        print(f"   🚨 Chaos probability: {chaos_probability*100:.0f}%")
        print(f"   🌊 Streaming: {'Confluent Cloud' if self.stream_manager else 'Local only'}")
        print()
        print("Press Ctrl+C to stop...")
        
        # Set up signal handler for graceful shutdown
        def signal_handler(sig, frame):
            print("\n⏹️  Stopping traffic generation...")
            self.stop_traffic()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        
        # Start continuous generation
        self.producer.start_continuous_generation(rate_per_second, chaos_probability)
        self.running = True
        
        # Keep main thread alive and show stats
        transaction_count = 0
        start_time = time.time()
        
        try:
            while self.running:
                time.sleep(10)  # Update stats every 10 seconds
                transaction_count += rate_per_second * 10
                elapsed = time.time() - start_time
                
                print(f"📊 Stats: {int(transaction_count)} transactions in {elapsed:.0f}s "
                      f"(avg {transaction_count/elapsed:.1f} TPS)")
                
        except KeyboardInterrupt:
            self.stop_traffic()
    
    def stop_traffic(self):
        """Stop traffic generation"""
        self.running = False
        if self.producer:
            self.producer.stop_continuous_generation()
        if self.stream_manager:
            self.stream_manager.cleanup()
        print("✅ Traffic generation stopped")


def main():
    """Main traffic generator function"""
    
    print("🚛 FRAUD DETECTION TRAFFIC GENERATOR")
    print("=" * 45)
    
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description="Generate transaction traffic for fraud detection testing")
    parser.add_argument("--rate", type=float, default=2.0, help="Transactions per second (default: 2.0)")
    parser.add_argument("--chaos", type=float, default=0.3, help="Chaos probability 0-1 (default: 0.3)")
    parser.add_argument("--demo", action="store_true", help="Run quick demo instead of continuous generation")
    
    args = parser.parse_args()
    
    if args.demo:
        # Run quick demo
        print("🧪 Running demo mode...")
        from transaction_producer import demo_transaction_producer
        success = demo_transaction_producer()
        return 0 if success else 1
    
    # Validate arguments
    if args.rate <= 0 or args.rate > 100:
        print("❌ Rate must be between 0 and 100 TPS")
        return 1
    
    if args.chaos < 0 or args.chaos > 1:
        print("❌ Chaos probability must be between 0 and 1")
        return 1
    
    # Create and setup traffic generator
    generator = TrafficGenerator()
    
    if not generator.setup():
        print("❌ Failed to setup traffic generator")
        return 1
    
    # Start traffic generation
    try:
        generator.start_traffic(args.rate, args.chaos)
        return 0
    except Exception as e:
        print(f"❌ Error during traffic generation: {e}")
        return 1


if __name__ == "__main__":
    exit(main())