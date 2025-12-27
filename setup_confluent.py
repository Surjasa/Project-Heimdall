#!/usr/bin/env python3
"""
Confluent Cloud Setup Script
Sets up topics and validates streaming infrastructure
"""

import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add src to path
sys.path.append('src')

from streaming import demo_streaming_setup

def main():
    """Set up Confluent Cloud streaming infrastructure"""
    
    print("🌊 CONFLUENT CLOUD SETUP - STREAMING FOUNDATION")
    print("=" * 55)
    
    # Check environment variables
    required_vars = [
        "CONFLUENT_BOOTSTRAP_SERVERS",
        "CONFLUENT_API_KEY", 
        "CONFLUENT_API_SECRET"
    ]
    
    missing_vars = []
    placeholder_vars = []
    
    for var in required_vars:
        value = os.getenv(var)
        if not value:
            missing_vars.append(var)
        elif value.startswith("your-"):
            placeholder_vars.append(var)
    
    if missing_vars or placeholder_vars:
        print("⚠️  Confluent Cloud credentials not configured")
        if missing_vars:
            print("❌ Missing variables:")
            for var in missing_vars:
                print(f"   - {var}")
        if placeholder_vars:
            print("❌ Placeholder values detected:")
            for var in placeholder_vars:
                print(f"   - {var} = {os.getenv(var)}")
        
        print("\n📋 Setup Instructions:")
        print("1. Create Confluent Cloud account at https://confluent.cloud")
        print("2. Create a new cluster (Basic tier is fine for development)")
        print("3. Create API Key & Secret in the cluster")
        print("4. Update your .env file with real values:")
        print("   CONFLUENT_BOOTSTRAP_SERVERS=pkc-xxxxx.region.provider.confluent.cloud:9092")
        print("   CONFLUENT_API_KEY=your-actual-api-key")
        print("   CONFLUENT_API_SECRET=your-actual-api-secret")
        print("\n🧪 Running in mock mode for now...")
    
    try:
        # Run the streaming setup (will use mock mode if no real credentials)
        success = demo_streaming_setup()
        
        if success:
            print("\n" + "=" * 55)
            print("✅ STREAMING FOUNDATION READY!")
            
            if missing_vars or placeholder_vars:
                print("🎯 Mock mode completed - ready for real Confluent setup")
                print("📝 Next: Configure real Confluent Cloud credentials")
            else:
                print("🎯 Real Confluent Cloud setup completed!")
                print("📝 Next: Build transaction producer (Task 2.2)")
            
            return 0
        else:
            print("\n❌ Streaming setup failed")
            return 1
            
    except Exception as e:
        print(f"\n❌ Error during setup: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())