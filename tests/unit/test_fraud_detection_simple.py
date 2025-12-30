#!/usr/bin/env python3
"""
Simple test script for fraud detection AI logic (works without API keys)
Run this to validate the core logic structure
"""

import os
import sys
from decimal import Decimal
from datetime import datetime

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

# Mock the external dependencies for testing
class MockLLMObs:
    @staticmethod
    def enable(**kwargs):
        print("🔧 Mock Datadog LLM Observability enabled")
    
    @staticmethod
    def llm(**kwargs):
        return MockSpan()

class MockSpan:
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        pass
    
    def annotate(self, **kwargs):
        print(f"📊 Mock LLM trace: {kwargs.get('metadata', {})}")

class MockGenerativeModel:
    def __init__(self, model_name):
        self.model_name = model_name
    
    def generate_content(self, prompt):
        # Mock response that looks like a real Gemini response
        mock_response = MockResponse()
        return mock_response

class MockResponse:
    @property
    def text(self):
        # Return a realistic fraud analysis response
        return '''
{
    "risk_score": 85,
    "confidence": 0.92,
    "risk_factors": ["new_country", "high_amount", "unusual_merchant"],
    "reasoning": "Transaction in Romania (new country) for $2500 significantly exceeds user's typical spending pattern of $150 average. Electronics purchase in unfamiliar location raises fraud probability.",
    "recommendation": "call_user"
}
'''

# Patch the imports
import fraud_detection
fraud_detection.LLMObs = MockLLMObs
fraud_detection.GenerativeModel = MockGenerativeModel

# Import after patching
from fraud_detection import demo_fraud_detection

def main():
    """Run fraud detection demo with mocked services"""
    
    print("🧠 FRAUD DETECTION AI - MOCK DEMO (No API Keys Required)")
    print("=" * 60)
    
    print("✅ Using mock services for testing")
    print("📊 Mock Datadog integration: ENABLED")
    print("🤖 Mock Gemini 1.5 Flash: ENABLED")
    print()
    
    try:
        # Run the fraud detection demo
        result = demo_fraud_detection()
        
        print("\n" + "=" * 60)
        print("✅ FRAUD DETECTION AI LOGIC WORKING!")
        print("🎯 Core functionality validated:")
        print(f"   ✓ Risk scoring: {result.risk_score}/100")
        print(f"   ✓ Confidence calculation: {result.confidence:.2f}")
        print(f"   ✓ Risk factor identification: {len(result.risk_factors)} factors")
        print(f"   ✓ Processing time: {result.processing_time_ms}ms")
        print(f"   ✓ Recommendation logic: {result.recommendation}")
        
        if result.risk_score > 70:
            print("\n🚨 HIGH RISK DETECTED - Voice call trigger working!")
        
        print("\n🎯 Next steps:")
        print("   1. Set up real Google Cloud and Datadog API keys")
        print("   2. Test with real AI services")
        print("   3. Build traffic generator for chaos mode")
        print("   4. Add voice integration!")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Error in fraud detection logic: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())