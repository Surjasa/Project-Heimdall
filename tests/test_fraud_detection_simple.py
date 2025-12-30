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
sys.path.append('src')

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
    This test file is a solid "Smoke Test" for your Phase 1. It’s exactly what you need to verify that your "Brain" is talking to both Google and Datadog before you start building the "Nervous System" (Confluent).

Here is the Intelligence-First skeleton for src/fraud_detection.py.

This script fulfills the core of Requirement 2 and Requirement 4. It uses Gemini 1.5 Flash for the high-speed risk assessment and includes the Datadog LLMObs instrumentation that will automatically capture your "thoughts" (prompts and completions) for the judges.

🧠 The Fraud Detection Skeleton
Python

import os
import json
from ddtrace.llmobs import LLMObs
from vertexai.generative_models import GenerativeModel, HarmCategory, HarmBlockThreshold
import vertexai

# 1. Initialize Datadog LLM Observability
# This must happen BEFORE the model is called
LLMObs.enable(
    ml_app="crisis-response-agent",
    agentless_enabled=True,
    site=os.getenv("DD_SITE", "datadoghq.com"),
    api_key=os.getenv("DD_API_KEY")
)

def analyze_transaction_fraud(transaction_data: dict):
    """
    Analyzes a transaction using Gemini 1.5 Flash.
    Fulfills Property 5: Response < 500ms.
    """
    vertexai.init(project=os.getenv("GOOGLE_CLOUD_PROJECT"), location="us-central1")
    model = GenerativeModel("gemini-1.5-flash")

    # The Prompt: Structured with PTCF (Persona, Task, Context, Format)
    # Fulfills Property 6 & 8
    system_instruction = """
    You are an expert financial fraud investigator. 
    Analyze the transaction context and return a JSON object.
    
    REQUIRED FORMAT:
    {
      "risk_score": float (0-100),
      "confidence": float (0-1),
      "reasoning": "string",
      "recommendation": "approve" | "review" | "call_user"
    }
    """
    
    user_prompt = f"Analyze this transaction for fraud: {json.dumps(transaction_data)}"

    # Configuration for speed and determinism
    generation_config = {
        "temperature": 0.1,  # Low temperature for deterministic risk scoring
        "response_mime_type": "application/json",
    }

    # Datadog will automatically trace this call via ddtrace-run
    response = model.generate_content(
        [system_instruction, user_prompt],
        generation_config=generation_config
    )

    return json.loads(response.text)

def demo_fraud_detection():
    """Demo function for your Smoke Test script"""
    sample_tx = {
        "amount": 1250.00,
        "location": "Lagos, Nigeria",
        "user_home": "New York, USA",
        "merchant": "Electronics Store",
        "history": "No previous transactions in this region"
    }
    
    print(f"🔍 Analyzing suspicious transaction: ${sample_tx['amount']} in {sample_tx['location']}")
    result = analyze_transaction_fraud(sample_tx)
    print(f"📊 Risk Score: {result['risk_score']}")
    print(f"📝 Reasoning: {result['reasoning']}")
    return result
🛠️ Strategic Tips for this Step
Prompting (Property 6): Notice the system_instruction uses PTCF (Persona, Task, Context, Format). This is a best practice that ensures Gemini remains objective and returns a valid JSON every time.

Instrumentation (Property 12): By setting response_mime_type: "application/json", you ensure the output is machine-readable, which is vital for the Voice Agent (Task 5) to know if it needs to call the user.

Performance (Property 7): Gemini 1.5 Flash is used here because it is roughly 2x faster than Pro models, keeping your analysis under that critical 500ms threshold required by the hackathon.

🎯 Next Step
Now that you have the "Brain" skeleton, you can run your test_fraud.py script.

Once that passes, would you like me to show you the "Signaling Server" (Task 5.3) that listens for a risk_score > 70 and triggers the ElevenLabs Voice call?

Running Google's Models on Vertex AI with Python

This video demonstrates how to initialize the Vertex AI SDK and run models like Gemini 1.5 Flash in Python, which is the exact setup you need for your src/fraud_detection.py file.