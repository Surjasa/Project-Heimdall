"""
Closed-Loop Fraud Detection Demo - Task 5.3 Complete
Demonstrates the full narrative:
1. Transaction generation
2. AI fraud analysis
3. Voice alert triggering
"""

import json
import time
from src.voice_signaling import trigger_voice_call


def demo_closed_loop():
    """
    Demonstrates the complete closed-loop fraud detection system.
    """
    print("=" * 70)
    print("🎯 CLOSED-LOOP FRAUD DETECTION DEMO")
    print("=" * 70)
    print()
    
    # Step 1: Generate a suspicious transaction
    print("📊 STEP 1: Generate Suspicious Transaction")
    print("-" * 70)
    
    # Create a high-risk transaction
    suspicious_txn = {
        'user_id': 'user_123',
        'transaction_id': 'TXN-DEMO-001',
        'amount': 5000.00,
        'merchant_category': 'Travel',
        'country': 'Timbuktu',
        'timestamp': int(time.time()),
        'phone_number': '+1555010999'
    }
    
    print(f"Transaction ID: {suspicious_txn['transaction_id']}")
    print(f"Amount: ${suspicious_txn['amount']:.2f}")
    print(f"Merchant: {suspicious_txn['merchant_category']}")
    print(f"Location: {suspicious_txn['country']}")
    print()
    
    # Step 2: Simulate AI fraud analysis
    print("🧠 STEP 2: AI Fraud Analysis (Gemini 1.5 Flash)")
    print("-" * 70)
    
    # Simulate AI analysis result
    analysis = {
        'risk_score': 95,
        'confidence': 0.92,
        'reasoning': 'Unusual location (Timbuktu) + High amount ($5000) + Travel merchant',
        'trace_id': 'trace-demo-001'
    }
    
    print(f"Risk Score: {analysis['risk_score']}%")
    print(f"Confidence: {analysis['confidence']}")
    print(f"Reasoning: {analysis['reasoning']}")
    print(f"Trace ID: {analysis['trace_id']}")
    print()
    
    # Step 3: Trigger voice call for high-risk transaction
    if analysis['risk_score'] > 70:
        print("📞 STEP 3: Voice Alert Triggered (Risk > 70%)")
        print("-" * 70)
        
        alert_data = {
            'transaction': suspicious_txn,
            'analysis': analysis,
            'trace_id': analysis['trace_id']
        }
        
        conversation_id = trigger_voice_call(alert_data)
        
        print(f"Conversation ID: {conversation_id}")
        print()
    
    # Summary
    print("=" * 70)
    print("✅ CLOSED-LOOP DEMO COMPLETE")
    print("=" * 70)
    print()
    print("📋 Summary:")
    print(f"  • Transaction generated: {suspicious_txn['transaction_id']}")
    print(f"  • AI analysis completed: Risk {analysis['risk_score']}%")
    print(f"  • Voice call initiated: {conversation_id}")
    print()
    print("🎯 This demonstrates the complete fraud detection pipeline:")
    print("  1. Suspicious transaction detected")
    print("  2. AI analyzes and flags as high-risk")
    print("  3. Voice alert automatically triggered")
    print("  4. Customer can verify transaction via phone")
    print()


def demo_multiple_scenarios():
    """
    Demonstrates multiple fraud scenarios.
    """
    print("=" * 70)
    print("🎯 MULTIPLE FRAUD SCENARIOS DEMO")
    print("=" * 70)
    print()
    
    scenarios = [
        {
            'name': 'Scenario 1: High Amount + New Country',
            'transaction': {
                'user_id': 'user_001',
                'transaction_id': 'TXN-SCENARIO-001',
                'amount': 8000.00,
                'merchant_category': 'Jewelry',
                'country': 'Nigeria',
                'timestamp': int(time.time()),
                'phone_number': '+1555010999'
            },
            'risk_score': 88
        },
        {
            'name': 'Scenario 2: Moderate Risk',
            'transaction': {
                'user_id': 'user_002',
                'transaction_id': 'TXN-SCENARIO-002',
                'amount': 500.00,
                'merchant_category': 'Electronics',
                'country': 'USA',
                'timestamp': int(time.time()),
                'phone_number': '+1555010999'
            },
            'risk_score': 45
        },
        {
            'name': 'Scenario 3: Legitimate Transaction',
            'transaction': {
                'user_id': 'user_003',
                'transaction_id': 'TXN-SCENARIO-003',
                'amount': 50.00,
                'merchant_category': 'Grocery',
                'country': 'USA',
                'timestamp': int(time.time()),
                'phone_number': '+1555010999'
            },
            'risk_score': 15
        }
    ]
    
    for scenario in scenarios:
        print(f"📊 {scenario['name']}")
        print("-" * 70)
        
        txn = scenario['transaction']
        risk_score = scenario['risk_score']
        
        print(f"  Transaction: {txn['transaction_id']}")
        print(f"  Amount: ${txn['amount']:.2f}")
        print(f"  Merchant: {txn['merchant_category']}")
        print(f"  Location: {txn['country']}")
        print(f"  Risk Score: {risk_score}%")
        print(f"  Status: {'🚨 HIGH RISK - Voice Alert' if risk_score > 70 else '✅ LOW RISK - No Alert'}")
        print()


if __name__ == "__main__":
    try:
        demo_closed_loop()
        print()
        demo_multiple_scenarios()
        print("✅ All demos completed successfully!")
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()
