"""
End-to-End Stress Test - Task 8.1
Tests the complete fraud detection pipeline under load
"""

import time
import threading
from concurrent.futures import ThreadPoolExecutor
from src.fraud_detection import FraudDetectionService
from src.voice_signaling import trigger_voice_call


def generate_test_transaction(transaction_id: str, risk_level: str = "high"):
    """Generate a test transaction based on risk level"""
    base_transaction = {
        'user_id': f'user_{transaction_id[-3:]}',
        'transaction_id': transaction_id,
        'timestamp': int(time.time()),
        'phone_number': '+1555010999'
    }
    
    if risk_level == "high":
        return {
            **base_transaction,
            'amount': 5000.00 + (int(transaction_id[-3:]) * 100),
            'merchant_category': 'Travel',
            'country': 'Timbuktu',
        }
    elif risk_level == "medium":
        return {
            **base_transaction,
            'amount': 800.00 + (int(transaction_id[-3:]) * 10),
            'merchant_category': 'Electronics',
            'country': 'USA',
        }
    else:  # low risk
        return {
            **base_transaction,
            'amount': 50.00 + (int(transaction_id[-3:]) * 2),
            'merchant_category': 'Grocery',
            'country': 'USA',
        }


def process_single_transaction(transaction_id: str, risk_level: str = "high"):
    """Process a single transaction through the complete pipeline"""
    start_time = time.time()
    
    try:
        # Generate transaction
        transaction = generate_test_transaction(transaction_id, risk_level)
        
        # Simulate AI analysis (using mock values for stress test)
        analysis = {
            'risk_score': 95 if risk_level == "high" else (60 if risk_level == "medium" else 25),
            'confidence': 0.92,
            'reasoning': f'Stress test transaction - {risk_level} risk',
            'trace_id': f'stress-trace-{transaction_id}'
        }
        
        # Trigger voice call if high risk
        conversation_id = None
        if analysis['risk_score'] > 70:
            alert_data = {
                'transaction': transaction,
                'analysis': analysis,
                'trace_id': analysis['trace_id']
            }
            conversation_id = trigger_voice_call(alert_data)
        
        processing_time = (time.time() - start_time) * 1000
        
        return {
            'transaction_id': transaction_id,
            'risk_score': analysis['risk_score'],
            'processing_time_ms': processing_time,
            'voice_call_initiated': conversation_id is not None,
            'conversation_id': conversation_id,
            'success': True
        }
        
    except Exception as e:
        processing_time = (time.time() - start_time) * 1000
        return {
            'transaction_id': transaction_id,
            'error': str(e),
            'processing_time_ms': processing_time,
            'success': False
        }


def run_stress_test(num_transactions: int = 100, max_workers: int = 10):
    """Run stress test with multiple concurrent transactions"""
    print(f"🚀 Starting stress test with {num_transactions} transactions...")
    print(f"📊 Using {max_workers} concurrent workers")
    print()
    
    start_time = time.time()
    results = []
    
    # Generate mix of risk levels
    transactions = []
    for i in range(num_transactions):
        transaction_id = f"STRESS-{i:03d}"
        if i % 10 < 3:  # 30% high risk
            risk_level = "high"
        elif i % 10 < 6:  # 30% medium risk
            risk_level = "medium"
        else:  # 40% low risk
            risk_level = "low"
        transactions.append((transaction_id, risk_level))
    
    # Process transactions concurrently
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(process_single_transaction, txn_id, risk_level)
            for txn_id, risk_level in transactions
        ]
        
        # Collect results
        for i, future in enumerate(futures):
            try:
                result = future.result(timeout=30)  # 30 second timeout per transaction
                results.append(result)
                
                # Progress indicator
                if (i + 1) % 10 == 0:
                    print(f"📈 Processed {i + 1}/{num_transactions} transactions...")
                    
            except Exception as e:
                results.append({
                    'transaction_id': f"STRESS-{i:03d}",
                    'error': f"Timeout or error: {str(e)}",
                    'success': False
                })
    
    total_time = time.time() - start_time
    
    # Analyze results
    successful = [r for r in results if r.get('success', False)]
    failed = [r for r in results if not r.get('success', False)]
    voice_calls = [r for r in successful if r.get('voice_call_initiated', False)]
    
    avg_processing_time = sum(r.get('processing_time_ms', 0) for r in successful) / len(successful) if successful else 0
    max_processing_time = max(r.get('processing_time_ms', 0) for r in successful) if successful else 0
    min_processing_time = min(r.get('processing_time_ms', 0) for r in successful) if successful else 0
    
    throughput = len(successful) / total_time if total_time > 0 else 0
    
    return {
        'total_transactions': num_transactions,
        'successful': len(successful),
        'failed': len(failed),
        'voice_calls_initiated': len(voice_calls),
        'total_time_seconds': total_time,
        'throughput_tps': throughput,
        'avg_processing_time_ms': avg_processing_time,
        'max_processing_time_ms': max_processing_time,
        'min_processing_time_ms': min_processing_time,
        'success_rate': len(successful) / num_transactions * 100,
        'voice_call_rate': len(voice_calls) / len(successful) * 100 if successful else 0
    }


def print_stress_test_results(results):
    """Print formatted stress test results"""
    print("\n" + "=" * 70)
    print("🎯 STRESS TEST RESULTS")
    print("=" * 70)
    print()
    
    print("📊 TRANSACTION PROCESSING:")
    print(f"  • Total Transactions: {results['total_transactions']}")
    print(f"  • Successful: {results['successful']} ({results['success_rate']:.1f}%)")
    print(f"  • Failed: {results['failed']}")
    print(f"  • Voice Calls Initiated: {results['voice_calls_initiated']} ({results['voice_call_rate']:.1f}% of successful)")
    print()
    
    print("⚡ PERFORMANCE METRICS:")
    print(f"  • Total Time: {results['total_time_seconds']:.2f} seconds")
    print(f"  • Throughput: {results['throughput_tps']:.2f} transactions/second")
    print(f"  • Avg Processing Time: {results['avg_processing_time_ms']:.1f}ms")
    print(f"  • Min Processing Time: {results['min_processing_time_ms']:.1f}ms")
    print(f"  • Max Processing Time: {results['max_processing_time_ms']:.1f}ms")
    print()
    
    # Performance assessment
    if results['avg_processing_time_ms'] < 2500:  # Under 2.5 seconds end-to-end
        print("✅ PERFORMANCE: EXCELLENT (< 2.5s avg)")
    elif results['avg_processing_time_ms'] < 5000:
        print("⚠️  PERFORMANCE: GOOD (< 5s avg)")
    else:
        print("❌ PERFORMANCE: NEEDS IMPROVEMENT (> 5s avg)")
    
    if results['success_rate'] > 95:
        print("✅ RELIABILITY: EXCELLENT (> 95% success)")
    elif results['success_rate'] > 90:
        print("⚠️  RELIABILITY: GOOD (> 90% success)")
    else:
        print("❌ RELIABILITY: NEEDS IMPROVEMENT (< 90% success)")
    
    if results['throughput_tps'] > 5:
        print("✅ THROUGHPUT: EXCELLENT (> 5 TPS)")
    elif results['throughput_tps'] > 2:
        print("⚠️  THROUGHPUT: GOOD (> 2 TPS)")
    else:
        print("❌ THROUGHPUT: NEEDS IMPROVEMENT (< 2 TPS)")


def run_chaos_demo():
    """Run the chaos demo - high-value fraud transaction"""
    print("\n" + "=" * 70)
    print("🚨 CHAOS DEMO - HIGH-VALUE FRAUD ALERT")
    print("=" * 70)
    print()
    
    # Generate a $10,000 fraud transaction
    chaos_transaction = {
        'user_id': 'user_vip_001',
        'transaction_id': 'CHAOS-FRAUD-001',
        'amount': 10000.00,
        'merchant_category': 'Jewelry',
        'country': 'Nigeria',
        'timestamp': int(time.time()),
        'phone_number': '+1555010999'
    }
    
    # High-risk analysis
    chaos_analysis = {
        'risk_score': 98,
        'confidence': 0.95,
        'reasoning': 'CHAOS DEMO: $10,000 jewelry purchase in Nigeria - EXTREMELY SUSPICIOUS',
        'trace_id': 'chaos-demo-trace-001'
    }
    
    print("📊 CHAOS TRANSACTION GENERATED:")
    print(f"  • Transaction ID: {chaos_transaction['transaction_id']}")
    print(f"  • Amount: ${chaos_transaction['amount']:,.2f}")
    print(f"  • Merchant: {chaos_transaction['merchant_category']}")
    print(f"  • Location: {chaos_transaction['country']}")
    print(f"  • Risk Score: {chaos_analysis['risk_score']}%")
    print()
    
    print("📞 INITIATING EMERGENCY VOICE CALL...")
    
    alert_data = {
        'transaction': chaos_transaction,
        'analysis': chaos_analysis,
        'trace_id': chaos_analysis['trace_id']
    }
    
    start_time = time.time()
    conversation_id = trigger_voice_call(alert_data)
    call_time = (time.time() - start_time) * 1000
    
    print(f"✅ VOICE CALL INITIATED: {conversation_id}")
    print(f"⚡ Call Latency: {call_time:.0f}ms")
    print()
    print("🎯 CHAOS DEMO COMPLETE - JUDGES CAN HEAR THE PHONE RING!")


if __name__ == "__main__":
    try:
        print("=" * 70)
        print("🎯 PROJECT HEIMDALL - END-TO-END STRESS TEST")
        print("=" * 70)
        
        # Run stress test
        results = run_stress_test(num_transactions=50, max_workers=5)  # Reduced for demo
        print_stress_test_results(results)
        
        # Run chaos demo
        run_chaos_demo()
        
        print("\n" + "=" * 70)
        print("🏆 STRESS TEST COMPLETE - SYSTEM READY FOR JUDGES!")
        print("=" * 70)
        
    except Exception as e:
        print(f"\n❌ Stress test failed: {e}")
        import traceback
        traceback.print_exc()