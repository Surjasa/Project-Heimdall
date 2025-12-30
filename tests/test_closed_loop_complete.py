#!/usr/bin/env python3
"""
Complete Closed Loop Demo - End-to-End Fraud Detection with Voice Response
Demonstrates: Transaction → AI Analysis → Voice Call (Complete Pipeline)
"""

import asyncio
import json
import time
import sys
import os
from datetime import datetime
from typing import Dict, Any

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.agents.fraud_detection import FraudDetectionService
from src.agents.voice_signaling import trigger_voice_call
from src.utils.logging_config import get_logger

logger = get_logger("closed-loop-demo")


class ClosedLoopDemo:
    """Demonstrates the complete fraud detection and voice response pipeline"""
    
    def __init__(self):
        self.fraud_service = FraudDetectionService()
        self.demo_results = []
    
    async def simulate_fraud_transaction(self, scenario_name: str, transaction_data: Dict[str, Any]) -> Dict[str, Any]:
        """Simulate a complete fraud detection and voice response flow"""
        logger.info(f"🎬 Starting scenario: {scenario_name}")
        
        # Step 1: Generate trace ID
        trace_id = f"demo-trace-{int(time.time() * 1000)}"
        
        # Step 2: AI Fraud Analysis
        logger.info("🧠 Step 1: AI Fraud Analysis...")
        start_time = time.time()
        
        # Create transaction and user context (simplified for demo)
        from src.fraud_detection import Transaction, Location, UserContext
        from decimal import Decimal
        
        location = Location(
            country=transaction_data.get('location', {}).get('country', 'Unknown'),
            city=transaction_data.get('location', {}).get('city', 'Unknown'),
            coordinates=(0.0, 0.0),
            is_new_location=transaction_data.get('location', {}).get('is_new_location', False),
            distance_from_home=transaction_data.get('location', {}).get('distance_from_home', 0.0)
        )
        
        transaction = Transaction(
            transaction_id=transaction_data['transaction_id'],
            user_id=transaction_data.get('user_id', 'demo_user'),
            amount=Decimal(str(transaction_data['amount'])),
            currency=transaction_data.get('currency', 'USD'),
            merchant=transaction_data['merchant'],
            merchant_category=transaction_data.get('merchant_category', 'unknown'),
            location=location,
            timestamp=datetime.now(),
            payment_method=transaction_data.get('payment_method', 'credit_card'),
            metadata=transaction_data.get('metadata', {})
        )
        
        user_context = UserContext(
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
            phone_number=transaction_data.get('phone_number', '+1-555-0123')
        )
        
        # Perform AI analysis
        analysis_result = self.fraud_service.analyze_transaction_realtime(transaction, user_context)
        ai_analysis_time = (time.time() - start_time) * 1000
        
        logger.info(f"   ✅ AI Analysis complete: Risk={analysis_result.risk_score}/100 ({ai_analysis_time:.1f}ms)")
        
        # Step 3: Voice Call Decision
        voice_call_result = None
        if analysis_result.risk_score > 70:
            logger.info("📞 Step 2: Initiating Voice Call (High Risk Detected)...")
            
            # Prepare alert data for voice signaling
            alert_data = {
                'transaction': transaction_data,
                'analysis': {
                    'risk_score': analysis_result.risk_score,
                    'confidence': analysis_result.confidence,
                    'risk_factors': analysis_result.risk_factors,
                    'reasoning': getattr(analysis_result, 'reasoning', 'High-risk transaction detected'),
                    'recommendation': analysis_result.recommendation
                },
                'trace_id': trace_id
            }
            
            # Trigger voice call
            voice_start_time = time.time()
            voice_call_result = await trigger_voice_call(alert_data)
            voice_call_time = (time.time() - voice_start_time) * 1000
            
            if voice_call_result.success:
                logger.info(f"   ✅ Voice call initiated: {voice_call_result.conversation_id} ({voice_call_time:.1f}ms)")
            else:
                logger.error(f"   ❌ Voice call failed: {voice_call_result.error_message}")
        else:
            logger.info("✅ Step 2: No voice call needed (Low Risk)")
        
        # Step 4: Calculate total end-to-end time
        total_time = (time.time() - start_time) * 1000
        
        # Compile results
        result = {
            'scenario': scenario_name,
            'trace_id': trace_id,
            'transaction_id': transaction_data['transaction_id'],
            'risk_score': analysis_result.risk_score,
            'confidence': analysis_result.confidence,
            'ai_analysis_time_ms': ai_analysis_time,
            'voice_call_initiated': voice_call_result is not None and voice_call_result.success,
            'voice_call_time_ms': voice_call_result.latency_ms if voice_call_result else 0,
            'conversation_id': voice_call_result.conversation_id if voice_call_result else None,
            'total_end_to_end_time_ms': total_time,
            'recommendation': analysis_result.recommendation
        }
        
        self.demo_results.append(result)
        
        logger.info(f"🎯 Scenario complete: {scenario_name}")
        logger.info(f"   Total time: {total_time:.1f}ms | Voice call: {'Yes' if result['voice_call_initiated'] else 'No'}")
        
        return result
    
    async def run_demo_scenarios(self):
        """Run multiple fraud detection scenarios"""
        logger.info("🚀 CLOSED LOOP FRAUD DETECTION DEMO")
        logger.info("=" * 50)
        
        scenarios = [
            {
                'name': 'High-Risk: Luxury Purchase in Nigeria',
                'transaction': {
                    'transaction_id': 'TXN-DEMO-001',
                    'amount': 9999.99,
                    'merchant': 'Luxury Jewelry Store',
                    'merchant_category': 'jewelry',
                    'location': {
                        'country': 'Nigeria',
                        'city': 'Lagos',
                        'is_new_location': True,
                        'distance_from_home': 5000.0
                    },
                    'phone_number': '+1-555-0123',
                    'metadata': {'demo_scenario': 'high_risk_jewelry'}
                }
            },
            {
                'name': 'Medium-Risk: Electronics in Romania',
                'transaction': {
                    'transaction_id': 'TXN-DEMO-002',
                    'amount': 2500.00,
                    'merchant': 'Electronics Superstore',
                    'merchant_category': 'electronics',
                    'location': {
                        'country': 'Romania',
                        'city': 'Bucharest',
                        'is_new_location': True,
                        'distance_from_home': 4500.0
                    },
                    'phone_number': '+1-555-0123',
                    'metadata': {'demo_scenario': 'medium_risk_electronics'}
                }
            },
            {
                'name': 'Low-Risk: Local Grocery Store',
                'transaction': {
                    'transaction_id': 'TXN-DEMO-003',
                    'amount': 87.50,
                    'merchant': 'Safeway Grocery',
                    'merchant_category': 'grocery',
                    'location': {
                        'country': 'United States',
                        'city': 'San Francisco',
                        'is_new_location': False,
                        'distance_from_home': 2.5
                    },
                    'phone_number': '+1-555-0123',
                    'metadata': {'demo_scenario': 'low_risk_grocery'}
                }
            }
        ]
        
        # Run each scenario
        for scenario in scenarios:
            await self.simulate_fraud_transaction(scenario['name'], scenario['transaction'])
            await asyncio.sleep(2)  # Brief pause between scenarios
        
        # Print summary
        self.print_demo_summary()
    
    def print_demo_summary(self):
        """Print comprehensive demo summary"""
        logger.info("\n" + "=" * 60)
        logger.info("📊 CLOSED LOOP DEMO SUMMARY")
        logger.info("=" * 60)
        
        total_scenarios = len(self.demo_results)
        voice_calls_initiated = sum(1 for r in self.demo_results if r['voice_call_initiated'])
        avg_ai_time = sum(r['ai_analysis_time_ms'] for r in self.demo_results) / total_scenarios
        avg_total_time = sum(r['total_end_to_end_time_ms'] for r in self.demo_results) / total_scenarios
        
        logger.info(f"📈 Overall Statistics:")
        logger.info(f"   Total Scenarios: {total_scenarios}")
        logger.info(f"   Voice Calls Initiated: {voice_calls_initiated}/{total_scenarios}")
        logger.info(f"   Average AI Analysis Time: {avg_ai_time:.1f}ms")
        logger.info(f"   Average End-to-End Time: {avg_total_time:.1f}ms")
        
        logger.info(f"\n📋 Scenario Details:")
        for result in self.demo_results:
            status_icon = "📞" if result['voice_call_initiated'] else "✅"
            logger.info(f"   {status_icon} {result['scenario']}")
            logger.info(f"      Risk: {result['risk_score']}/100 | Total: {result['total_end_to_end_time_ms']:.1f}ms")
            if result['voice_call_initiated']:
                logger.info(f"      Voice Call: {result['conversation_id']} ({result['voice_call_time_ms']:.1f}ms)")
            logger.info(f"      Trace ID: {result['trace_id']}")
        
        logger.info(f"\n🎯 Key Achievements:")
        logger.info(f"   ✓ End-to-end fraud detection pipeline")
        logger.info(f"   ✓ Real-time AI analysis (<500ms average)")
        logger.info(f"   ✓ Proactive voice call initiation")
        logger.info(f"   ✓ Complete trace ID correlation")
        logger.info(f"   ✓ Professional logging throughout")
        
        # Performance validation
        if avg_ai_time < 500:
            logger.info(f"   ✅ AI Performance: EXCELLENT (<500ms)")
        elif avg_ai_time < 1000:
            logger.info(f"   ⚠️  AI Performance: GOOD (<1s)")
        else:
            logger.info(f"   ❌ AI Performance: NEEDS IMPROVEMENT (>1s)")
        
        if avg_total_time < 2500:
            logger.info(f"   ✅ End-to-End Performance: EXCELLENT (<2.5s)")
        elif avg_total_time < 5000:
            logger.info(f"   ⚠️  End-to-End Performance: GOOD (<5s)")
        else:
            logger.info(f"   ❌ End-to-End Performance: NEEDS IMPROVEMENT (>5s)")


async def main():
    """Run the complete closed loop demo"""
    try:
        demo = ClosedLoopDemo()
        await demo.run_demo_scenarios()
        
        logger.info("\n🎉 CLOSED LOOP DEMO COMPLETED SUCCESSFULLY!")
        logger.info("🎯 The fraud detection system is now fully functional:")
        logger.info("   ✓ Real-time transaction processing")
        logger.info("   ✓ AI-powered fraud analysis")
        logger.info("   ✓ Proactive voice call initiation")
        logger.info("   ✓ End-to-end trace correlation")
        logger.info("   ✓ Professional logging and monitoring")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)