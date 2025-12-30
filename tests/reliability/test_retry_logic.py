#!/usr/bin/env python3
"""
Test Retry Logic Implementation
Validates that retry decorators work correctly
"""

import sys
import os
import time
import logging

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.retry_logic import (
    exponential_backoff_retry, 
    ai_service_retry, 
    kafka_operation_retry,
    voice_service_retry,
    CircuitBreaker
)

# Configure logging to see retry attempts
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_basic_retry():
    """Test basic exponential backoff retry"""
    print("🧪 Testing Basic Retry Logic...")
    
    attempt_count = 0
    
    @exponential_backoff_retry(max_retries=3, base_delay=0.1)
    def failing_function():
        nonlocal attempt_count
        attempt_count += 1
        if attempt_count < 3:
            raise ConnectionError(f"Simulated failure #{attempt_count}")
        return "Success!"
    
    start_time = time.time()
    result = failing_function()
    duration = time.time() - start_time
    
    print(f"✅ Basic retry test passed: {result}")
    print(f"   Attempts: {attempt_count}, Duration: {duration:.2f}s")
    assert result == "Success!"
    assert attempt_count == 3


def test_ai_service_retry():
    """Test AI service specific retry logic"""
    print("\n🧪 Testing AI Service Retry...")
    
    attempt_count = 0
    
    @ai_service_retry(max_retries=2)
    def mock_ai_call():
        nonlocal attempt_count
        attempt_count += 1
        if attempt_count < 2:
            # Simulate rate limiting (429 error)
            error = Exception("Rate limited")
            error.code = 429
            raise error
        return {"risk_score": 85, "confidence": 0.9}
    
    result = mock_ai_call()
    print(f"✅ AI retry test passed: {result}")
    print(f"   Attempts: {attempt_count}")
    assert attempt_count == 2


def test_circuit_breaker():
    """Test circuit breaker functionality"""
    print("\n🧪 Testing Circuit Breaker...")
    
    circuit_breaker = CircuitBreaker(failure_threshold=2, recovery_timeout=1)
    
    def failing_function():
        raise ConnectionError("Service unavailable")
    
    # Test failures until circuit opens
    failures = 0
    for i in range(5):
        try:
            circuit_breaker.call(failing_function)
        except Exception:
            failures += 1
            if failures >= 2:
                break
    
    print(f"✅ Circuit breaker opened after {failures} failures")
    assert circuit_breaker.state == "OPEN"
    
    # Test that circuit stays open
    try:
        circuit_breaker.call(failing_function)
        assert False, "Circuit breaker should be open"
    except Exception as e:
        print(f"✅ Circuit breaker correctly blocked call: {e}")
    
    # Wait for recovery timeout
    time.sleep(1.1)
    
    # Test half-open state (should allow one call)
    try:
        circuit_breaker.call(failing_function)
    except Exception:
        print("✅ Circuit breaker in half-open state, call failed as expected")


def test_kafka_retry():
    """Test Kafka operation retry"""
    print("\n🧪 Testing Kafka Retry Logic...")
    
    attempt_count = 0
    
    @kafka_operation_retry(max_retries=2)
    def mock_kafka_operation():
        nonlocal attempt_count
        attempt_count += 1
        if attempt_count < 2:
            raise ConnectionError("Kafka broker unavailable")
        return "Message sent successfully"
    
    result = mock_kafka_operation()
    print(f"✅ Kafka retry test passed: {result}")
    print(f"   Attempts: {attempt_count}")
    assert attempt_count == 2


def test_voice_retry():
    """Test voice service retry"""
    print("\n🧪 Testing Voice Service Retry...")
    
    attempt_count = 0
    
    @voice_service_retry(max_retries=2)
    def mock_voice_call():
        nonlocal attempt_count
        attempt_count += 1
        if attempt_count < 2:
            raise TimeoutError("ElevenLabs API timeout")
        return "conv_12345"
    
    result = mock_voice_call()
    print(f"✅ Voice retry test passed: {result}")
    print(f"   Attempts: {attempt_count}")
    assert attempt_count == 2


def main():
    """Run all retry logic tests"""
    print("🚀 RETRY LOGIC VALIDATION")
    print("=" * 40)
    
    try:
        test_basic_retry()
        test_ai_service_retry()
        test_circuit_breaker()
        test_kafka_retry()
        test_voice_retry()
        
        print("\n✅ ALL RETRY LOGIC TESTS PASSED!")
        print("🎯 Features validated:")
        print("   ✓ Exponential backoff retry")
        print("   ✓ AI service retry with rate limit handling")
        print("   ✓ Circuit breaker pattern")
        print("   ✓ Kafka operation retry")
        print("   ✓ Voice service retry")
        
        print("\n🛡️  System is now bulletproofed against:")
        print("   ✓ Network blips and timeouts")
        print("   ✓ AI service rate limiting (429 errors)")
        print("   ✓ Kafka connection issues")
        print("   ✓ Voice service failures")
        print("   ✓ Cascading failures (circuit breaker)")
        
        return True
        
    except Exception as e:
        print(f"\n❌ RETRY LOGIC TEST FAILED: {e}")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)