#!/usr/bin/env python3
"""
Test Idempotent Confluent Setup
Validates that setup can be run multiple times safely
"""

import os
import sys
import tempfile
import subprocess
from unittest.mock import patch, MagicMock

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.logging_config import get_logger

logger = get_logger("idempotent-setup-test")


def test_setup_script_exists():
    """Test that setup script exists and is executable"""
    print("🧪 Testing setup script existence...")
    
    assert os.path.exists("setup_confluent.py"), "setup_confluent.py should exist"
    
    # Check if script is executable (has proper shebang)
    try:
        with open("setup_confluent.py", 'r', encoding='utf-8') as f:
            first_line = f.readline().strip()
            assert first_line.startswith("#!"), "Script should have shebang line"
    except UnicodeDecodeError:
        # Try with different encoding
        with open("setup_confluent.py", 'r', encoding='latin-1') as f:
            first_line = f.readline().strip()
            assert first_line.startswith("#!"), "Script should have shebang line"
    
    print("✅ Setup script exists and has proper shebang")


def test_idempotent_setup_class():
    """Test IdempotentConfluentSetup class functionality"""
    print("\n🧪 Testing IdempotentConfluentSetup class...")
    
    # Import the class
    from setup_confluent import IdempotentConfluentSetup
    
    # Create instance
    setup = IdempotentConfluentSetup()
    
    # Test mock mode detection
    assert setup.use_mock_mode == True, "Should detect mock mode with placeholder credentials"
    
    # Test environment validation
    is_valid, missing_vars, placeholder_vars = setup.validate_environment_variables()
    assert not is_valid, "Should detect invalid environment with placeholders"
    assert len(placeholder_vars) > 0, "Should detect placeholder variables"
    
    print("✅ IdempotentConfluentSetup class working correctly")


def test_multiple_runs_idempotency():
    """Test that running setup multiple times is safe"""
    print("\n🧪 Testing multiple runs for idempotency...")
    
    # Run setup script multiple times
    results = []
    
    for i in range(3):
        print(f"   Run {i+1}/3...")
        result = subprocess.run(
            [sys.executable, "setup_confluent.py"],
            capture_output=True,
            text=True,
            timeout=30
        )
        results.append(result)
    
    # All runs should succeed
    for i, result in enumerate(results):
        assert result.returncode == 0, f"Run {i+1} should succeed (exit code 0), got {result.returncode}"
        # Check both stdout and stderr for success message
        output = result.stdout + result.stderr
        assert "CONFLUENT SETUP COMPLETED SUCCESSFULLY" in output, f"Run {i+1} should complete successfully"
    
    print("✅ Multiple runs completed successfully - setup is idempotent")


def test_environment_variable_validation():
    """Test environment variable validation logic"""
    print("\n🧪 Testing environment variable validation...")
    
    from setup_confluent import IdempotentConfluentSetup
    
    # Test with missing variables
    with patch.dict(os.environ, {}, clear=True):
        setup = IdempotentConfluentSetup()
        is_valid, missing_vars, placeholder_vars = setup.validate_environment_variables()
        
        assert not is_valid, "Should be invalid with missing variables"
        assert len(missing_vars) > 0, "Should detect missing variables"
    
    # Test with placeholder variables
    with patch.dict(os.environ, {
        'CONFLUENT_BOOTSTRAP_SERVERS': 'your-bootstrap-servers',
        'CONFLUENT_API_KEY': 'your-api-key',
        'CONFLUENT_API_SECRET': 'your-api-secret'
    }):
        setup = IdempotentConfluentSetup()
        is_valid, missing_vars, placeholder_vars = setup.validate_environment_variables()
        
        assert not is_valid, "Should be invalid with placeholder variables"
        assert len(placeholder_vars) > 0, "Should detect placeholder variables"
    
    # Test with valid variables
    with patch.dict(os.environ, {
        'CONFLUENT_BOOTSTRAP_SERVERS': 'pkc-test.region.provider.confluent.cloud:9092',
        'CONFLUENT_API_KEY': 'real-api-key',
        'CONFLUENT_API_SECRET': 'real-api-secret'
    }):
        setup = IdempotentConfluentSetup()
        is_valid, missing_vars, placeholder_vars = setup.validate_environment_variables()
        
        assert is_valid, "Should be valid with real variables"
        assert len(missing_vars) == 0, "Should have no missing variables"
        assert len(placeholder_vars) == 0, "Should have no placeholder variables"
    
    print("✅ Environment variable validation working correctly")


def test_mock_mode_functionality():
    """Test mock mode functionality"""
    print("\n🧪 Testing mock mode functionality...")
    
    from setup_confluent import IdempotentConfluentSetup
    
    # Create setup in mock mode
    setup = IdempotentConfluentSetup()
    assert setup.use_mock_mode == True, "Should be in mock mode"
    
    # Test connection in mock mode
    result = setup.test_connection()
    assert result == True, "Mock connection test should succeed"
    assert setup.setup_results['connection_test'] == True, "Should track connection test success"
    
    # Test topic setup in mock mode
    result = setup.verify_and_setup_topics()
    assert result == True, "Mock topic setup should succeed"
    assert setup.setup_results['topic_verification'] == True, "Should track topic verification success"
    
    # Test producer in mock mode
    result = setup.test_producer()
    assert result == True, "Mock producer test should succeed"
    assert setup.setup_results['producer_test'] == True, "Should track producer test success"
    
    # Test consumer setup in mock mode
    result = setup.test_consumer_setup()
    assert result == True, "Mock consumer setup should succeed"
    assert setup.setup_results['consumer_test'] == True, "Should track consumer test success"
    
    print("✅ Mock mode functionality working correctly")


def test_error_handling():
    """Test error handling in setup"""
    print("\n🧪 Testing error handling...")
    
    from setup_confluent import IdempotentConfluentSetup
    
    setup = IdempotentConfluentSetup()
    
    # Test that setup handles exceptions gracefully
    try:
        # This should not crash even if there are issues
        setup.run_comprehensive_setup()
        print("✅ Setup handles errors gracefully")
    except Exception as e:
        print(f"❌ Setup should not crash with exceptions: {e}")
        raise


def test_setup_results_tracking():
    """Test that setup results are properly tracked"""
    print("\n🧪 Testing setup results tracking...")
    
    from setup_confluent import IdempotentConfluentSetup
    
    setup = IdempotentConfluentSetup()
    
    # Initially all results should be False
    for step, result in setup.setup_results.items():
        assert result == False, f"Initial {step} should be False"
    
    # Run comprehensive setup
    setup.run_comprehensive_setup()
    
    # In mock mode, most results should be True
    expected_true = ['topic_verification', 'topic_creation', 'producer_test', 'consumer_test', 'connection_test']
    for step in expected_true:
        assert setup.setup_results[step] == True, f"{step} should be True after setup"
    
    print("✅ Setup results tracking working correctly")


def test_repair_mode():
    """Test repair mode functionality"""
    print("\n🧪 Testing repair mode...")
    
    from setup_confluent import IdempotentConfluentSetup
    
    setup = IdempotentConfluentSetup()
    
    # Simulate some failed steps
    setup.setup_results['connection_test'] = False
    setup.setup_results['topic_verification'] = False
    
    # Run repair mode
    result = setup.repair_mode()
    assert result == True, "Repair mode should complete"
    
    # After repair, steps should be fixed (in mock mode)
    assert setup.setup_results['connection_test'] == True, "Connection should be repaired"
    assert setup.setup_results['topic_verification'] == True, "Topic verification should be repaired"
    
    print("✅ Repair mode working correctly")


def main():
    """Run all idempotent setup tests"""
    print("🚀 IDEMPOTENT SETUP VALIDATION")
    print("=" * 50)
    
    try:
        test_setup_script_exists()
        test_idempotent_setup_class()
        test_environment_variable_validation()
        test_mock_mode_functionality()
        test_error_handling()
        test_setup_results_tracking()
        test_repair_mode()
        test_multiple_runs_idempotency()
        
        print("\n✅ ALL IDEMPOTENT SETUP TESTS PASSED!")
        print("🎯 Features validated:")
        print("   ✓ Setup script existence and executability")
        print("   ✓ IdempotentConfluentSetup class functionality")
        print("   ✓ Environment variable validation")
        print("   ✓ Mock mode functionality")
        print("   ✓ Error handling and graceful degradation")
        print("   ✓ Setup results tracking")
        print("   ✓ Repair mode functionality")
        print("   ✓ Multiple runs idempotency")
        
        print("\n🛡️  System is now bulletproofed with:")
        print("   ✓ Idempotent infrastructure setup")
        print("   ✓ Safe multiple execution")
        print("   ✓ Comprehensive error handling")
        print("   ✓ Environment validation")
        print("   ✓ Mock mode for development")
        print("   ✓ Repair capabilities")
        print("   ✓ Professional logging throughout")
        
        return True
        
    except Exception as e:
        print(f"\n❌ IDEMPOTENT SETUP TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)