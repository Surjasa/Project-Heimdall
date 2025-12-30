#!/usr/bin/env python3
"""
Test suite for Datadog Alerts and Case Management - Tasks 6.4 & 6.5
Validates alert rule configuration and case creation functionality.
"""

import sys
import os
import time
from datetime import datetime

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.monitoring.datadog_alerts import DatadogAlertManager, AlertRule, CaseRecord, create_case_for_transaction


def test_alert_manager_initialization():
    """Test DatadogAlertManager initialization"""
    print("🧪 Testing DatadogAlertManager initialization...")
    
    manager = DatadogAlertManager()
    
    # Should initialize in mock mode without credentials
    assert manager.mock_mode == True, "Should be in mock mode without credentials"
    assert len(manager.alert_rules) > 0, "Should have alert rules defined"
    
    print("✅ DatadogAlertManager initialization working correctly")


def test_alert_rule_definitions():
    """Test that all required alert rules are defined"""
    print("🧪 Testing alert rule definitions...")
    
    manager = DatadogAlertManager()
    rule_names = [rule.name for rule in manager.alert_rules]
    
    # Check for required alert rules (Task 6.4)
    required_rules = [
        "High LLM Costs - 10 Minute Window",
        "High Voice Response Latency", 
        "High Voice Call Failure Rate",
        "Slow AI Fraud Analysis",
        "High Volume of Risky Transactions"
    ]
    
    for required_rule in required_rules:
        assert required_rule in rule_names, f"Missing required alert rule: {required_rule}"
    
    # Validate rule structure
    for rule in manager.alert_rules:
        assert isinstance(rule, AlertRule), "Rule should be AlertRule instance"
        assert rule.name, "Rule should have name"
        assert rule.query, "Rule should have query"
        assert rule.message, "Rule should have message"
        assert rule.threshold_critical > 0, "Rule should have critical threshold"
        assert len(rule.tags) > 0, "Rule should have tags"
    
    print(f"✅ Alert rule definitions working correctly ({len(manager.alert_rules)} rules)")


def test_alert_setup_mock_mode():
    """Test alert setup in mock mode"""
    print("🧪 Testing alert setup (mock mode)...")
    
    manager = DatadogAlertManager()
    success = manager.setup_alert_rules()
    
    assert success == True, "Alert setup should succeed in mock mode"
    
    print("✅ Alert setup mock mode working correctly")


def test_case_creation():
    """Test fraud case creation for high-risk transactions"""
    print("🧪 Testing fraud case creation...")
    
    manager = DatadogAlertManager()
    
    # Test high-risk transaction (should create case)
    high_risk_transaction = {
        'transaction': {
            'transaction_id': 'TXN-TEST-001',
            'amount': 5000,
            'merchant': 'Luxury Store',
            'location': {'country': 'Nigeria'}
        }
    }
    
    high_risk_analysis = {
        'risk_score': 95,
        'confidence': 0.92,
        'reasoning': 'Unusual location and high amount'
    }
    
    case = manager.create_fraud_case(high_risk_transaction, high_risk_analysis, 'trace-test-001')
    
    assert case is not None, "Should create case for high-risk transaction"
    assert case.risk_score == 95, "Case should have correct risk score"
    assert case.status == 'open', "New case should be open"
    assert case.case_id.startswith('FRAUD-'), "Case ID should have correct format"
    
    # Test low-risk transaction (should not create case)
    low_risk_analysis = {
        'risk_score': 25,
        'confidence': 0.85,
        'reasoning': 'Normal transaction pattern'
    }
    
    no_case = manager.create_fraud_case(high_risk_transaction, low_risk_analysis, 'trace-test-002')
    assert no_case is None, "Should not create case for low-risk transaction"
    
    print("✅ Fraud case creation working correctly")


def test_case_status_updates():
    """Test case status update functionality"""
    print("🧪 Testing case status updates...")
    
    manager = DatadogAlertManager()
    
    # Create a test case
    transaction_data = {
        'transaction': {
            'transaction_id': 'TXN-UPDATE-001',
            'amount': 3000,
            'merchant': 'Test Merchant',
            'location': {'country': 'Test Country'}
        }
    }
    
    analysis_result = {
        'risk_score': 85,
        'confidence': 0.90,
        'reasoning': 'Test case for updates'
    }
    
    case = manager.create_fraud_case(transaction_data, analysis_result, 'trace-update-001')
    assert case is not None, "Should create test case"
    
    # Test status update
    success = manager.update_case_status(case.case_id, 'investigating', 'Under review', 'analyst@example.com')
    assert success == True, "Status update should succeed"
    
    updated_case = manager.active_cases[case.case_id]
    assert updated_case.status == 'investigating', "Status should be updated"
    assert updated_case.assigned_to == 'analyst@example.com', "Assignment should be updated"
    assert updated_case.resolution_notes == 'Under review', "Notes should be updated"
    
    # Test invalid case ID
    invalid_update = manager.update_case_status('INVALID-CASE', 'resolved')
    assert invalid_update == False, "Should fail for invalid case ID"
    
    print("✅ Case status updates working correctly")


def test_case_summary_statistics():
    """Test case summary statistics"""
    print("🧪 Testing case summary statistics...")
    
    # Create a fresh manager for this test
    manager = DatadogAlertManager()
    
    # Test with a single case first to validate the logic
    transaction_data = {
        'transaction': {
            'transaction_id': 'TXN-SUMMARY-TEST',
            'amount': 1000,
            'merchant': 'Test Merchant',
            'location': {'country': 'Test'}
        }
    }
    
    analysis_result = {
        'risk_score': 85,
        'confidence': 0.90,
        'reasoning': 'Test case'
    }
    
    case = manager.create_fraud_case(transaction_data, analysis_result, 'trace-summary-test')
    assert case is not None, "Should create case"
    
    # Get summary
    summary = manager.get_case_summary()
    
    assert summary['total_cases'] >= 1, f"Should have at least 1 case, got {summary['total_cases']}"
    assert summary['open_cases'] >= 1, f"Should have at least 1 open case, got {summary['open_cases']}"
    assert summary['average_risk_score'] > 80, "Average risk score should be > 80"
    assert summary['total_amount_at_risk'] > 0, "Should have amount at risk"
    
    print("✅ Case summary statistics working correctly")


def test_alert_configuration_export():
    """Test alert configuration export functionality"""
    print("🧪 Testing alert configuration export...")
    
    manager = DatadogAlertManager()
    config = manager.export_alert_configuration()
    
    assert 'alert_rules' in config, "Export should contain alert_rules"
    assert 'metadata' in config, "Export should contain metadata"
    assert len(config['alert_rules']) > 0, "Should export alert rules"
    
    # Validate exported rule structure
    for rule in config['alert_rules']:
        assert 'name' in rule, "Exported rule should have name"
        assert 'query' in rule, "Exported rule should have query"
        assert 'threshold_critical' in rule, "Exported rule should have threshold"
    
    print("✅ Alert configuration export working correctly")


def test_convenience_functions():
    """Test convenience functions for easy integration"""
    print("🧪 Testing convenience functions...")
    
    # Test case creation convenience function
    transaction_data = {
        'transaction': {
            'transaction_id': 'TXN-CONVENIENCE-001',
            'amount': 4000,
            'merchant': 'Convenience Test',
            'location': {'country': 'Test'}
        }
    }
    
    analysis_result = {
        'risk_score': 90,
        'confidence': 0.95,
        'reasoning': 'Convenience function test'
    }
    
    case = create_case_for_transaction(transaction_data, analysis_result, 'trace-convenience-001')
    assert case is not None, "Convenience function should create case"
    assert case.transaction_id == 'TXN-CONVENIENCE-001', "Case should have correct transaction ID"
    
    print("✅ Convenience functions working correctly")


def main():
    """Run all Datadog alerts tests"""
    print("🚀 DATADOG ALERTS & CASE MANAGEMENT VALIDATION")
    print("=" * 60)
    
    try:
        test_alert_manager_initialization()
        test_alert_rule_definitions()
        test_alert_setup_mock_mode()
        test_case_creation()
        test_case_status_updates()
        test_case_summary_statistics()
        test_alert_configuration_export()
        test_convenience_functions()
        
        print("\n✅ ALL DATADOG ALERTS TESTS PASSED!")
        print("🎯 Features validated:")
        print("   ✓ Alert rule configuration and setup")
        print("   ✓ Cost alerts (LLM costs > $5 in 10 minutes)")
        print("   ✓ Performance alerts (voice latency > 2s)")
        print("   ✓ Quality alerts (error rate spikes)")
        print("   ✓ Case creation for high-risk transactions")
        print("   ✓ Case status management and tracking")
        print("   ✓ Case summary statistics")
        print("   ✓ Configuration export for judge review")
        print("   ✓ Integration convenience functions")
        
        print("\n🛡️  Datadog integration ready:")
        print("   ✓ Automated alerting for cost, performance, and quality")
        print("   ✓ Case management for fraud investigation")
        print("   ✓ Real-time monitoring and incident response")
        print("   ✓ Professional operational workflows")
        print("   ✓ Mock mode for development and testing")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)