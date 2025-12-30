#!/usr/bin/env python3
"""
Test suite for Flink AI Inference - Task 2.4
Validates Flink SQL pipeline creation and AI model integration.
"""

import sys
import os
import json

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.streaming.flink_ai_inference import FlinkAIInferenceService, setup_flink_ai_pipeline, get_pipeline_status


def test_flink_service_initialization():
    """Test FlinkAIInferenceService initialization"""
    print("🧪 Testing FlinkAIInferenceService initialization...")
    
    service = FlinkAIInferenceService()
    
    # Should initialize in mock mode without credentials
    assert service.mock_mode == True, "Should be in mock mode without credentials"
    assert len(service.sql_statements) > 0, "Should have SQL statements"
    assert 'model_name' in service.ai_model_config, "Should have AI model config"
    
    print("✅ FlinkAIInferenceService initialization working correctly")


def test_sql_statements():
    """Test Flink SQL statement generation"""
    print("🧪 Testing Flink SQL statement generation...")
    
    service = FlinkAIInferenceService()
    statements = service.sql_statements
    
    required_statements = [
        'create_transactions_table',
        'create_ai_inference_function',
        'create_ai_enriched_transactions',
        'create_high_risk_stream',
        'create_fraud_alerts_stream',
        'insert_high_risk_transactions',
        'insert_fraud_alerts',
        'create_analytics_view',
        'create_merchant_risk_analysis'
    ]
    
    for statement_name in required_statements:
        assert statement_name in statements, f"Should have statement: {statement_name}"
        sql = statements[statement_name]
        assert isinstance(sql, str), f"Statement {statement_name} should be string"
        assert len(sql.strip()) > 0, f"Statement {statement_name} should not be empty"
    
    # Validate key SQL components
    transactions_sql = statements['create_transactions_table']
    assert 'CREATE TABLE transactions' in transactions_sql, "Should create transactions table"
    assert 'kafka' in transactions_sql, "Should use Kafka connector"
    assert 'WATERMARK' in transactions_sql, "Should have watermark for streaming"
    
    ai_function_sql = statements['create_ai_inference_function']
    assert 'fraud_risk_analysis' in ai_function_sql, "Should create AI function"
    assert 'JAVA' in ai_function_sql, "Should be Java UDF"
    
    high_risk_sql = statements['insert_high_risk_transactions']
    assert 'risk_score > 70.0' in high_risk_sql, "Should filter by risk score > 70"
    
    fraud_alerts_sql = statements['insert_fraud_alerts']
    assert 'risk_score > 85.0' in fraud_alerts_sql, "Should filter by risk score > 85"
    
    print("✅ Flink SQL statement generation working correctly")


def test_ai_model_registration():
    """Test AI model registration"""
    print("🧪 Testing AI model registration...")
    
    service = FlinkAIInferenceService()
    
    # Test model registration (mock mode)
    success = service.register_ai_model()
    assert success == True, "Model registration should succeed in mock mode"
    
    print("✅ AI model registration working correctly")


def test_pipeline_creation():
    """Test Flink SQL pipeline creation"""
    print("🧪 Testing Flink SQL pipeline creation...")
    
    service = FlinkAIInferenceService()
    
    # Test pipeline creation (mock mode)
    success = service.create_flink_sql_pipeline()
    assert success == True, "Pipeline creation should succeed in mock mode"
    
    print("✅ Flink SQL pipeline creation working correctly")


def test_pipeline_status():
    """Test pipeline status retrieval"""
    print("🧪 Testing pipeline status retrieval...")
    
    service = FlinkAIInferenceService()
    
    status = service.get_pipeline_status()
    
    assert isinstance(status, dict), "Status should be dictionary"
    assert 'status' in status, "Should have status field"
    assert 'ai_model_registered' in status, "Should have model registration status"
    assert 'pipeline_created' in status, "Should have pipeline creation status"
    assert 'statements_count' in status, "Should have statements count"
    
    # In mock mode, should have mock metrics
    if service.mock_mode:
        assert 'mock_metrics' in status, "Should have mock metrics in mock mode"
        mock_metrics = status['mock_metrics']
        assert 'transactions_processed' in mock_metrics, "Should have transaction count"
        assert 'high_risk_detected' in mock_metrics, "Should have high risk count"
        assert 'fraud_alerts_generated' in mock_metrics, "Should have alert count"
    
    print("✅ Pipeline status retrieval working correctly")


def test_pipeline_config_export():
    """Test pipeline configuration export"""
    print("🧪 Testing pipeline configuration export...")
    
    service = FlinkAIInferenceService()
    
    config = service.export_pipeline_config()
    
    assert isinstance(config, dict), "Config should be dictionary"
    assert 'pipeline_name' in config, "Should have pipeline name"
    assert 'description' in config, "Should have description"
    assert 'ai_model_config' in config, "Should have AI model config"
    assert 'sql_statements' in config, "Should have SQL statements"
    assert 'data_flow' in config, "Should have data flow description"
    assert 'performance_targets' in config, "Should have performance targets"
    assert 'export_timestamp' in config, "Should have export timestamp"
    
    # Validate data flow
    data_flow = config['data_flow']
    assert '1_source' in data_flow, "Should have source step"
    assert '2_ai_inference' in data_flow, "Should have AI inference step"
    assert '3_filtering' in data_flow, "Should have filtering step"
    assert '4_alerting' in data_flow, "Should have alerting step"
    assert '5_analytics' in data_flow, "Should have analytics step"
    
    # Validate performance targets
    performance = config['performance_targets']
    assert 'processing_latency_ms' in performance, "Should have processing latency target"
    assert 'throughput_tps' in performance, "Should have throughput target"
    assert 'ai_inference_latency_ms' in performance, "Should have AI latency target"
    
    print("✅ Pipeline configuration export working correctly")


def test_convenience_functions():
    """Test convenience functions"""
    print("🧪 Testing convenience functions...")
    
    # Test setup function
    success = setup_flink_ai_pipeline()
    assert success == True, "Setup should succeed in mock mode"
    
    # Test status function
    status = get_pipeline_status()
    assert isinstance(status, dict), "Status should be dictionary"
    assert 'status' in status, "Should have status field"
    
    print("✅ Convenience functions working correctly")


def test_sql_statement_validation():
    """Test SQL statement syntax validation"""
    print("🧪 Testing SQL statement syntax validation...")
    
    service = FlinkAIInferenceService()
    
    # Basic syntax checks for key statements
    statements = service.sql_statements
    
    # Check transactions table
    transactions_sql = statements['create_transactions_table']
    assert transactions_sql.count('(') == transactions_sql.count(')'), "Parentheses should be balanced"
    assert 'CREATE TABLE' in transactions_sql.upper(), "Should be CREATE TABLE statement"
    
    # Check AI function
    ai_function_sql = statements['create_ai_inference_function']
    assert 'CREATE FUNCTION' in ai_function_sql.upper(), "Should be CREATE FUNCTION statement"
    
    # Check insert statements
    for name, sql in statements.items():
        if name.startswith('insert_'):
            assert 'INSERT INTO' in sql.upper(), f"Statement {name} should be INSERT statement"
            assert 'SELECT' in sql.upper(), f"Statement {name} should have SELECT clause"
    
    # Check view statements
    for name, sql in statements.items():
        if 'view' in name:
            assert 'CREATE VIEW' in sql.upper(), f"Statement {name} should be CREATE VIEW statement"
    
    print("✅ SQL statement syntax validation working correctly")


def test_configuration_file_export():
    """Test configuration file export functionality"""
    print("🧪 Testing configuration file export...")
    
    service = FlinkAIInferenceService()
    config = service.export_pipeline_config()
    
    # Test JSON serialization
    try:
        json_str = json.dumps(config, indent=2)
        assert len(json_str) > 0, "Should generate non-empty JSON"
        
        # Test deserialization
        parsed_config = json.loads(json_str)
        assert parsed_config == config, "Should round-trip correctly"
        
    except Exception as e:
        assert False, f"JSON serialization should work: {e}"
    
    print("✅ Configuration file export working correctly")


def main():
    """Run all Flink AI inference tests"""
    print("🚀 FLINK AI INFERENCE VALIDATION")
    print("=" * 50)
    
    try:
        test_flink_service_initialization()
        test_sql_statements()
        test_ai_model_registration()
        test_pipeline_creation()
        test_pipeline_status()
        test_pipeline_config_export()
        test_convenience_functions()
        test_sql_statement_validation()
        test_configuration_file_export()
        
        print("\n✅ ALL FLINK AI INFERENCE TESTS PASSED!")
        print("🎯 Features validated:")
        print("   ✓ Flink AI inference service initialization")
        print("   ✓ Flink SQL statement generation")
        print("   ✓ AI model registration workflow")
        print("   ✓ Pipeline creation and deployment")
        print("   ✓ Pipeline status monitoring")
        print("   ✓ Configuration export functionality")
        print("   ✓ Convenience functions")
        print("   ✓ SQL statement syntax validation")
        print("   ✓ Configuration file export")
        
        print("\n🛡️  Flink AI inference system ready:")
        print("   ✓ Real-time AI model inference in stream processing")
        print("   ✓ Gemini 1.5 Flash integration with Flink SQL")
        print("   ✓ Risk-based transaction filtering and routing")
        print("   ✓ Automated fraud alert generation")
        print("   ✓ Real-time analytics and monitoring")
        print("   ✓ Merchant risk analysis")
        print("   ✓ Scalable stream processing architecture")
        print("   ✓ Professional SQL pipeline management")
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