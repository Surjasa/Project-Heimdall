#!/usr/bin/env python3
"""
Test suite for Environment Configuration Validator - Task 9.7
Validates environment configuration validation system.
"""

import sys
import os
import tempfile
from pathlib import Path

# Add src to path for imports
sys.path.append('src')

from utils.env_validator import EnvironmentValidator, validate_environment, print_validation_report


def test_env_validator_initialization():
    """Test EnvironmentValidator initialization"""
    print("🧪 Testing EnvironmentValidator initialization...")
    
    validator = EnvironmentValidator()
    
    # Should have environment definitions
    assert len(validator.env_definitions) > 0, "Should have environment variable definitions"
    
    # Check required variables are defined
    required_vars = [var for var, defn in validator.env_definitions.items() if defn.required]
    assert len(required_vars) > 0, "Should have required variables"
    
    # Check categories are defined
    categories = set(defn.category for defn in validator.env_definitions.values())
    expected_categories = ['confluent', 'google_cloud', 'system']
    for category in expected_categories:
        assert category in categories, f"Should have {category} category"
    
    print("✅ EnvironmentValidator initialization working correctly")


def test_validation_with_empty_environment():
    """Test validation with no environment variables set"""
    print("🧪 Testing validation with empty environment...")
    
    # Save current environment
    original_env = dict(os.environ)
    
    try:
        # Clear relevant environment variables
        env_vars_to_clear = [
            'CONFLUENT_BOOTSTRAP_SERVERS', 'CONFLUENT_API_KEY', 'CONFLUENT_API_SECRET',
            'GOOGLE_CLOUD_PROJECT', 'DD_API_KEY', 'ELEVENLABS_API_KEY'
        ]
        
        for var in env_vars_to_clear:
            if var in os.environ:
                del os.environ[var]
        
        validator = EnvironmentValidator()
        result = validator.validate_environment()
        
        # Should not be valid due to missing required variables
        assert not result.is_valid, "Should be invalid with missing required variables"
        assert len(result.missing_required) > 0, "Should have missing required variables"
        assert len(result.errors) > 0, "Should have validation errors"
        
        # Should have suggestions
        assert len(result.suggestions) > 0, "Should have helpful suggestions"
        
    finally:
        # Restore environment
        os.environ.clear()
        os.environ.update(original_env)
    
    print("✅ Empty environment validation working correctly")


def test_validation_with_placeholder_values():
    """Test validation with placeholder values"""
    print("🧪 Testing validation with placeholder values...")
    
    # Save current environment
    original_env = dict(os.environ)
    
    try:
        # Set placeholder values
        os.environ['CONFLUENT_BOOTSTRAP_SERVERS'] = 'your-bootstrap-servers'
        os.environ['CONFLUENT_API_KEY'] = 'your-api-key'
        os.environ['CONFLUENT_API_SECRET'] = 'your-api-secret'
        os.environ['GOOGLE_CLOUD_PROJECT'] = 'your-project-id'
        
        validator = EnvironmentValidator()
        result = validator.validate_environment()
        
        # Should not be valid due to placeholder values
        assert not result.is_valid, "Should be invalid with placeholder values"
        assert len(result.errors) > 0, "Should have errors for placeholder values"
        
        # Check specific error messages
        placeholder_errors = [error for error in result.errors if 'placeholder' in error]
        assert len(placeholder_errors) > 0, "Should have placeholder-specific errors"
        
    finally:
        # Restore environment
        os.environ.clear()
        os.environ.update(original_env)
    
    print("✅ Placeholder value validation working correctly")


def test_validation_with_valid_configuration():
    """Test validation with valid configuration"""
    print("🧪 Testing validation with valid configuration...")
    
    # Save current environment
    original_env = dict(os.environ)
    
    try:
        # Set valid values
        os.environ['CONFLUENT_BOOTSTRAP_SERVERS'] = 'pkc-test.us-central1.gcp.confluent.cloud:9092'
        os.environ['CONFLUENT_API_KEY'] = 'ABCDEFGHIJKLMNOP'
        os.environ['CONFLUENT_API_SECRET'] = 'abcdefghijklmnopqrstuvwxyz1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ12'
        os.environ['GOOGLE_CLOUD_PROJECT'] = 'my-fraud-detection-project'
        
        # Optional but valid values
        os.environ['DD_API_KEY'] = 'abcdef1234567890abcdef1234567890'
        os.environ['VOICE_SERVER_PORT'] = '8000'
        os.environ['LOG_LEVEL'] = 'INFO'
        os.environ['MOCK_MODE'] = 'false'
        
        validator = EnvironmentValidator()
        result = validator.validate_environment()
        
        # Should be valid
        assert result.is_valid, f"Should be valid with proper configuration. Errors: {result.errors}"
        assert len(result.missing_required) == 0, "Should have no missing required variables"
        
        # May have warnings for missing optional variables
        assert len(result.missing_optional) >= 0, "May have missing optional variables"
        
    finally:
        # Restore environment
        os.environ.clear()
        os.environ.update(original_env)
    
    print("✅ Valid configuration validation working correctly")


def test_format_validation():
    """Test format validation for specific variable types"""
    print("🧪 Testing format validation...")
    
    # Save current environment
    original_env = dict(os.environ)
    
    try:
        # Set required variables to valid values first
        os.environ['CONFLUENT_BOOTSTRAP_SERVERS'] = 'pkc-test.us-central1.gcp.confluent.cloud:9092'
        os.environ['CONFLUENT_API_KEY'] = 'ABCDEFGHIJKLMNOP'
        os.environ['CONFLUENT_API_SECRET'] = 'abcdefghijklmnopqrstuvwxyz1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ12'
        os.environ['GOOGLE_CLOUD_PROJECT'] = 'my-fraud-detection-project'
        
        # Test invalid formats
        os.environ['VOICE_SERVER_PORT'] = 'invalid-port'  # Should be number
        os.environ['DEFAULT_PHONE_NUMBER'] = '555-1234'  # Should be E.164 format
        os.environ['LOG_LEVEL'] = 'INVALID'  # Should be valid log level
        os.environ['MOCK_MODE'] = 'maybe'  # Should be boolean
        
        validator = EnvironmentValidator()
        result = validator.validate_environment()
        
        # Should not be valid due to format errors
        assert not result.is_valid, "Should be invalid with format errors"
        assert len(result.invalid_format) > 0, "Should have format validation errors"
        
        # Check specific format errors
        format_vars = ['VOICE_SERVER_PORT', 'DEFAULT_PHONE_NUMBER', 'LOG_LEVEL', 'MOCK_MODE']
        for var in format_vars:
            assert var in result.invalid_format, f"Should detect format error for {var}"
        
    finally:
        # Restore environment
        os.environ.clear()
        os.environ.update(original_env)
    
    print("✅ Format validation working correctly")


def test_env_file_loading():
    """Test loading environment variables from .env file"""
    print("🧪 Testing .env file loading...")
    
    # Create temporary .env file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.env', delete=False) as f:
        f.write("""
# Test environment file
CONFLUENT_BOOTSTRAP_SERVERS=pkc-test.us-central1.gcp.confluent.cloud:9092
CONFLUENT_API_KEY=TESTKEY123456789
CONFLUENT_API_SECRET=testsecret123456789012345678901234567890123456789012345678901234
GOOGLE_CLOUD_PROJECT=test-project-12345

# Optional settings
VOICE_SERVER_PORT=8000
LOG_LEVEL=DEBUG
MOCK_MODE=true
""")
        env_file_path = f.name
    
    try:
        validator = EnvironmentValidator()
        result = validator.validate_environment(env_file_path)
        
        # Should be valid with loaded configuration
        assert result.is_valid, f"Should be valid with .env file. Errors: {result.errors}"
        
        # Check that values were loaded
        assert os.getenv('CONFLUENT_BOOTSTRAP_SERVERS') == 'pkc-test.us-central1.gcp.confluent.cloud:9092'
        assert os.getenv('GOOGLE_CLOUD_PROJECT') == 'test-project-12345'
        assert os.getenv('LOG_LEVEL') == 'DEBUG'
        
    finally:
        # Clean up
        os.unlink(env_file_path)
    
    print("✅ .env file loading working correctly")


def test_category_validation():
    """Test category-specific validation logic"""
    print("🧪 Testing category-specific validation...")
    
    # Save current environment
    original_env = dict(os.environ)
    
    try:
        # Set partial Confluent configuration
        os.environ['CONFLUENT_BOOTSTRAP_SERVERS'] = 'pkc-test.us-central1.gcp.confluent.cloud:9092'
        os.environ['CONFLUENT_API_KEY'] = 'TESTKEY123456789'
        # Missing CONFLUENT_API_SECRET - this should trigger partial warning
        
        # Clear the secret to ensure it's missing
        if 'CONFLUENT_API_SECRET' in os.environ:
            del os.environ['CONFLUENT_API_SECRET']
        
        os.environ['GOOGLE_CLOUD_PROJECT'] = 'test-project-12345'
        
        validator = EnvironmentValidator()
        result = validator.validate_environment()
        
        # Should have warnings about partial configuration
        partial_warnings = [w for w in result.warnings if 'Partial' in w]
        assert len(partial_warnings) > 0, "Should warn about partial Confluent configuration"
        
    finally:
        # Restore environment
        os.environ.clear()
        os.environ.update(original_env)
    
    print("✅ Category-specific validation working correctly")


def test_validation_report_generation():
    """Test validation report generation"""
    print("🧪 Testing validation report generation...")
    
    validator = EnvironmentValidator()
    
    # Test with current environment (should work without errors)
    result = validator.validate_environment()
    
    # Should be able to print report without errors
    try:
        validator.print_validation_report(result)
        print("✅ Validation report generated successfully")
    except Exception as e:
        assert False, f"Report generation should not fail: {e}"
    
    print("✅ Validation report generation working correctly")


def test_convenience_functions():
    """Test convenience functions"""
    print("🧪 Testing convenience functions...")
    
    # Test validate_environment function
    result = validate_environment()
    assert hasattr(result, 'is_valid'), "Should return ValidationResult"
    assert hasattr(result, 'errors'), "Should have errors attribute"
    assert hasattr(result, 'warnings'), "Should have warnings attribute"
    
    # Test print_validation_report function (should not raise errors)
    try:
        print_validation_report()
    except Exception as e:
        assert False, f"print_validation_report should not fail: {e}"
    
    print("✅ Convenience functions working correctly")


def test_configuration_template_export():
    """Test configuration template export"""
    print("🧪 Testing configuration template export...")
    
    validator = EnvironmentValidator()
    
    # Export template to temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        template_file = f.name
    
    try:
        validator.export_configuration_template(template_file)
        
        # Check that file was created
        assert Path(template_file).exists(), "Template file should be created"
        
        # Check file content
        import json
        with open(template_file, 'r') as f:
            template = json.load(f)
        
        assert 'environment_variables' in template, "Should have environment_variables section"
        assert 'categories' in template, "Should have categories section"
        assert len(template['environment_variables']) > 0, "Should have variable definitions"
        
    finally:
        # Clean up
        if Path(template_file).exists():
            os.unlink(template_file)
    
    print("✅ Configuration template export working correctly")


def main():
    """Run all environment validation tests"""
    print("🚀 ENVIRONMENT CONFIGURATION VALIDATION TESTS")
    print("=" * 60)
    
    try:
        test_env_validator_initialization()
        test_validation_with_empty_environment()
        test_validation_with_placeholder_values()
        test_validation_with_valid_configuration()
        test_format_validation()
        test_env_file_loading()
        test_category_validation()
        test_validation_report_generation()
        test_convenience_functions()
        test_configuration_template_export()
        
        print("\n✅ ALL ENVIRONMENT VALIDATION TESTS PASSED!")
        print("🎯 Features validated:")
        print("   ✓ Environment validator initialization")
        print("   ✓ Empty environment validation")
        print("   ✓ Placeholder value detection")
        print("   ✓ Valid configuration validation")
        print("   ✓ Format validation for specific types")
        print("   ✓ .env file loading")
        print("   ✓ Category-specific validation")
        print("   ✓ Validation report generation")
        print("   ✓ Convenience functions")
        print("   ✓ Configuration template export")
        
        print("\n🛡️  Environment validation system ready:")
        print("   ✓ Comprehensive variable validation")
        print("   ✓ Format checking for URLs, phone numbers, etc.")
        print("   ✓ Category-specific validation logic")
        print("   ✓ Helpful error messages and suggestions")
        print("   ✓ .env file loading and validation")
        print("   ✓ Professional validation reporting")
        print("   ✓ Configuration template generation")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)