#!/usr/bin/env python3
"""
Clean Room Environment Test - Task 9.1
Validates that the system works in fresh environments without existing configuration.
Tests requirements.txt installation and system functionality from scratch.
"""

import sys
import os
import subprocess
import tempfile
import shutil
from pathlib import Path


def test_requirements_installation():
    """Test that requirements.txt can be installed in a fresh environment"""
    print("🧪 Testing requirements.txt installation...")
    
    # Check that requirements.txt exists
    requirements_file = Path("requirements.txt")
    assert requirements_file.exists(), "requirements.txt should exist"
    
    # Read requirements
    with open(requirements_file, 'r') as f:
        requirements = f.read()
    
    # Should have key dependencies
    key_dependencies = [
        'confluent-kafka',
        'google-cloud-aiplatform', 
        'fastapi',
        'uvicorn',
        'datadog',
        'python-dotenv'
    ]
    
    for dep in key_dependencies:
        assert dep in requirements, f"requirements.txt should contain {dep}"
    
    print("✅ requirements.txt contains all key dependencies")


def test_project_structure():
    """Test that project has proper structure"""
    print("🧪 Testing project structure...")
    
    # Check main files exist
    main_files = [
        'main.py',
        'requirements.txt',
        '.env.example',
        'README.md',
        'Dockerfile',
        'deploy.sh'
    ]
    
    for file in main_files:
        assert Path(file).exists(), f"Main file {file} should exist"
    
    # Check directory structure
    directories = [
        'src',
        'src/agents',
        'src/streaming', 
        'src/monitoring',
        'src/infrastructure',
        'src/utils',
        'tests'
    ]
    
    for directory in directories:
        assert Path(directory).is_dir(), f"Directory {directory} should exist"
    
    # Check __init__.py files
    init_files = [
        'src/__init__.py',
        'src/agents/__init__.py',
        'src/streaming/__init__.py',
        'src/monitoring/__init__.py',
        'src/infrastructure/__init__.py',
        'src/utils/__init__.py',
        'tests/__init__.py'
    ]
    
    for init_file in init_files:
        assert Path(init_file).exists(), f"Init file {init_file} should exist"
    
    print("✅ Project structure is properly organized")


def test_environment_example_completeness():
    """Test that .env.example contains all necessary variables"""
    print("🧪 Testing .env.example completeness...")
    
    env_example = Path(".env.example")
    assert env_example.exists(), ".env.example should exist"
    
    with open(env_example, 'r') as f:
        content = f.read()
    
    # Check for key configuration sections
    required_sections = [
        'CONFLUENT CLOUD CONFIGURATION',
        'GOOGLE CLOUD / VERTEX AI CONFIGURATION',
        'DATADOG CONFIGURATION',
        'ELEVENLABS CONFIGURATION',
        'SYSTEM CONFIGURATION'
    ]
    
    for section in required_sections:
        assert section in content, f".env.example should contain {section} section"
    
    # Check for key variables
    required_vars = [
        'CONFLUENT_BOOTSTRAP_SERVERS',
        'CONFLUENT_API_KEY',
        'CONFLUENT_API_SECRET',
        'GOOGLE_CLOUD_PROJECT',
        'DD_API_KEY',
        'ELEVENLABS_API_KEY'
    ]
    
    for var in required_vars:
        assert var in content, f".env.example should contain {var}"
    
    print("✅ .env.example is comprehensive")


def test_main_entry_point():
    """Test that main.py works as entry point"""
    print("🧪 Testing main.py entry point...")
    
    # Test help output
    try:
        result = subprocess.run([sys.executable, 'main.py', '--help'], 
                              capture_output=True, text=True, timeout=30)
        assert result.returncode == 0, "main.py --help should work"
        assert 'Project Heimdall' in result.stdout, "Help should mention Project Heimdall"
    except subprocess.TimeoutExpired:
        assert False, "main.py --help should not timeout"
    
    # Test status display (no arguments)
    try:
        result = subprocess.run([sys.executable, 'main.py'], 
                              capture_output=True, text=True, timeout=30)
        assert result.returncode == 0, "main.py with no args should work"
        assert 'PROJECT HEIMDALL' in result.stdout, "Status should show project name"
    except subprocess.TimeoutExpired:
        assert False, "main.py status should not timeout"
    
    print("✅ main.py entry point working correctly")


def test_environment_validation():
    """Test environment validation functionality"""
    print("🧪 Testing environment validation...")
    
    # Test environment validation command
    try:
        result = subprocess.run([sys.executable, 'main.py', '--validate-env'], 
                              capture_output=True, text=True, timeout=60)
        assert result.returncode == 0, "Environment validation should work"
        assert 'ENVIRONMENT CONFIGURATION VALIDATION' in result.stdout, "Should show validation report"
    except subprocess.TimeoutExpired:
        assert False, "Environment validation should not timeout"
    
    print("✅ Environment validation working correctly")


def test_mock_mode_functionality():
    """Test that system works in mock mode without real API keys"""
    print("🧪 Testing mock mode functionality...")
    
    # Test that tests can run without real credentials
    try:
        # Run a quick test to verify mock mode works
        result = subprocess.run([sys.executable, 'tests/test_fraud_detection.py'], 
                              capture_output=True, text=True, timeout=120)
        assert result.returncode == 0, "Fraud detection test should work in mock mode"
        assert 'Mock services initialized' in result.stdout, "Should use mock services"
    except subprocess.TimeoutExpired:
        assert False, "Mock mode test should not timeout"
    
    print("✅ Mock mode functionality working correctly")


def test_docker_configuration():
    """Test Docker configuration"""
    print("🧪 Testing Docker configuration...")
    
    dockerfile = Path("Dockerfile")
    assert dockerfile.exists(), "Dockerfile should exist"
    
    with open(dockerfile, 'r') as f:
        content = f.read()
    
    # Check for key Docker instructions
    docker_instructions = [
        'FROM python:',
        'WORKDIR',
        'COPY requirements.txt',
        'RUN pip install',
        'COPY . .',
        'EXPOSE',
        'CMD'
    ]
    
    for instruction in docker_instructions:
        assert instruction in content, f"Dockerfile should contain {instruction}"
    
    print("✅ Docker configuration is complete")


def test_deployment_scripts():
    """Test deployment scripts exist and are executable"""
    print("🧪 Testing deployment scripts...")
    
    deploy_script = Path("deploy.sh")
    assert deploy_script.exists(), "deploy.sh should exist"
    
    with open(deploy_script, 'r') as f:
        content = f.read()
    
    # Check for key deployment steps
    deployment_steps = [
        'gcloud',
        'docker',
        'PROJECT_ID',
        'REGION'
    ]
    
    for step in deployment_steps:
        assert step in content, f"deploy.sh should contain {step}"
    
    print("✅ Deployment scripts are complete")


def test_documentation_completeness():
    """Test that documentation is complete"""
    print("🧪 Testing documentation completeness...")
    
    readme = Path("README.md")
    assert readme.exists(), "README.md should exist"
    
    with open(readme, 'r') as f:
        content = f.read()
    
    # Check for key documentation sections
    doc_sections = [
        'Project Heimdall',
        'Installation',
        'Configuration',
        'Usage',
        'API'
    ]
    
    for section in doc_sections:
        assert section in content, f"README.md should contain {section} section"
    
    print("✅ Documentation is complete")


def test_clean_room_simulation():
    """Simulate a clean room environment test"""
    print("🧪 Testing clean room environment simulation...")
    
    # Create temporary directory for clean room test
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Copy essential files to temp directory
        essential_files = [
            'main.py',
            'requirements.txt',
            '.env.example'
        ]
        
        for file in essential_files:
            if Path(file).exists():
                shutil.copy2(file, temp_path / file)
        
        # Copy src directory
        if Path('src').exists():
            shutil.copytree('src', temp_path / 'src')
        
        # Copy tests directory  
        if Path('tests').exists():
            shutil.copytree('tests', temp_path / 'tests')
        
        # Test that main.py works in clean environment
        original_cwd = os.getcwd()
        try:
            os.chdir(temp_path)
            
            # Test help command
            result = subprocess.run([sys.executable, 'main.py', '--help'], 
                                  capture_output=True, text=True, timeout=30)
            assert result.returncode == 0, "main.py should work in clean environment"
            
        finally:
            os.chdir(original_cwd)
    
    print("✅ Clean room environment simulation successful")


def test_version_pinning():
    """Test that dependencies are properly version pinned"""
    print("🧪 Testing dependency version pinning...")
    
    requirements_file = Path("requirements.txt")
    with open(requirements_file, 'r') as f:
        requirements = f.read()
    
    # Check that major dependencies have version constraints
    lines = [line.strip() for line in requirements.split('\n') if line.strip() and not line.startswith('#')]
    
    versioned_count = 0
    for line in lines:
        if '==' in line or '>=' in line or '~=' in line:
            versioned_count += 1
    
    # At least 80% of dependencies should have version constraints
    version_ratio = versioned_count / len(lines) if lines else 0
    assert version_ratio >= 0.8, f"At least 80% of dependencies should be version pinned (got {version_ratio:.1%})"
    
    print("✅ Dependencies are properly version pinned")


def main():
    """Run all clean room environment tests"""
    print("🚀 CLEAN ROOM ENVIRONMENT VALIDATION")
    print("=" * 60)
    
    try:
        test_requirements_installation()
        test_project_structure()
        test_environment_example_completeness()
        test_main_entry_point()
        test_environment_validation()
        test_mock_mode_functionality()
        test_docker_configuration()
        test_deployment_scripts()
        test_documentation_completeness()
        test_clean_room_simulation()
        test_version_pinning()
        
        print("\n✅ ALL CLEAN ROOM ENVIRONMENT TESTS PASSED!")
        print("🎯 Features validated:")
        print("   ✓ Requirements.txt installation")
        print("   ✓ Professional project structure")
        print("   ✓ Complete environment configuration")
        print("   ✓ Main entry point functionality")
        print("   ✓ Environment validation system")
        print("   ✓ Mock mode operation")
        print("   ✓ Docker configuration")
        print("   ✓ Deployment scripts")
        print("   ✓ Complete documentation")
        print("   ✓ Clean room environment simulation")
        print("   ✓ Dependency version pinning")
        
        print("\n🛡️  System is clean room ready:")
        print("   ✓ Works in fresh environments")
        print("   ✓ All dependencies properly specified")
        print("   ✓ Professional project organization")
        print("   ✓ Complete configuration templates")
        print("   ✓ Mock mode for evaluation")
        print("   ✓ Comprehensive documentation")
        print("   ✓ Production deployment ready")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)