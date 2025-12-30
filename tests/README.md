# Test Architecture

This repository uses layered testing to ensure correctness, reliability, and judge-ready evaluation.

## Test Layers

### `unit/` - Fast logic validation with no external dependencies
- `test_fraud_detection_simple.py` - Simplified AI fraud analysis logic
- `test_topic_recovery_simple.py` - Topic recovery logic validation

### `core/` - Core system behavior and AI workflows  
- `test_fraud_detection.py` - AI fraud analysis with Gemini 1.5 Flash
- `test_voice_signaling.py` - ElevenLabs voice call orchestration
- `test_datadog_alerts.py` - Monitoring and alert system
- `test_metrics.py` - Custom metrics collection

### `integration/` - Multi-component and pipeline validation
- `test_end_to_end.py` - Complete pipeline integration
- `test_closed_loop_complete.py` - Full closed-loop system validation
- `test_closed_loop_demo.py` - Demo-specific closed-loop tests
- `test_flink_ai_inference.py` - Flink SQL AI pipeline

### `stress/` - Load, performance, and chaos testing
- `test_end_to_end_stress.py` - Performance and load testing
- `test_traffic_generator.py` - Load testing and chaos engineering

### `reliability/` - Failure handling, retries, and recovery
- `test_retry_logic.py` - Retry framework and circuit breakers
- `test_log_sanitization.py` - PII protection and professional logging
- `test_topic_recovery.py` - Kafka topic auto-recovery (comprehensive)
- `test_idempotent_setup.py` - Infrastructure setup reliability

### `validation/` - Clean-room and environment verification
- `test_clean_room_environment.py` - Clean room deployment validation
- `test_env_validation.py` - Environment configuration validation
- `test_postmortem_analysis.py` - Fraud pattern analysis

## Running Tests

### All Tests
```bash
python main.py --test
```

### By Layer
```bash
# Fast unit tests (no external dependencies)
python -m pytest tests/unit/ -v

# Core functionality
python -m pytest tests/core/ -v

# Integration testing
python -m pytest tests/integration/ -v

# Performance testing
python -m pytest tests/stress/ -v

# Reliability testing
python -m pytest tests/reliability/ -v

# Environment validation
python -m pytest tests/validation/ -v
```

### Individual Tests
```bash
# Quick validation
python tests/unit/test_fraud_detection_simple.py
python tests/unit/test_topic_recovery_simple.py

# Core features
python tests/core/test_fraud_detection.py
python tests/core/test_voice_signaling.py

# Full pipeline
python tests/integration/test_end_to_end.py
python tests/integration/test_closed_loop_complete.py
```

## Mock Mode Support

All tests support mock mode for evaluation without API keys:
- **Unit tests**: Pure logic validation, no external calls
- **Core tests**: Mock AI and voice services automatically
- **Integration tests**: End-to-end pipeline with mocked external services
- **Stress tests**: Load testing with simulated responses
- **Reliability tests**: Error injection and recovery validation
- **Validation tests**: Clean room environment simulation

## Test Philosophy

This test architecture follows the **Test Pyramid** principle:
- **Many unit tests** (fast, isolated, no dependencies)
- **Some integration tests** (realistic scenarios, mocked externals)
- **Few end-to-end tests** (complete system validation)

Plus additional layers for production readiness:
- **Stress tests** for performance validation
- **Reliability tests** for failure scenarios
- **Validation tests** for deployment readiness

All tests are designed to pass in mock mode for immediate judge evaluation while also supporting real API integration for production deployment.