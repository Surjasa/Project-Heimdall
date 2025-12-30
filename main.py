#!/usr/bin/env python3
"""
Project Heimdall - Security Nervous System Orchestrator
Real-Time AI Fraud Intervention System

The "One-Command" launcher for the complete security infrastructure:
- The Brain (AI Fraud Consumer)
- The Mouth (Voice Signaling Server) 
- The Nervous System (Kafka Streaming)
- The Monitor (Metrics & Observability)

Usage:
    python main.py --start-security-system    # Launch complete system
    python main.py --demo                     # Run quick demo
    python main.py --setup                    # Setup infrastructure
    python main.py --test                     # Run all tests
"""

import sys
import os
import argparse
import asyncio
import time
import threading
import subprocess
from typing import Optional, List
from concurrent.futures import ThreadPoolExecutor

# Add src to path
sys.path.append('src')

class SecuritySystemOrchestrator:
    """Orchestrates the complete Project Heimdall security nervous system"""
    
    def __init__(self):
        self.processes = {}
        self.running = False
        self.executor = ThreadPoolExecutor(max_workers=4)
    
    def start_component(self, name: str, command: List[str], cwd: str = None) -> subprocess.Popen:
        """Start a system component as a background process"""
        try:
            print(f"🚀 Starting {name}...")
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=cwd,
                bufsize=1,
                universal_newlines=True
            )
            self.processes[name] = process
            print(f"✅ {name} started (PID: {process.pid})")
            return process
        except Exception as e:
            print(f"❌ Failed to start {name}: {e}")
            return None
    
    def monitor_component(self, name: str, process: subprocess.Popen):
        """Monitor a component's output in the background"""
        try:
            while process.poll() is None and self.running:
                # Use non-blocking read with timeout
                try:
                    output = process.stdout.readline()
                    if output:
                        print(f"[{name}] {output.strip()}")
                except:
                    pass  # Non-blocking, continue if no output
                time.sleep(0.5)  # Reduced frequency to avoid CPU spinning
        except Exception as e:
            print(f"❌ Error monitoring {name}: {e}")
    
    def start_security_system(self):
        """Launch the complete Project Heimdall security nervous system"""
        print("🛡️  PROJECT HEIMDALL - SECURITY NERVOUS SYSTEM")
        print("=" * 60)
        print("🧠 The Brain: AI Fraud Detection Consumer")
        print("📞 The Mouth: Voice Signaling Server")
        print("🌊 The Nervous System: Kafka Streaming Infrastructure")
        print("📊 The Monitor: Datadog Observability")
        print("=" * 60)
        
        self.running = True
        
        # Component 1: The Voice Signaling Server (The Mouth)
        voice_process = self.start_component(
            "Voice Signaling Server",
            [sys.executable, "src/voice_signaling.py", "--port", "8000"]
        )
        
        if voice_process:
            # Start monitoring in background
            threading.Thread(
                target=self.monitor_component,
                args=("Voice Server", voice_process),
                daemon=True
            ).start()
        
        # Give voice server time to start
        time.sleep(1)  # Reduced from 3 seconds
        
        # Component 2: The AI Fraud Consumer (The Brain)
        consumer_process = self.start_component(
            "AI Fraud Consumer",
            [sys.executable, "src/streaming/consumer.py"]
        )
        
        if consumer_process:
            # Start monitoring in background
            threading.Thread(
                target=self.monitor_component,
                args=("Fraud Consumer", consumer_process),
                daemon=True
            ).start()
        
        # Give consumer time to start
        time.sleep(1)  # Reduced from 2 seconds
        
        print("\n🎯 SECURITY NERVOUS SYSTEM ACTIVE!")
        print("=" * 60)
        print("📊 Datadog Dashboard: Monitor at https://app.datadoghq.com")
        print("📞 Voice Server API: http://localhost:8000")
        print("🧠 Fraud Consumer: Processing transactions in real-time")
        print("🌊 Kafka Streaming: Ready for transaction flow")
        print()
        print("🎬 To generate test transactions:")
        print("   python traffic_generator.py")
        print()
        print("⏹️  Press Ctrl+C to shutdown the security system")
        
        try:
            # Keep the orchestrator running
            while self.running:
                # Check if processes are still alive
                alive_count = 0
                for name, process in self.processes.items():
                    if process and process.poll() is None:
                        alive_count += 1
                    elif process and process.poll() is not None:
                        print(f"⚠️  {name} has stopped (exit code: {process.poll()})")
                
                if alive_count == 0:
                    print("❌ All components have stopped")
                    break
                
                time.sleep(2)  # Reduced check frequency from 5 seconds
                
        except KeyboardInterrupt:
            print("\n⏹️  Shutting down security nervous system...")
            self.shutdown_system()
    
    def quick_component_test(self):
        """Quick test to validate components can start without running full system"""
        print("🧪 QUICK COMPONENT VALIDATION")
        print("=" * 50)
        
        # Test 1: Voice Server startup
        print("🔍 Testing Voice Server startup...")
        voice_process = self.start_component(
            "Voice Server Test",
            [sys.executable, "src/voice_signaling.py", "--port", "8001"],  # Different port to avoid conflicts
        )
        
        if voice_process:
            time.sleep(2)  # Give it time to start
            if voice_process.poll() is None:
                print("✅ Voice Server starts successfully")
                voice_process.terminate()
                voice_process.wait(timeout=5)
            else:
                print("❌ Voice Server failed to start")
        
        # Test 2: Consumer startup
        print("🔍 Testing Consumer startup...")
        consumer_process = self.start_component(
            "Consumer Test",
            [sys.executable, "src/streaming/consumer.py"]
        )
        
        if consumer_process:
            time.sleep(2)  # Give it time to start
            if consumer_process.poll() is None:
                print("✅ Consumer starts successfully")
                consumer_process.terminate()
                consumer_process.wait(timeout=5)
            else:
                print("❌ Consumer failed to start")
        
        print("✅ Quick component validation complete")
        return True
    
    def shutdown_system(self):
        """Gracefully shutdown all components"""
        self.running = False
        
        print("🔄 Stopping all components...")
        for name, process in self.processes.items():
            if process and process.poll() is None:
                print(f"⏹️  Stopping {name}...")
                process.terminate()
                try:
                    process.wait(timeout=10)
                    print(f"✅ {name} stopped gracefully")
                except subprocess.TimeoutExpired:
                    print(f"⚠️  Force killing {name}...")
                    process.kill()
        
        print("✅ Security nervous system shutdown complete")
    
    def start_demo_mode(self):
        """Start in demo mode with simulated transactions"""
        print("🎬 PROJECT HEIMDALL - DEMO MODE")
        print("=" * 50)
        print("🧪 Simulating complete fraud detection pipeline...")
        
        # Run a simple fraud detection demo
        os.system("python tests/test_fraud_detection.py")
    
    def start_traffic_generator(self):
        """Start traffic generator for testing"""
        print("🚛 Starting traffic generator...")
        print("🎯 Available options:")
        print("   • Quick demo: python traffic_generator.py --demo")
        print("   • Continuous load: python traffic_generator.py --continuous --duration 5 --rate 30")
        print("   • Chaos scenarios: python traffic_generator.py --chaos 'High Volume Burst'")
        print("   • List scenarios: python traffic_generator.py --list-scenarios")
        print()
        print("🚀 Running quick demo...")
        os.system("python traffic_generator.py --demo")


def run_setup():
    """Run Confluent Cloud setup"""
    print("🔧 Setting up Confluent Cloud infrastructure...")
    result = os.system("python setup_confluent.py")
    return result == 0

def run_tests():
    """Run comprehensive test suite"""
    print("🧪 Running comprehensive test suite...")
    
    tests = [
        # Unit tests (fast, no dependencies)
        "tests/unit/test_fraud_detection_simple.py",
        "tests/unit/test_topic_recovery_simple.py",
        
        # Core tests (main functionality)
        "tests/core/test_fraud_detection.py",
        "tests/core/test_voice_signaling.py",
        "tests/core/test_datadog_alerts.py",
        "tests/core/test_metrics.py",
        
        # Integration tests (multi-component)
        "tests/integration/test_end_to_end.py",
        "tests/integration/test_closed_loop_complete.py",
        "tests/integration/test_closed_loop_demo.py",
        "tests/integration/test_flink_ai_inference.py",
        
        # Stress tests (performance)
        "tests/stress/test_end_to_end_stress.py",
        "tests/stress/test_traffic_generator.py",
        
        # Reliability tests (bulletproofing)
        "tests/reliability/test_retry_logic.py",
        "tests/reliability/test_log_sanitization.py",
        "tests/reliability/test_topic_recovery.py",
        "tests/reliability/test_idempotent_setup.py",
        
        # Validation tests (environment)
        "tests/validation/test_clean_room_environment.py",
        "tests/validation/test_env_validation.py",
        "tests/validation/test_postmortem_analysis.py"
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        print(f"\n🔍 Running {test}...")
        result = os.system(f"python {test}")
        if result == 0:
            print(f"✅ {test} PASSED")
            passed += 1
        else:
            print(f"❌ {test} FAILED")
            failed += 1
    
    print(f"\n📊 Test Results: {passed} passed, {failed} failed")
    return failed == 0

def print_system_status():
    """Print system status and available commands"""
    print("PROJECT HEIMDALL - Real-Time AI Fraud Intervention")
    print("=" * 60)
    print()
    print("System Architecture:")
    print("   The Brain        - Gemini 1.5 Flash AI analysis")
    print("   The Mouth        - ElevenLabs conversational AI") 
    print("   Nervous System   - Confluent Cloud Kafka streaming")
    print("   The Monitor      - Datadog observability dashboard")
    print()
    print("Available Commands:")
    print("   python main.py --start-security-system  # Launch complete system")
    print("   python main.py --quick-test             # Quick component validation (fast)")
    print("   python main.py --setup                  # Setup infrastructure")
    print("   python main.py --demo                   # Run demonstration")
    print("   python main.py --test                   # Run all tests")
    print("   python main.py --traffic                # Generate test traffic")
    print("   python main.py --validate-env           # Validate environment configuration")
    print()
    print("Quick Start (Judge Demo):")
    print("   1. python main.py --setup")
    print("   2. python main.py --start-security-system")
    print("   3. python main.py --traffic  (in new terminal)")
    print()
    print("Monitoring:")
    print("   • Datadog Dashboard: https://app.datadoghq.com")
    print("   • Voice Server API: http://localhost:8000")
    print("   • System Health: http://localhost:8000/health")
    print()
    print("The Customer Experience:")
    print("   1. Customer swipes card for $2,000 jewelry in Nigeria")
    print("   2. AI detects 97% fraud probability in <500ms")
    print("   3. Card is soft-blocked, customer's phone rings in <2 seconds")
    print("   4. AI Agent: 'Hello, this is security. Was this purchase you?'")
    print("   5. Customer confirms → Card unblocked instantly")
    print("   6. Fraud prevented before completion")

def main():
    """Main entry point for Project Heimdall Security Nervous System"""
    parser = argparse.ArgumentParser(
        description="Project Heimdall - Real-Time AI Fraud Intervention Security System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Project Heimdall Security Nervous System

This system operates as the "security nervous system" of a bank:
- Detects fraud in real-time using AI
- Calls customers immediately for verification  
- Prevents fraud before it completes

Examples:
  python main.py --start-security-system    Launch complete system
  python main.py --setup                    Setup infrastructure  
  python main.py --demo                     Run demonstration
  python main.py --test                     Run all tests
  python main.py --traffic                  Generate test traffic
        """
    )
    
    parser.add_argument('--start-security-system', action='store_true',
                       help='Launch the complete Project Heimdall security nervous system')
    parser.add_argument('--quick-test', action='store_true',
                       help='Quick validation that components can start (fast)')
    parser.add_argument('--setup', action='store_true', 
                       help='Setup Confluent Cloud infrastructure')
    parser.add_argument('--demo', action='store_true',
                       help='Run fraud detection demonstration')
    parser.add_argument('--test', action='store_true',
                       help='Run comprehensive test suite')
    parser.add_argument('--traffic', action='store_true',
                       help='Generate test traffic for the system')
    parser.add_argument('--validate-env', action='store_true',
                       help='Validate environment configuration')
    
    args = parser.parse_args()
    
    # If no arguments provided, show status
    if not any([args.start_security_system, args.quick_test, args.setup, args.demo, args.test, args.traffic, args.validate_env]):
        print_system_status()
        return 0
    
    try:
        if args.setup:
            success = run_setup()
            return 0 if success else 1
        elif args.quick_test:
            orchestrator = SecuritySystemOrchestrator()
            success = orchestrator.quick_component_test()
            return 0 if success else 1
        elif args.demo:
            orchestrator = SecuritySystemOrchestrator()
            orchestrator.start_demo_mode()
            return 0
        elif args.test:
            success = run_tests()
            return 0 if success else 1
        elif args.traffic:
            orchestrator = SecuritySystemOrchestrator()
            orchestrator.start_traffic_generator()
            return 0
        elif args.validate_env:
            from src.utils.env_validator import print_validation_report
            print_validation_report()
            return 0
        elif args.start_security_system:
            orchestrator = SecuritySystemOrchestrator()
            orchestrator.start_security_system()
            return 0
        
        return 0
        
    except KeyboardInterrupt:
        print("\n⏹️  Operation cancelled by user")
        return 1
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return 1

if __name__ == "__main__":
    exit(main())