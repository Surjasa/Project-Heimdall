"""
Fraud Detection Service using Gemini 1.5 Flash with Datadog LLM Observability
Core AI logic for real-time fraud analysis
"""

import json
import time
from dataclasses import dataclass, asdict
from decimal import Decimal
from datetime import datetime
from typing import Dict, List, Any, Optional
from contextlib import nullcontext
import os

# Datadog LLM Observability SDK
from ddtrace.llmobs import LLMObs
import ddtrace

# Google Vertex AI
import vertexai
from vertexai.generative_models import GenerativeModel, Part
from google.api_core import exceptions as gcp_exceptions

# Initialize Datadog LLM Observability (only if API key is available)
DD_API_KEY = os.getenv("DD_API_KEY")
if DD_API_KEY:
    LLMObs.enable(
        ml_app="fraud-response-system",
        site="datadoghq.com",
        api_key=DD_API_KEY,
        agentless_enabled=True,
    )
    DATADOG_ENABLED = True
else:
    DATADOG_ENABLED = False
    print("⚠️  Datadog LLM Observability disabled (DD_API_KEY not set)")

# Initialize Vertex AI
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "your-project-id")
LOCATION = os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")
vertexai.init(project=PROJECT_ID, location=LOCATION)


@dataclass
class Location:
    country: str
    city: str
    coordinates: tuple[float, float]
    is_new_location: bool
    distance_from_home: float


@dataclass
class Transaction:
    transaction_id: str
    user_id: str
    amount: Decimal
    currency: str
    merchant: str
    merchant_category: str
    location: Location
    timestamp: datetime
    payment_method: str
    metadata: Dict[str, Any]


@dataclass
class UserContext:
    user_id: str
    home_location: Location
    spending_patterns: Dict[str, Any]
    recent_transactions: List[Dict[str, Any]]
    risk_profile: str  # "low", "medium", "high"
    phone_number: str


@dataclass
class RiskAssessment:
    transaction_id: str
    risk_score: float  # 0-100
    confidence: float  # 0-1
    risk_factors: List[str]
    model_used: str
    processing_time_ms: int
    recommendation: str  # "approve", "review", "call_user"


class FraudDetectionService:
    """AI-powered fraud detection using Gemini 1.5 Flash with Datadog observability"""
    
    def __init__(self):
        self.model = GenerativeModel("gemini-1.5-flash-002")
        self.model_name = "gemini-1.5-flash"
        
    @ddtrace.tracer.wrap("fraud_detection.analyze_transaction")
    def analyze_transaction_realtime(self, transaction: Transaction, user_context: UserContext) -> RiskAssessment:
        """
        Analyze transaction for fraud using Gemini 1.5 Flash
        Validates: Requirements 2.1, 2.2, 2.5
        """
        start_time = time.time()
        
        # Create analysis prompt with context
        prompt = self._create_fraud_analysis_prompt(transaction, user_context)
        
        # Get current trace ID for debugging
        trace_id = ddtrace.tracer.current_span().trace_id if ddtrace.tracer.current_span() else "No Trace Active"
        
        # Use Datadog LLM Observability to trace the AI call (if enabled)
        if DATADOG_ENABLED:
            llm_context = LLMObs.llm(
                model_name=self.model_name,
                name="fraud_analysis",
                model_provider="google",
                session_id=f"session_{transaction.user_id}",
                ml_app="fraud-response-system"
            )
        else:
            # Use a no-op context manager when Datadog is disabled
            from contextlib import nullcontext
            llm_context = nullcontext()
        
        with llm_context as span:
            # Annotate the input prompt (if span exists)
            if span and DATADOG_ENABLED:
                span.annotate(
                    input_data=prompt,
                    output_data=None,  # Will be set after response
                    metadata={
                        "transaction_id": transaction.transaction_id,
                        "user_id": transaction.user_id,
                        "amount": float(transaction.amount),
                        "trace_id": trace_id
                    }
                )
            
            try:
                # Call Gemini 1.5 Flash with optimized configuration
                generation_config = {
                    "temperature": 0.1,  # Low temperature for deterministic risk scoring
                    "response_mime_type": "application/json",  # Ensure JSON response
                }
                
                response = self.model.generate_content(
                    prompt,
                    generation_config=generation_config
                )
                response_text = response.text
                
                # Parse the structured response
                risk_assessment = self._parse_ai_response(
                    response_text, 
                    transaction.transaction_id,
                    start_time
                )
                
                # Annotate the output and metrics (if span exists)
                if span and DATADOG_ENABLED:
                    span.annotate(
                        input_data=prompt,
                        output_data=response_text,
                        metadata={
                            "transaction_id": transaction.transaction_id,
                            "user_id": transaction.user_id,
                            "amount": float(transaction.amount),
                            "risk_score": risk_assessment.risk_score,
                            "confidence": risk_assessment.confidence,
                            "processing_time_ms": risk_assessment.processing_time_ms,
                            "trace_id": trace_id
                        }
                    )
                
                return risk_assessment
                
            except gcp_exceptions.PermissionDenied as e:
                # Specific GCP permission error
                error_msg = f"GCP Permission Denied: {str(e)}"
                if span and DATADOG_ENABLED:
                    span.annotate(
                        input_data=prompt,
                        output_data=error_msg,
                        metadata={
                            "transaction_id": transaction.transaction_id,
                            "error_type": "permission_denied",
                            "error": error_msg,
                            "trace_id": trace_id
                        }
                    )
                return RiskAssessment(
                    transaction_id=transaction.transaction_id,
                    risk_score=50.0,  # Medium risk when auth fails
                    confidence=0.1,
                    risk_factors=["gcp_permission_error"],
                    model_used=self.model_name,
                    processing_time_ms=int((time.time() - start_time) * 1000),
                    recommendation="review"
                )
            except gcp_exceptions.GoogleAPIError as e:
                # Other GCP API errors
                error_msg = f"GCP API Error: {str(e)}"
                if span and DATADOG_ENABLED:
                    span.annotate(
                        input_data=prompt,
                        output_data=error_msg,
                        metadata={
                            "transaction_id": transaction.transaction_id,
                            "error_type": "gcp_api_error",
                            "error": error_msg,
                            "trace_id": trace_id
                        }
                    )
                return RiskAssessment(
                    transaction_id=transaction.transaction_id,
                    risk_score=50.0,  # Medium risk when API fails
                    confidence=0.1,
                    risk_factors=["gcp_api_error"],
                    model_used=self.model_name,
                    processing_time_ms=int((time.time() - start_time) * 1000),
                    recommendation="review"
                )
            except Exception as e:
                # Generic fallback for other errors
                error_msg = f"Unexpected error: {str(e)}"
                if span and DATADOG_ENABLED:
                    span.annotate(
                        input_data=prompt,
                        output_data=error_msg,
                        metadata={
                            "transaction_id": transaction.transaction_id,
                            "error_type": "unexpected_error",
                            "error": error_msg,
                            "trace_id": trace_id
                        }
                    )
                return RiskAssessment(
                    transaction_id=transaction.transaction_id,
                    risk_score=50.0,  # Medium risk when AI fails
                    confidence=0.1,
                    risk_factors=["ai_service_error"],
                    model_used=self.model_name,
                    processing_time_ms=int((time.time() - start_time) * 1000),
                    recommendation="review"
                )
    
    def _create_fraud_analysis_prompt(self, transaction: Transaction, user_context: UserContext) -> str:
        """Create structured prompt for fraud analysis"""
        
        # Convert transaction to dict for JSON serialization
        transaction_dict = {
            "transaction_id": transaction.transaction_id,
            "user_id": transaction.user_id,
            "amount": float(transaction.amount),
            "currency": transaction.currency,
            "merchant": transaction.merchant,
            "merchant_category": transaction.merchant_category,
            "location": {
                "country": transaction.location.country,
                "city": transaction.location.city,
                "is_new_location": transaction.location.is_new_location,
                "distance_from_home": transaction.location.distance_from_home
            },
            "timestamp": transaction.timestamp.isoformat(),
            "payment_method": transaction.payment_method
        }
        
        user_context_dict = {
            "user_id": user_context.user_id,
            "home_location": {
                "country": user_context.home_location.country,
                "city": user_context.home_location.city
            },
            "spending_patterns": user_context.spending_patterns,
            "recent_transactions": user_context.recent_transactions,
            "risk_profile": user_context.risk_profile
        }
        
        prompt = f"""
You are an expert fraud detection analyst. Analyze this credit card transaction for potential fraud.

TRANSACTION DATA:
{json.dumps(transaction_dict, indent=2)}

USER CONTEXT:
{json.dumps(user_context_dict, indent=2)}

ANALYSIS REQUIREMENTS:
1. Consider location anomalies (new countries, unusual distances)
2. Evaluate spending patterns (amount vs. history, merchant type)
3. Assess timing factors (time since last transaction, unusual hours)
4. Review user risk profile and recent activity

RESPONSE FORMAT (JSON only):
{{
    "risk_score": <0-100 integer>,
    "confidence": <0.0-1.0 float>,
    "risk_factors": ["factor1", "factor2", ...],
    "reasoning": "Brief explanation of the assessment",
    "recommendation": "approve|review|call_user"
}}

FRAUD INDICATORS:
- New country transactions (especially high-risk countries)
- Amounts significantly above user's typical spending
- Multiple transactions in short time periods
- Unusual merchant categories for the user
- Transactions far from home location without travel history

Respond with JSON only, no additional text.
"""
        return prompt
    
    def _parse_ai_response(self, response_text: str, transaction_id: str, start_time: float) -> RiskAssessment:
        """Parse AI response into structured RiskAssessment"""
        processing_time_ms = int((time.time() - start_time) * 1000)
        
        try:
            # Clean response text (remove markdown formatting if present)
            clean_response = response_text.strip()
            if clean_response.startswith("```json"):
                clean_response = clean_response[7:]
            if clean_response.endswith("```"):
                clean_response = clean_response[:-3]
            clean_response = clean_response.strip()
            
            # Parse JSON response
            ai_result = json.loads(clean_response)
            
            # Validate and extract fields
            risk_score = float(ai_result.get("risk_score", 50))
            confidence = float(ai_result.get("confidence", 0.5))
            risk_factors = ai_result.get("risk_factors", [])
            recommendation = ai_result.get("recommendation", "review")
            
            # Ensure risk_score is in valid range
            risk_score = max(0, min(100, risk_score))
            confidence = max(0, min(1, confidence))
            
            return RiskAssessment(
                transaction_id=transaction_id,
                risk_score=risk_score,
                confidence=confidence,
                risk_factors=risk_factors,
                model_used=self.model_name,
                processing_time_ms=processing_time_ms,
                recommendation=recommendation
            )
            
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            # Fallback parsing for malformed responses
            return RiskAssessment(
                transaction_id=transaction_id,
                risk_score=50.0,
                confidence=0.3,
                risk_factors=["parsing_error"],
                model_used=self.model_name,
                processing_time_ms=processing_time_ms,
                recommendation="review"
            )


# Demo function for immediate testing
def demo_fraud_detection():
    """Demo function to test fraud detection with hardcoded suspicious transaction"""
    
    # Create suspicious transaction (new country, high amount)
    suspicious_transaction = Transaction(
        transaction_id="txn_suspicious_001",
        user_id="user_12345",
        amount=Decimal("2500.00"),
        currency="USD",
        merchant="Luxury Electronics Store",
        merchant_category="electronics",
        location=Location(
            country="Romania",  # New country
            city="Bucharest",
            coordinates=(44.4268, 26.1025),
            is_new_location=True,
            distance_from_home=5000.0  # 5000 miles from home
        ),
        timestamp=datetime.now(),
        payment_method="credit_card",
        metadata={"ip_country": "Romania", "device_new": True}
    )
    
    # Create user context (US-based user)
    user_context = UserContext(
        user_id="user_12345",
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
        recent_transactions=[
            {"amount": 45.50, "merchant": "Grocery Store", "country": "United States"},
            {"amount": 12.99, "merchant": "Coffee Shop", "country": "United States"}
        ],
        risk_profile="low",
        phone_number="+1-555-0123"
    )
    
    # Analyze the transaction
    fraud_service = FraudDetectionService()
    result = fraud_service.analyze_transaction_realtime(suspicious_transaction, user_context)
    
    # Get trace ID for Datadog debugging
    trace_id = ddtrace.tracer.current_span().trace_id if ddtrace.tracer.current_span() else "No Trace Active"
    
    print("=== FRAUD DETECTION DEMO ===")
    print(f"🔍 Datadog Trace ID: {trace_id}")
    print(f"Transaction ID: {result.transaction_id}")
    print(f"Risk Score: {result.risk_score}/100")
    print(f"Confidence: {result.confidence:.2f}")
    print(f"Risk Factors: {', '.join(result.risk_factors)}")
    print(f"Recommendation: {result.recommendation}")
    print(f"Processing Time: {result.processing_time_ms}ms")
    print(f"Model Used: {result.model_used}")
    
    # Check if this should trigger a voice call (risk > 70%)
    if result.risk_score > 70:
        print("\n🚨 HIGH RISK DETECTED - VOICE CALL SHOULD BE INITIATED")
    else:
        print(f"\n✅ Risk level: {result.recommendation.upper()}")
    
    return result


if __name__ == "__main__":
    demo_fraud_detection()