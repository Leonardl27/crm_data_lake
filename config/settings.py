"""
Life Insurance CRM Data Lake Configuration
"""
from pathlib import Path

# Base paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
QA_DIR = DATA_DIR / "qa"
PROD_DIR = DATA_DIR / "prod"

# Life Insurance Domain Configuration
LIFE_INSURANCE_CONFIG = {
    "product_types": [
        "Term Life",
        "Whole Life",
        "Universal Life",
        "Variable Life",
        "Final Expense"
    ],
    "health_classes": [
        "Preferred Plus",
        "Preferred",
        "Standard Plus",
        "Standard",
        "Substandard"
    ],
    "underwriting_statuses": [
        "Pending",
        "In Review",
        "Approved",
        "Declined",
        "Referred"
    ],
    "claim_types": [
        "Death Benefit",
        "Accelerated Death Benefit",
        "Terminal Illness",
        "Accidental Death"
    ],
    "policy_statuses": [
        "Active",
        "Lapsed",
        "Surrendered",
        "Paid Up",
        "Terminated"
    ],
    "quote_statuses": [
        "Draft",
        "Sent",
        "Viewed",
        "Expired",
        "Converted"
    ],
    "claim_statuses": [
        "Filed",
        "Under Review",
        "Approved",
        "Denied",
        "Paid",
        "Closed"
    ],
    "payment_frequencies": [
        "Monthly",
        "Quarterly",
        "Semi-Annual",
        "Annual"
    ],
    "coverage_ranges": {
        "min": 25000,
        "max": 2000000
    },
    "term_years_options": [10, 15, 20, 25, 30],
    "conversion_rates": {
        "quote_to_application": 0.30,
        "application_to_policy": 0.70,
        "policy_to_claim_annual": 0.02
    }
}

# Life Insurance Entities (in dependency order for promotion)
LIFE_INSURANCE_ENTITIES = ["customers", "agents", "quotes", "applications", "policies", "claims"]

# Quality check thresholds
QA_THRESHOLDS = {
    "max_null_percentage": 5.0,
    "max_duplicate_percentage": 1.0,
    "required_fields": {
        "customers": ["Customer_ID__c", "Email__c", "First_Name__c", "Last_Name__c"],
        "agents": ["Agent_ID__c", "Name", "Email"],
        "quotes": ["Quote_ID__c", "Customer_ID__c", "Product_Type__c", "Coverage_Amount__c", "Status__c"],
        "applications": ["Application_ID__c", "Quote_ID__c", "Customer_ID__c", "Underwriting_Status__c"],
        "policies": ["Policy_ID__c", "Application_ID__c", "Customer_ID__c", "Policy_Number__c", "Status__c"],
        "claims": ["Claim_ID__c", "Policy_ID__c", "Customer_ID__c", "Claim_Type__c", "Status__c"]
    }
}
