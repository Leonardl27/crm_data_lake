"""
Life Insurance Data Generator using Faker
Generates realistic life insurance journey data: Customer -> Quote -> Application -> Policy -> Claims
"""
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

from faker import Faker

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config.settings import QA_DIR, LIFE_INSURANCE_CONFIG

fake = Faker()
Faker.seed(42)
random.seed(42)


class LifeInsuranceGenerator:
    """Generates realistic life insurance data with proper relationships"""

    def __init__(self, num_customers: int = 100):
        self.num_customers = num_customers
        self.config = LIFE_INSURANCE_CONFIG
        self.customers = []
        self.agents = []
        self.quotes = []
        self.applications = []
        self.policies = []
        self.claims = []

    def generate_all(self) -> dict:
        """Generate all entities in dependency order"""
        print("Generating life insurance data...")
        print(f"  - Generating 20 agents...")
        self.generate_agents(20)
        print(f"  - Generating {self.num_customers} customers...")
        self.generate_customers()
        print(f"  - Generating quotes...")
        self.generate_quotes()
        print(f"  - Generating applications (~30% conversion)...")
        self.generate_applications()
        print(f"  - Generating policies (~70% of approved)...")
        self.generate_policies()
        print(f"  - Generating claims (~2% annual rate)...")
        self.generate_claims()

        print(f"\nGeneration complete:")
        print(f"  - agents: {len(self.agents)} records")
        print(f"  - customers: {len(self.customers)} records")
        print(f"  - quotes: {len(self.quotes)} records")
        print(f"  - applications: {len(self.applications)} records")
        print(f"  - policies: {len(self.policies)} records")
        print(f"  - claims: {len(self.claims)} records")

        return {
            "agents": self.agents,
            "customers": self.customers,
            "quotes": self.quotes,
            "applications": self.applications,
            "policies": self.policies,
            "claims": self.claims,
        }

    def generate_agents(self, num_agents: int = 20) -> list[dict]:
        """Generate insurance agents"""
        agents = []
        for i in range(num_agents):
            agent = {
                "Agent_ID__c": f"AGT-{i+1:05d}",
                "Name": fake.name(),
                "Email": fake.company_email(),
                "Phone": fake.phone_number(),
                "License_Number__c": f"INS{fake.random_number(digits=8, fix_len=True)}",
                "Region__c": fake.state(),
                "Years_Experience__c": random.randint(1, 30),
                "Specialization__c": random.choice(self.config["product_types"]),
                "Active__c": random.random() > 0.1,
            }
            agents.append(agent)
        self.agents = agents
        return agents

    def generate_customers(self) -> list[dict]:
        """Generate customer records"""
        customers = []
        for i in range(self.num_customers):
            dob = fake.date_of_birth(minimum_age=18, maximum_age=75)
            age = (datetime.now().date() - dob).days // 365

            customer = {
                "Customer_ID__c": f"CUST-{i+1:05d}",
                "First_Name__c": fake.first_name(),
                "Last_Name__c": fake.last_name(),
                "Email__c": fake.email(),
                "Phone__c": fake.phone_number(),
                "Date_of_Birth__c": dob.isoformat(),
                "Age__c": age,
                "Gender__c": random.choice(["Male", "Female"]),
                "Address__c": fake.street_address(),
                "City__c": fake.city(),
                "State__c": fake.state_abbr(),
                "Zip_Code__c": fake.zipcode(),
                "Smoker__c": random.random() < 0.25,
                "Annual_Income__c": random.randint(30000, 500000),
                "Employment_Status__c": random.choice(
                    ["Employed", "Self-Employed", "Retired", "Unemployed"]
                ),
                "Occupation__c": fake.job(),
                "Created_Date__c": fake.date_between(
                    start_date="-2y", end_date="today"
                ).isoformat(),
            }
            customers.append(customer)
        self.customers = customers
        return customers

    def generate_quotes(self) -> list[dict]:
        """Generate quotes - each customer gets 1-3 quotes"""
        quotes = []
        quote_counter = 0

        for customer in self.customers:
            num_quotes = random.choices([1, 2, 3], weights=[60, 30, 10])[0]

            for _ in range(num_quotes):
                quote_counter += 1
                product_type = random.choice(self.config["product_types"])

                coverage_amount = random.choice(
                    [25000, 50000, 100000, 250000, 500000, 750000, 1000000, 1500000, 2000000]
                )

                age = customer["Age__c"]
                smoker = customer["Smoker__c"]
                base_rate = coverage_amount * 0.001
                age_factor = 1 + (age - 30) * 0.02 if age > 30 else 1
                smoker_factor = 1.5 if smoker else 1
                product_factors = {
                    "Term Life": 0.8,
                    "Whole Life": 1.5,
                    "Universal Life": 1.3,
                    "Variable Life": 1.4,
                    "Final Expense": 2.0,
                }
                premium = (
                    base_rate
                    * age_factor
                    * smoker_factor
                    * product_factors.get(product_type, 1)
                )

                customer_created = datetime.fromisoformat(customer["Created_Date__c"]).date()
                quote_date = fake.date_between(start_date=customer_created, end_date="today")
                expiry_date = quote_date + timedelta(days=30)

                agent = random.choice(self.agents)

                quote = {
                    "Quote_ID__c": f"QUO-{quote_counter:06d}",
                    "Customer_ID__c": customer["Customer_ID__c"],
                    "Agent_ID__c": agent["Agent_ID__c"],
                    "Product_Type__c": product_type,
                    "Coverage_Amount__c": coverage_amount,
                    "Premium_Monthly__c": round(premium, 2),
                    "Term_Years__c": (
                        random.choice(self.config["term_years_options"])
                        if product_type == "Term Life"
                        else None
                    ),
                    "Status__c": random.choice(self.config["quote_statuses"]),
                    "Created_Date__c": quote_date.isoformat(),
                    "Expiry_Date__c": expiry_date.isoformat(),
                    "Risk_Category__c": random.choice(["Low", "Medium", "High"]),
                    "Source__c": random.choice(["Web", "Phone", "Agent", "Referral"]),
                }
                quotes.append(quote)

        self.quotes = quotes
        return quotes

    def generate_applications(self) -> list[dict]:
        """Generate applications - ~30% of quotes convert to applications"""
        applications = []
        conversion_rate = self.config["conversion_rates"]["quote_to_application"]

        converted_quotes = [q for q in self.quotes if q["Status__c"] == "Converted"]
        additional_needed = int(len(self.quotes) * conversion_rate) - len(converted_quotes)

        if additional_needed > 0:
            non_converted = [q for q in self.quotes if q["Status__c"] != "Converted"]
            additional = random.sample(
                non_converted, min(additional_needed, len(non_converted))
            )
            converted_quotes.extend(additional)

        for idx, quote in enumerate(converted_quotes):
            quote_date = datetime.fromisoformat(quote["Created_Date__c"]).date()
            app_date = quote_date + timedelta(days=random.randint(1, 14))
            if app_date > datetime.now().date():
                app_date = datetime.now().date()

            underwriting_status = random.choices(
                self.config["underwriting_statuses"], weights=[15, 15, 50, 15, 5]
            )[0]

            health_class = random.choices(
                self.config["health_classes"], weights=[10, 25, 30, 25, 10]
            )[0]

            decision_date = None
            if underwriting_status in ["Approved", "Declined"]:
                decision_date = app_date + timedelta(days=random.randint(7, 30))
                if decision_date > datetime.now().date():
                    decision_date = datetime.now().date()

            application = {
                "Application_ID__c": f"APP-{idx+1:06d}",
                "Quote_ID__c": quote["Quote_ID__c"],
                "Customer_ID__c": quote["Customer_ID__c"],
                "Agent_ID__c": quote["Agent_ID__c"],
                "Product_Type__c": quote["Product_Type__c"],
                "Coverage_Amount__c": quote["Coverage_Amount__c"],
                "Premium_Monthly__c": quote["Premium_Monthly__c"],
                "Application_Date__c": app_date.isoformat(),
                "Underwriting_Status__c": underwriting_status,
                "Health_Class__c": health_class,
                "Risk_Score__c": random.randint(1, 100),
                "Medical_Exam_Required__c": random.random() > 0.3,
                "Medical_Exam_Date__c": (
                    (app_date + timedelta(days=random.randint(3, 14))).isoformat()
                    if random.random() > 0.3
                    else None
                ),
                "Decision_Date__c": decision_date.isoformat() if decision_date else None,
                "Notes__c": fake.paragraph() if random.random() > 0.7 else None,
            }
            applications.append(application)

        self.applications = applications
        return applications

    def generate_policies(self) -> list[dict]:
        """Generate policies - ~70% of approved applications become policies"""
        policies = []
        conversion_rate = self.config["conversion_rates"]["application_to_policy"]

        approved_apps = [
            a for a in self.applications if a["Underwriting_Status__c"] == "Approved"
        ]
        num_to_convert = int(len(approved_apps) * conversion_rate)
        apps_to_convert = random.sample(
            approved_apps, min(num_to_convert, len(approved_apps))
        )

        for app in apps_to_convert:
            quote = next(
                (q for q in self.quotes if q["Quote_ID__c"] == app["Quote_ID__c"]), None
            )
            if not quote:
                continue

            decision_date = datetime.fromisoformat(app["Decision_Date__c"]).date()
            effective_date = decision_date + timedelta(days=random.randint(1, 14))
            if effective_date > datetime.now().date():
                effective_date = datetime.now().date()

            if quote["Term_Years__c"]:
                expiry_date = effective_date + timedelta(
                    days=365 * quote["Term_Years__c"]
                )
            else:
                expiry_date = effective_date + timedelta(days=365 * 99)

            payment_freq = random.choice(self.config["payment_frequencies"])
            premium_multipliers = {
                "Monthly": 1,
                "Quarterly": 3,
                "Semi-Annual": 6,
                "Annual": 12,
            }

            policy_age_days = (datetime.now().date() - effective_date).days
            if policy_age_days < 90:
                status = "Active"
            else:
                status = random.choices(
                    self.config["policy_statuses"], weights=[70, 10, 5, 10, 5]
                )[0]

            beneficiary_relationships = [
                "Spouse",
                "Child",
                "Parent",
                "Sibling",
                "Other",
            ]

            policy = {
                "Policy_ID__c": f"POL-{len(policies)+1:06d}",
                "Application_ID__c": app["Application_ID__c"],
                "Customer_ID__c": app["Customer_ID__c"],
                "Policy_Number__c": f"LI{fake.random_number(digits=10, fix_len=True)}",
                "Product_Type__c": quote["Product_Type__c"],
                "Effective_Date__c": effective_date.isoformat(),
                "Expiry_Date__c": expiry_date.isoformat(),
                "Coverage_Amount__c": quote["Coverage_Amount__c"],
                "Premium_Amount__c": round(
                    quote["Premium_Monthly__c"] * premium_multipliers[payment_freq], 2
                ),
                "Payment_Frequency__c": payment_freq,
                "Beneficiary_Name__c": fake.name(),
                "Beneficiary_Relationship__c": random.choice(beneficiary_relationships),
                "Status__c": status,
                "Cash_Value__c": (
                    self._calculate_cash_value(
                        quote["Product_Type__c"],
                        quote["Coverage_Amount__c"],
                        policy_age_days,
                    )
                    if "Whole" in quote["Product_Type__c"]
                    or "Universal" in quote["Product_Type__c"]
                    else 0
                ),
                "Last_Payment_Date__c": (
                    (
                        datetime.now().date() - timedelta(days=random.randint(1, 30))
                    ).isoformat()
                    if status == "Active"
                    else None
                ),
            }
            policies.append(policy)

        self.policies = policies
        return policies

    def generate_claims(self) -> list[dict]:
        """Generate claims - ~2% of policies have claims annually"""
        claims = []
        claim_rate = self.config["conversion_rates"]["policy_to_claim_annual"]

        eligible_policies = []
        for policy in self.policies:
            if policy["Status__c"] in ["Active", "Paid Up"]:
                policy_age_years = (
                    datetime.now().date()
                    - datetime.fromisoformat(policy["Effective_Date__c"]).date()
                ).days / 365
                if random.random() < (claim_rate * max(1, policy_age_years)):
                    eligible_policies.append(policy)

        for policy in eligible_policies:
            effective_date = datetime.fromisoformat(policy["Effective_Date__c"]).date()
            filed_date = fake.date_between(start_date=effective_date, end_date="today")

            claim_type = random.choices(
                self.config["claim_types"], weights=[60, 20, 15, 5]
            )[0]

            if claim_type == "Death Benefit":
                claim_amount = policy["Coverage_Amount__c"]
            elif claim_type == "Accelerated Death Benefit":
                claim_amount = policy["Coverage_Amount__c"] * random.uniform(0.25, 0.75)
            elif claim_type == "Terminal Illness":
                claim_amount = policy["Coverage_Amount__c"] * random.uniform(0.50, 0.90)
            else:
                claim_amount = policy["Coverage_Amount__c"] * random.uniform(1.0, 2.0)

            processing_days = random.randint(15, 90)
            processed_date = filed_date + timedelta(days=processing_days)

            if processed_date > datetime.now().date():
                processed_date = None
                status = random.choice(["Filed", "Under Review"])
                payout_amount = None
            else:
                status = random.choices(
                    ["Approved", "Paid", "Denied", "Closed"], weights=[20, 50, 10, 20]
                )[0]
                if status == "Denied":
                    payout_amount = 0
                else:
                    payout_amount = round(claim_amount * random.uniform(0.95, 1.0), 2)

            denial_reasons = [
                "Policy lapsed",
                "Exclusion period",
                "Fraud suspected",
                "Documentation incomplete",
            ]

            claim = {
                "Claim_ID__c": f"CLM-{len(claims)+1:06d}",
                "Policy_ID__c": policy["Policy_ID__c"],
                "Customer_ID__c": policy["Customer_ID__c"],
                "Policy_Number__c": policy["Policy_Number__c"],
                "Claim_Type__c": claim_type,
                "Claim_Amount__c": round(claim_amount, 2),
                "Filed_Date__c": filed_date.isoformat(),
                "Status__c": status,
                "Processed_Date__c": (
                    processed_date.isoformat() if processed_date else None
                ),
                "Payout_Amount__c": payout_amount,
                "Denial_Reason__c": (
                    random.choice(denial_reasons) if status == "Denied" else None
                ),
                "Adjuster_ID__c": f"ADJ-{random.randint(1, 50):05d}",
                "Notes__c": fake.paragraph() if random.random() > 0.7 else None,
            }
            claims.append(claim)

        self.claims = claims
        return claims

    def _calculate_cash_value(
        self, product_type: str, coverage: float, days_active: int
    ) -> float:
        """Calculate cash value for permanent life policies"""
        years = days_active / 365
        if years < 1:
            return 0
        growth_rate = 0.03 if "Whole" in product_type else 0.04
        base_value = coverage * 0.02 * years
        return round(base_value * (1 + growth_rate) ** years, 2)


def save_to_qa(data: list[dict], entity_name: str) -> Path:
    """Save generated data to QA layer"""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_dir = QA_DIR / entity_name
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"{entity_name}_{timestamp}.json"

    output_data = {
        "metadata": {
            "source": "Life Insurance Data Generator (Faker)",
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "record_count": len(data),
            "layer": "QA",
        },
        "data": data,
    }

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2)

    return output_file


def run_extraction(num_customers: int = 100) -> dict[str, Path]:
    """Run the full extraction process"""
    generator = LifeInsuranceGenerator(num_customers)
    all_data = generator.generate_all()

    output_files = {}
    for entity_name, data in all_data.items():
        output_file = save_to_qa(data, entity_name)
        output_files[entity_name] = output_file
        print(f"Saved {entity_name} to: {output_file}")

    return output_files


if __name__ == "__main__":
    run_extraction(100)
