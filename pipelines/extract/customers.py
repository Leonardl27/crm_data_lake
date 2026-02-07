"""
Customer Data Extraction Pipeline
Fetches customer data from RandomUser API
"""
import json
import requests
from datetime import datetime
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config.settings import APIS, QA_DIR


def extract_customers(num_customers: int = 50) -> list[dict]:
    """
    Extract customer data from RandomUser API

    Args:
        num_customers: Number of customers to generate

    Returns:
        List of customer dictionaries
    """
    api_config = APIS["customers"]
    params = {**api_config["params"], "results": num_customers}

    print(f"Fetching {num_customers} customers from {api_config['name']}...")

    response = requests.get(api_config["url"], params=params)
    response.raise_for_status()

    raw_data = response.json()
    customers = []

    for idx, user in enumerate(raw_data.get("results", []), start=1):
        customer = {
            "id": f"CUST-{idx:05d}",
            "email": user["email"],
            "first_name": user["name"]["first"],
            "last_name": user["name"]["last"],
            "gender": user["gender"],
            "phone": user["phone"],
            "cell": user["cell"],
            "address": {
                "street": f"{user['location']['street']['number']} {user['location']['street']['name']}",
                "city": user["location"]["city"],
                "state": user["location"]["state"],
                "country": user["location"]["country"],
                "postcode": str(user["location"]["postcode"]),
            },
            "date_of_birth": user["dob"]["date"],
            "age": user["dob"]["age"],
            "registered_date": user["registered"]["date"],
            "picture_url": user["picture"]["medium"],
            "nationality": user["nat"],
            "extracted_at": datetime.utcnow().isoformat()
        }
        customers.append(customer)

    print(f"Successfully extracted {len(customers)} customers")
    return customers


def save_to_qa(customers: list[dict]) -> Path:
    """
    Save extracted customers to QA layer

    Args:
        customers: List of customer dictionaries

    Returns:
        Path to saved file
    """
    output_dir = QA_DIR / "customers"
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"customers_{timestamp}.json"

    data = {
        "metadata": {
            "source": APIS["customers"]["name"],
            "extracted_at": datetime.utcnow().isoformat(),
            "record_count": len(customers),
            "layer": "QA"
        },
        "data": customers
    }

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    print(f"Saved to QA layer: {output_file}")
    return output_file


def run_extraction(num_customers: int = 50) -> Path:
    """Run the full extraction pipeline"""
    customers = extract_customers(num_customers)
    return save_to_qa(customers)


if __name__ == "__main__":
    run_extraction()
