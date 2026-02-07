"""
CRM Data Lake Configuration
"""
from pathlib import Path

# Base paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
QA_DIR = DATA_DIR / "qa"
PROD_DIR = DATA_DIR / "prod"

# API Endpoints
APIS = {
    "customers": {
        "name": "RandomUser API",
        "url": "https://randomuser.me/api/",
        "params": {"results": 50, "nat": "us,gb,ca"},
        "description": "Generates realistic fake customer profiles"
    },
    "interactions": {
        "name": "JSONPlaceholder API",
        "endpoints": {
            "posts": "https://jsonplaceholder.typicode.com/posts",
            "comments": "https://jsonplaceholder.typicode.com/comments",
            "users": "https://jsonplaceholder.typicode.com/users"
        },
        "description": "Provides posts, comments, and user interaction data"
    }
}

# Quality check thresholds
QA_THRESHOLDS = {
    "max_null_percentage": 5.0,
    "max_duplicate_percentage": 1.0,
    "required_fields": {
        "customers": ["id", "email", "first_name", "last_name"],
        "interactions": ["id", "user_id", "type", "content", "timestamp"]
    }
}
