"""
Interactions Data Extraction Pipeline
Fetches interaction data from JSONPlaceholder API
"""
import json
import requests
from datetime import datetime, timedelta
from pathlib import Path
import random
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config.settings import APIS, QA_DIR


def extract_interactions() -> list[dict]:
    """
    Extract interaction data from JSONPlaceholder API
    Combines posts and comments into unified interaction records

    Returns:
        List of interaction dictionaries
    """
    api_config = APIS["interactions"]
    endpoints = api_config["endpoints"]
    interactions = []

    # Fetch posts
    print(f"Fetching posts from {api_config['name']}...")
    posts_response = requests.get(endpoints["posts"])
    posts_response.raise_for_status()
    posts = posts_response.json()

    # Fetch comments
    print(f"Fetching comments from {api_config['name']}...")
    comments_response = requests.get(endpoints["comments"])
    comments_response.raise_for_status()
    comments = comments_response.json()

    # Transform posts into interactions
    base_date = datetime.utcnow() - timedelta(days=90)

    for post in posts:
        timestamp = base_date + timedelta(
            days=random.randint(0, 90),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        interaction = {
            "id": f"INT-{post['id']:05d}",
            "user_id": f"CUST-{post['userId']:05d}",
            "type": "post",
            "title": post["title"],
            "content": post["body"],
            "timestamp": timestamp.isoformat(),
            "sentiment": random.choice(["positive", "neutral", "negative"]),
            "channel": random.choice(["web", "mobile", "email"]),
            "extracted_at": datetime.utcnow().isoformat()
        }
        interactions.append(interaction)

    # Transform comments into interactions
    for comment in comments:
        timestamp = base_date + timedelta(
            days=random.randint(0, 90),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        interaction = {
            "id": f"INT-{100 + comment['id']:05d}",
            "user_id": f"CUST-{comment['postId']:05d}",  # Link to post author
            "type": "comment",
            "title": comment["name"],
            "content": comment["body"],
            "email": comment["email"],
            "parent_id": f"INT-{comment['postId']:05d}",
            "timestamp": timestamp.isoformat(),
            "sentiment": random.choice(["positive", "neutral", "negative"]),
            "channel": random.choice(["web", "mobile", "email"]),
            "extracted_at": datetime.utcnow().isoformat()
        }
        interactions.append(interaction)

    print(f"Successfully extracted {len(interactions)} interactions")
    return interactions


def save_to_qa(interactions: list[dict]) -> Path:
    """
    Save extracted interactions to QA layer

    Args:
        interactions: List of interaction dictionaries

    Returns:
        Path to saved file
    """
    output_dir = QA_DIR / "interactions"
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"interactions_{timestamp}.json"

    # Calculate summary stats
    type_counts = {}
    sentiment_counts = {}
    channel_counts = {}

    for interaction in interactions:
        type_counts[interaction["type"]] = type_counts.get(interaction["type"], 0) + 1
        sentiment_counts[interaction["sentiment"]] = sentiment_counts.get(interaction["sentiment"], 0) + 1
        channel_counts[interaction["channel"]] = channel_counts.get(interaction["channel"], 0) + 1

    data = {
        "metadata": {
            "source": APIS["interactions"]["name"],
            "extracted_at": datetime.utcnow().isoformat(),
            "record_count": len(interactions),
            "layer": "QA",
            "summary": {
                "by_type": type_counts,
                "by_sentiment": sentiment_counts,
                "by_channel": channel_counts
            }
        },
        "data": interactions
    }

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    print(f"Saved to QA layer: {output_file}")
    return output_file


def run_extraction() -> Path:
    """Run the full extraction pipeline"""
    interactions = extract_interactions()
    return save_to_qa(interactions)


if __name__ == "__main__":
    run_extraction()
