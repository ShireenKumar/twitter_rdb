'''
Contributions: Arshia created this file.
'''
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME', 'twitter_db'),
    'port': int(os.getenv('DB_PORT', 3306))
}

# Redis config
REDIS_CONFIG = {
    'host': os.getenv('REDIS_HOST', 'localhost'),
    'port': int(os.getenv('REDIS_PORT', 6379)),
    'db': int(os.getenv('REDIS_DB', 0)),
    'decode_responses': True
}

# Performance test settings
TWEET_TARGET_COUNT = 1_000_000  # Test with 1 million tweets
TIMELINE_TEST_DURATION = 60      # Test timelines for 60 seconds