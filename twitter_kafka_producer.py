import tweepy
from json import dumps
from kafka import KafkaProducer
from rich import print
from time import sleep
from dotenv import load_dotenv
import os

load_dotenv()
BEARER_TOKEN = os.getenv("BEARER_TOKEN")  # Must have v2 access token

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
    value_serializer=lambda v: dumps(v).encode('utf-8')
)

# Create Twitter v2 client
client = tweepy.Client(bearer_token=BEARER_TOKEN)

search_query = 'music -is:retweet lang:en'  # exclude retweets and only English
tweet_count = 10  # number of tweets to fetch

# Fetch tweets in batches of 100 (max allowed by Twitter API)
tweets = client.search_recent_tweets(
    query=search_query,
    tweet_fields=['id','text','lang','public_metrics','entities','author_id','created_at'],
    expansions=['author_id'],
    user_fields=['name','location','public_metrics'],
    max_results=10
)

# Build a dictionary of users for easy lookup
users = {u.id: u for u in tweets.includes['users']}

for tweet in tweets.data:
    user = users.get(tweet.author_id)
    
    hashtags = []
    if tweet.entities and 'hashtags' in tweet.entities:
        hashtags = [ht['tag'] for ht in tweet.entities['hashtags']]

    cur_data = {
        "id_str": tweet.id,
        "username": user.name if user else None,
        "tweet": tweet.text,
        "location": user.location if user else None,
        "retweet_count": tweet.public_metrics['retweet_count'],
        "favorite_count": tweet.public_metrics['like_count'],
        "followers_count": user.public_metrics['followers_count'] if user else None,
        "lang": tweet.lang,
        "hashtags": hashtags
    }

    try:
        producer.send('my-topic-test', value=cur_data)
        producer.flush()
        print(cur_data)
    except Exception as e:
        print(f"[ERROR] Failed to send tweet: {e}")

    sleep(0.5)
