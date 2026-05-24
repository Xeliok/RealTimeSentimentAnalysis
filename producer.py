import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

def json_serializer(data):
    """Serialize the data into JSON and encode it in UTF-8 for Kafka."""
    return json.dumps(data).encode('utf-8')

# Kafka Producer Configuration
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=json_serializer
)

topic_name = 'tweets_stream'

# English sample tweets list for VADER sentiment analysis
sample_tweets = [
    "I love this new feature, it is absolutely brilliant!",    # Positive
    "This is the worst experience I have ever had...",         # Negative
    "The weather is quite cloudy and gray today.",             # Neutral
    "I am incredibly happy and satisfied with my purchase.",   # Positive
    "Miserable customer service, I am deeply disappointed.",   # Negative
    "Just a standard automated message to test the system.",  # Neutral
    "Amazing product! I highly recommend it to everyone.",     # Positive
    "Nothing special to report about my day today.",           # Neutral
    "I absolutely hate it when the application bugs like this.",# Negative
    "A fantastic and beautiful day is starting right now!"     # Positive
]

print(f"Starting Data Producer. Sending messages to topic '{topic_name}'...")

try:
    while True:
        # Select a random tweet
        tweet_text = random.choice(sample_tweets)
        
        # Create the tweet data dictionary
        tweet_data = {
            "text": tweet_text,
            "timestamp": datetime.now().isoformat()
        }
        
        # Send data to Kafka topic
        producer.send(topic_name, value=tweet_data)
        print(f"Message sent: {tweet_data}")
        
        # 1-second pause to simulate a continuous but readable stream
        time.sleep(1)
except KeyboardInterrupt:
    print("\nProducer stopped by user.")
finally:
    # Clean closure of the producer
    producer.close()
