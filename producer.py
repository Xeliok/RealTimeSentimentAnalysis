import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

def json_serializer(data):
    """Sérialise les données en JSON et les encode en UTF-8 pour Kafka."""
    return json.dumps(data).encode('utf-8')

# Configuration du producteur Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=json_serializer
)

topic_name = 'tweets_stream'

# Liste de phrases factices pour simuler différents sentiments
sample_tweets = [
    "J'adore cette nouvelle fonctionnalité, c'est génial !",   # Positif
    "C'est la pire expérience que j'ai jamais eue...",       # Négatif
    "Le temps est nuageux aujourd'hui.",                     # Neutre
    "Je suis très content de mon achat.",                    # Positif
    "Service client misérable, je suis très déçu.",          # Négatif
    "Juste un message normal pour tester le système.",       # Neutre
    "Incroyable ! Je recommande vivement ce produit.",       # Positif
    "Rien à signaler de particulier aujourd'hui.",           # Neutre
    "Je déteste quand ça bugge comme ça.",                   # Négatif
    "Une journée fantastique commence !"                     # Positif
]

print(f"Démarrage du Data Producer. Envoi des messages vers le topic '{topic_name}'...")

try:
    while True:
        # Sélection aléatoire d'un tweet
        tweet_text = random.choice(sample_tweets)
        
        # Création du dictionnaire représentant le tweet
        tweet_data = {
            "text": tweet_text,
            "timestamp": datetime.now().isoformat()
        }
        
        # Envoi vers le topic Kafka
        producer.send(topic_name, value=tweet_data)
        print(f"Message envoyé : {tweet_data}")
        
        # Pause de 1 seconde pour simuler un flux continu mais lisible
        time.sleep(1)
except KeyboardInterrupt:
    print("\nArrêt du producer demandé par l'utilisateur.")
finally:
    # Fermeture propre du producer
    producer.close()
