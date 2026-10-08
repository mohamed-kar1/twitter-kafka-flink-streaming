# Twitter Kafka Flink Streaming

Pipeline de streaming de tweets : **Twitter/X API → Kafka → Flink (PyFlink) → Elasticsearch / HDFS**.

## Architecture

```
Twitter API v2 ──> Kafka (topic: my-topic-test) ──┬─> Flink SQL ──> Elasticsearch
                                                  ├─> Flink Word Count ──> Elasticsearch
                                                  └─> Consumer Python ──> HDFS
```

## Fichiers

| Fichier | Rôle |
|---|---|
| `twitter_kafka_producer.py` | Récupère des tweets via l'API Twitter v2 et les envoie dans Kafka |
| `twitter_kafka_consumer.py` | Consumer Kafka simple (affichage console) |
| `hdfs_consumer.py` | Consumer Kafka qui écrit les tweets dans HDFS |
| `kafka_flink_streaming.py` | Job PyFlink : lecture Kafka + affichage console (debug) |
| `kafka_flink_elasticsearch_streaming.py` | Job PyFlink : Kafka → Elasticsearch |
| `pyflink_word_count.py` | Job PyFlink : comptage de mots des tweets → Elasticsearch |

## Prérequis

- Python 3, Kafka (3 brokers : `localhost:9092`, `9093`, `9094`), Flink, Elasticsearch 7 (`localhost:9200`)
- Hadoop/HDFS (uniquement pour `hdfs_consumer.py`)
- Un compte développeur Twitter/X avec un Bearer Token

## Installation

```bash
git clone https://github.com/mohamed-kar1/twitter-kafka-flink-streaming.git
cd twitter-kafka-flink-streaming
pip install -r requirements.txt
```

Configuration des clés :

```bash
cp .env.example .env
# puis renseigne BEARER_TOKEN dans .env
```

## JARs Flink (non inclus dans le repo)

Télécharge les connecteurs et place-les dans le dossier `lib/` de ton installation Flink :

- [flink-sql-connector-kafka-3.1.0-1.18.jar](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar)
- [flink-sql-connector-elasticsearch7-3.0.1-1.17.jar](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7/3.0.1-1.17/flink-sql-connector-elasticsearch7-3.0.1-1.17.jar)

> Les scripts `kafka_flink_streaming.py` et `pyflink_word_count.py` contiennent des chemins JAR en dur (`file:///home/med/flink-1.13.6/lib/...`). Adapte-les à ton installation.

## Utilisation

Dans cet ordre, chaque commande dans un terminal :

```bash
python twitter_kafka_producer.py                   # envoie les tweets dans Kafka
python twitter_kafka_consumer.py                   # vérifie la réception
python kafka_flink_elasticsearch_streaming.py      # Kafka -> Elasticsearch
python pyflink_word_count.py                       # word count -> Elasticsearch
python hdfs_consumer.py                            # (optionnel) archivage HDFS
```

## Sécurité

Ne commite jamais ton fichier `.env` : il est ignoré par `.gitignore`.
