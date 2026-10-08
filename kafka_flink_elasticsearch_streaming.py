# -*- coding: utf-8 -*-
from pyflink.table import TableEnvironment, EnvironmentSettings

# 1. Initialisation de l'environnement de table
# Le mode streaming et le Blink Planner sont configurés ici de manière standard
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = TableEnvironment.create(env_settings)

# 2. Définition de la table Source (Kafka)
# Lit les tweets en JSON depuis les 3 brokers Kafka
source_ddl = """
CREATE TABLE source_table(
    id_str VARCHAR,
    username VARCHAR,
    tweet VARCHAR,
    location VARCHAR,
    retweet_count BIGINT,
    favorite_count BIGINT,
    followers_count BIGINT,
    lang VARCHAR,
    hashtags STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'my-topic-test',
    'properties.bootstrap.servers' = 'localhost:9092,localhost:9093,localhost:9094',
    'properties.group.id' = 'flink_twitter_group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
)
"""
t_env.execute_sql(source_ddl)

# 3. Définition de la table Destination (Elasticsearch)
# Envoie les données vers l'index Elasticsearch 7
sink_ddl = """
CREATE TABLE sink_table(
    id_str VARCHAR,
    username VARCHAR,
    tweet VARCHAR,
    location VARCHAR,
    retweet_count BIGINT,
    favorite_count BIGINT,
    followers_count BIGINT,
    lang VARCHAR,
    hashtags STRING,
    PRIMARY KEY (id_str) NOT ENFORCED
) WITH (
    'connector' = 'elasticsearch-7',
    'hosts' = 'http://localhost:9200',
    'index' = 'demo_kafka_flink_streaming',
    'format' = 'json'
)
"""
t_env.execute_sql(sink_ddl)

# 4. Requête SQL de transfert
# On sélectionne toutes les colonnes de la source pour les injecter dans le sink
result_table = t_env.sql_query("SELECT * FROM source_table")

print("--- [INFO] Initialisation terminée. Envoi du Job au Cluster Flink... ---")

# 5. Lancement de l'exécution
# execute_insert lance le job sur le cluster défini dans le terminal
result_table.execute_insert('sink_table').wait()