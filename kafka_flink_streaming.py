from pyflink.table import TableEnvironment, EnvironmentSettings

# 1. Création de l'environnement
env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

# 2. Configuration du connecteur Kafka
t_env.get_config().get_configuration().set_string(
    "pipeline.jars",
    "file:///home/med/flink-1.13.6/lib/flink-sql-connector-kafka_2.12-1.13.6.jar"
)
    

# 3. Table Source pointant vers le topic partagé
source_ddl = """
CREATE TABLE twitter_source (
    id_str STRING,
    username STRING,
    tweet STRING,
    location STRING,
    retweet_count BIGINT,
    favorite_count BIGINT,
    followers_count BIGINT,
    lang STRING,
    hashtags ARRAY<STRING>
) WITH (
    'connector' = 'kafka',
    'topic' = 'my-topic-test',
    'properties.bootstrap.servers' = 'localhost:9092,localhost:9093,localhost:9094',
    'properties.group.id' = 'flink_twitter_debug_group',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
)
"""
t_env.execute_sql(source_ddl)

# 4. Requête SQL simple
result_table = t_env.sql_query("""
    SELECT
        username,
        tweet,
        lang,
        retweet_count,
        followers_count
    FROM twitter_source
""")

# 5. Affichage direct dans la console (pour débogage)
result_table.execute().print()