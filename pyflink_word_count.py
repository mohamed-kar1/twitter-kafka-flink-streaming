from pyflink.table import TableEnvironment, EnvironmentSettings, DataTypes
from pyflink.table.expressions import col
from pyflink.table.udf import udf, udtf, ScalarFunction
import string, re, nltk

def word_count_stream_processing():
    # 1. Environnement
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = TableEnvironment.create(env_settings)

    # 2. Configuration des JARs et Python
    config = t_env.get_config().get_configuration()
    jars = [
        "file:///home/med/flink-1.13.6/lib/flink-table-planner-blink_2.12-1.13.6.jar",
        "file:///home/med/flink-1.13.6/lib/flink-sql-connector-kafka_2.12-1.13.6.jar",
        "file:///home/med/flink-1.13.6/lib/flink-sql-connector-elasticsearch7_2.12-1.13.6.jar"
    ]
    config.set_string("pipeline.jars", ";".join(jars))
    
    # Correction du chemin Python
    t_env.get_config().set_python_executable("/usr/bin/python3")

    # 3. Tables DDL
    source_ddl = """
        CREATE TABLE source_table(tweet STRING) WITH (
            'connector' = 'kafka',
            'topic' = 'my-topic-test',
            'properties.bootstrap.servers' = 'localhost:9092,localhost:9093,localhost:9094',
            'properties.group.id' = 'flink_wordcount_group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )"""
    
    sink_ddl = """
        CREATE TABLE sink_table(word VARCHAR, number BIGINT, PRIMARY KEY (word) NOT ENFORCED) WITH (
            'connector' = 'elasticsearch-7',
            'index' = 'twitter_wordcount',
            'hosts' = 'http://localhost:9200',
            'format' = 'json',
            'sink.bulk-flush.max-actions' = '1'
        )"""

    t_env.execute_sql(source_ddl)
    t_env.execute_sql(sink_ddl)

    # 4. UDFs (Logique de nettoyage)
    @udf(result_type=DataTypes.STRING())
    def preprocess_text(text):
        if text is None: return ""
        text = re.sub('((www\.[^\s]+)|(https?://[^\s]+))', 'URL', text)
        text = re.sub('@[^\s]+', 'USER', text)
        text = re.sub('[^a-zA-Z]+', ' ', text)
        return text.lower().strip()

    @udtf(result_types=[DataTypes.STRING()])
    def split(line):
        if line:
            for s in line.split():
                if len(s) > 2: yield s

    # 5. Pipeline
    source_table = t_env.from_path('source_table')
    result = source_table \
        .select(preprocess_text(col('tweet')).alias('line')) \
        .flat_map(split).alias('word') \
        .group_by(col('word')) \
        .select(col('word'), col('word').count.alias('number'))

    print("Démarrage Word Count...")
    result.execute_insert('sink_table').wait()

if __name__ == '__main__':
    word_count_stream_processing()