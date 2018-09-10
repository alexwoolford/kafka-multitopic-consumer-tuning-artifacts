from streamsets.sdk import DataCollector
import requests
import json
import time
import uuid
import mysql.connector
from fabric import Connection


def run_benchmark(max_batch_size, replication_factor, partitions, threads, seconds):

    datacollector_url = 'http://sdc.woolford.io:18630'
    data_collector = DataCollector(datacollector_url, username="admin", password="admin")

    builder = data_collector.get_pipeline_builder()

    kafka_consumer = builder.add_stage("Kafka Multitopic Consumer")
    kafka_consumer.library = "streamsets-datacollector-cdh_kafka_3_0-lib"
    kafka_consumer.data_format = "JSON"
    kafka_consumer.broker_uri = "cdh03.woolford.io:9092,cdh04.woolford.io:9092,cdh05.woolford.io:9092,cdh06.woolford.io:9092"
    kafka_consumer.topic_list = ["kafka-benchmark-replication-factor-{0}-partitions-{1}".format(replication_factor, partitions)]
    kafka_consumer.number_of_threads = threads
    kafka_consumer.configuration_properties = [{"key": "auto.offset.reset", "value": "earliest"}]
    kafka_consumer.consumer_group = str(uuid.uuid4())
    kafka_consumer.max_batch_size_in_records = max_batch_size

    trash = builder.add_stage('Trash')

    kafka_consumer >> trash  # connect the Dev Data Generator origin to the Trash destination.

    pipeline = builder.build('kafka-trash')
    data_collector.add_pipeline(pipeline)

    data_collector.start_pipeline(pipeline)

    time.sleep(seconds)

    # get message count
    metrics_url = "{0}/rest/v1/pipeline/{1}/metrics".format(datacollector_url, pipeline.id)
    headers = {'Content-Type': 'application/json', 'X-Requested-By': 'sdc'}
    auth = ('admin', 'admin')
    r = requests.get(metrics_url, headers=headers, auth=auth)
    metrics = json.loads(r.content)
    message_count = metrics['counters']['stage.KafkaMultitopicConsumer_01.outputRecords.counter']['count']

    data_collector.stop_pipeline(pipeline)

    persist_metric(max_batch_size=max_batch_size, replication_factor=replication_factor, partitions=partitions, threads=threads, seconds=seconds, message_count=message_count)

    Connection('sdc').run('systemctl restart sdc', hide=True)
    time.sleep(60)


def persist_metric(max_batch_size, replication_factor, partitions, threads, seconds, message_count):
    db = mysql.connector.connect(
        host="deepthought",
        user="root",
        passwd="V1ctoria",
        database="kafka_benchmark"
    )
    cursor = db.cursor()
    sql = "insert into kafka_benchmark (max_batch_size, replication_factor, partitions, threads, seconds, message_count) values (%s, %s, %s, %s, %s, %s)"
    val = (max_batch_size, replication_factor, partitions, threads, seconds, message_count)
    cursor.execute(sql, val)
    db.commit()


if __name__ == "__main__":
    max_batch_size_list = [1, 100, 1000, 10000, 100000]
    replication_factor_list = [1]
    partitions_list = [5]
    threads_list = [1, 2, 3, 4, 5, 6]

    for max_batch_size in max_batch_size_list:
        for replication_factor in replication_factor_list:
            for partitions in partitions_list:
                for threads in threads_list:
                    run_benchmark(max_batch_size=max_batch_size, replication_factor=replication_factor, partitions=partitions, threads=threads, seconds=30)

