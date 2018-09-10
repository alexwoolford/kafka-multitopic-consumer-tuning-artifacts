
from subprocess import call
from streamsets.sdk import DataCollector

# This is run on a Kafka node because the client doesn't support topic creation with replication factor and partitions
#
# partitions_list = [1, 2, 3, 4, 5]
# replication_factor_list = [1, 2, 3, 4, 5]
# for partitions in partitions_list:
#     for replication_factor in replication_factor_list:
#         call(["/usr/bin/kafka-topics", "--create", "--zookeeper",
#               "cdh01.woolford.io:2181,cdh02.woolford.io:2181,cdh03.woolford.io:2181",
#               "--replication-factor", str(replication_factor),
#               "--partitions", str(partitions),
#               "--topic", "kafka-benchmark-replication-factor-{0}-partitions-{1}".format(replication_factor, partitions)])


def get_topics():
    partitions_list = [1, 2, 3, 4, 5]
    replication_factor_list = [1, 2, 3, 4, 5]
    topics = []
    for partitions in partitions_list:
        for replication_factor in replication_factor_list:
            topic = "kafka-benchmark-replication-factor-{0}-partitions-{1}".format(replication_factor, partitions)
            topics.append(topic)
    return topics


def create_stage_data_pipeline():
    datacollector_url = 'http://sdc.woolford.io:18630'
    data_collector = DataCollector(datacollector_url, username="admin", password="admin")

    builder = data_collector.get_pipeline_builder()

    dev_data_generator = builder.add_stage("Dev Data Generator")
    dev_data_generator.fields_to_generate = [{"type": "LONG", "field": "id", "precision": 10, "scale": 2}]

    kafka_producers = []
    for topic in get_topics():
        kafka_producer = builder.add_stage("Kafka Producer")
        kafka_producer.library = "streamsets-datacollector-cdh_kafka_3_0-lib"
        kafka_producer.data_format = "JSON"
        kafka_producer.broker_uri = "cdh03.woolford.io:9092,cdh04.woolford.io:9092,cdh05.woolford.io:9092,cdh06.woolford.io:9092"
        kafka_producer.topic = topic

        kafka_producers.append(kafka_producer)

    dev_data_generator >> kafka_producers

    pipeline = builder.build('stage-data')
    data_collector.add_pipeline(pipeline)


if __name__ == "__main__":
    create_stage_data_pipeline()

