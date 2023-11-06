import argparse
import arrow
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from datetime import datetime
from ip2asn import ip2asn
import logging
import msgpack
import os
import sys

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        logging.error('Message delivery failed: {}'.format(err))
    else:
        # print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
        pass

def create_topic(topic, replication):

    admin_client = AdminClient({'bootstrap.servers': KAFKA_HOST})

    # compacted topic
    topic_list = [NewTopic(topic, num_partitions=5, replication_factor=replication,
                  config={'cleanup.policy': 'compact'})]
    created_topic = admin_client.create_topics(topic_list)
    for topic, f in created_topic.items():
        try:
            f.result()  # The result itself is None
            logging.warning("Topic {} created".format(topic))
        except Exception as e:
            logging.warning("Failed to create topic {}: {}".format(topic, e))


if __name__ == "__main__":

    global KAFKA_HOST
    KAFKA_HOST = os.environ["KAFKA_HOST"]

    default_date = str(arrow.now().replace(day=1, hour=0, minute=0, second=0))

    parser = argparse.ArgumentParser()
    parser.add_argument("--date",
                        help="Date of the dump to push to kafka. Push data for current month if empty.",
                        type=str, default=default_date)
    parser.add_argument("--topic",
                        help="Name of the topic where the data is pushed. (default: ihr_ip2asn)",
                        type=str, default='ihr_ip2asn')
    parser.add_argument("--replication",
                        help="Replication factor for the created topic. (default: 2)",
                        type=int, default=2)
    args = parser.parse_args()

    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
            format=FORMAT,
            level=logging.WARN,
            datefmt='%Y-%m-%d %H:%M:%S',
            handlers=[logging.StreamHandler()]
            )
    logging.warning("Started: %s" % sys.argv)
    logging.warning("Arguments: %s" % args)

    date = arrow.get(args.date)
    ia = ip2asn(f"db/rib.{date.year}{date.month:02d}{date.day:02d}.pickle.bz2")

    # create the kafka topic
    create_topic(args.topic, args.replication)

    # Create producer
    producer = Producer({
        'bootstrap.servers': KAFKA_HOST,
        'queue.buffering.max.messages': 10000000,
        'queue.buffering.max.kbytes': 2097151,
        'linger.ms': 200,
        'batch.num.messages': 1000000,
        'message.max.bytes': 999000,
        'default.topic.config': {'compression.codec': 'snappy'}
        })

    # push all prefixes to kafka
    for rnode in ia.rtree:
        try:
            producer.produce(
                args.topic,
                msgpack.packb(rnode.data, use_bin_type=True),
                callback=delivery_report,
                timestamp=int(date.timestamp()*1000),
                key=rnode.prefix
                )

            # Trigger any available delivery report callbacks from previous produce() calls
            producer.poll(0)
        except BufferError:
            logging.warning('Buffer error, the queue must be full! Flushing...')
            producer.flush()

            logging.info('Queue flushed, will write the message again')
            producer.produce(
                args.topic,
                msgpack.packb(rnode.data, use_bin_type=True), 
                callback=delivery_report,
                timestamp=int(date.timestamp()*1000),
                key=rnode.prefix
            )
            producer.poll(0)

    # Wait for any outstanding messages to be delivered and delivery report
    # callbacks to be triggered.
    producer.flush()

