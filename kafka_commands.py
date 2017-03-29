#!/usr/bin/env python

import subprocess


class KafkaCommands(object):

    def __init__(self, zk):
        self.zk = zk

    def run_proc(self, command):
        process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()
        return output, error

    def describe_kafka_topic(self, topic_name):
        command = '/opt/kafka/bin/kafka-topics.sh --zookeeper {0} --describe --topic {1}'.format(
            self.zk,
            topic_name
        )
        return self.run_proc(command=command)

    def generate_partition_reassignment_json(self, file_name, topic_name, broker_list):
        stripped_broker_list = ",".join(broker_list)
        command = '''/opt/kafka/bin/kafka-reassign-partitions.sh
        --zookeeper {0}
        --topics-to-move-json-file {1}
        --broker-list {2}
        --generate'''.format(self.zk, file_name, stripped_broker_list)
        return self.run_proc(command=command)

    def apply_reassignment_json(self, file_name):
        command = '''/opt/kafka/bin/kafka-reassign-partitions.sh
        --zookeeper {0}
        --reassignment-json-file {1}
        --execute'''.format(self.zk, file_name)
        return self.run_proc(command=command)

    def verify_reassignment(self, file_name):
        command = '''/opt/kafka/bin/kafka-reassign-partitions.sh
        --zookeeper {0}
        --reassignment-json-file {1}
        --execute'''.format(self.zk, file_name)
        return self.run_proc(command=command)


if __name__ == '__main__':
    KafkaCommands().describe_kafka_topic(topic_name='__consumer_offsets')
