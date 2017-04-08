#!/usr/bin/env python

import requests
import json

from time import sleep
from random import sample

from kafka_helpers.kafka_commands import KafkaCommands
from helpers.create_json import CreateJson
from helpers.log import SetLogging
from helpers.args import Args


class KafkaMonitor(object):

    def __init__(self, exhibitor, zookeeper):
        self.exhibitor_endpoint = exhibitor
        self.zookeeper_elb = zookeeper
        self.desired_rf = 3
        self.min_rf = 2

    def logger(self, level, msg, exit_code=None):
        return SetLogging().log(level, msg, exit_code) 

    def get_broker_ids(self):
        response = requests.get("{0}/exhibitor/v1/explorer/node?key=/brokers/ids".format(
            self.exhibitor_endpoint)
        )

        if response.status_code == 200:

            return self.set_broker_list(payload=response.text)

        else:

            self.logger(level='error',
                        details="There has been a problem with your request. Status Code: {0} -- ERROR OUTPUT: {1}"
                        .format(response.status_code, response.text),
                        exit_code=200)

    def set_broker_list(self, payload):
        json_data = json.loads(payload)
        return [x.get('title') for x in json_data]

    def get_topic_details(self, topic):
        return KafkaCommands(zk=self.zookeeper_elb).describe_kafka_topic(topic_name=topic)

    def compare_topic_isr_per_partition(self, topic):

        # If replication factor is less than EXPECTED_RF, then we need to take action

        topic_output = self.get_topic_details(topic=topic)
        topic_list = topic_output[0].replace('\t', '\n').splitlines()
        partition_count = next(r.split(':') for r in topic_list if 'PartitionCount' in r)[1]
        rf = next(r.split(':') for r in topic_list if 'ReplicationFactor' in r)[1]
        isr = [x.split()[1] for x in topic_list if 'Isr:' in x]

        for brokers_isr in isr:

            if int(rf) != len(brokers_isr.split(',')) and int(rf) > len(brokers_isr.split(',')):

                self.logger("info", "Isr count: {0} does NOT match desired RF: {1}".format(len(brokers_isr.split(',')), rf))
                return True, rf, partition_count

            elif int(rf) < self.min_rf:

                self.logger("error", "Isr count: {0} is less than expected RF of {1}".format(rf, self.min_rf))
                return True, rf, partition_count

        self.logger("info", "Isr count: {0} matches RF: {1}".format(len(brokers_isr.split(',')), rf))
        return False, rf, partition_count

    def validate_rf_broker_ratio(self, brokers, rf, count=0, limit=10):
        if len(brokers) < rf:

            if count >= limit:
                self.logger("warning", "A new broker has not come in with in the allotted timeframe, moving on...")
                return brokers

            # After a certain amount of failures, create a failed-healthcheck.txt file that the healthcheck can see
            self.logger("warning", "Up Brokers: {0} RF: {2} -- Need broker count to match RF at a minimum".format(
                brokers,
                len(brokers),
                rf
            ))

            broker_ids = self.get_broker_ids()
            count+=1
            sleep(6)

            return self.validate_rf_broker_ratio(brokers=broker_ids, rf=rf, count=count)

        else:

            return brokers

    def create_partition_scheme(self, topic, partition_list, replicas):
        return CreateJson.generate_json_template(
            topic=topic, partition_list=partition_list, replicas=replicas
        )

    def determine_rf(self, brokers):
        if len(brokers) < self.desired_rf:
            self.desired_rf = len(brokers)

    def validate_safe_rf(self, brokers):
        if len(brokers) < self.min_rf:
            self.logger("error", "UNABLE TO PROCEED: MINIMUM REQUIREMENT OF ISR IS: {0}, AND WE HAVE {1} ACTIVE BROKERS.".format(
                self.min_rf, brokers), 99)

    def prepare_topic_for_rebalance(self, topic, active_brokers, all_brokers=None, partitions=32, search_replace=False):
        if search_replace:
            self.determine_rf(brokers=all_brokers)
            random_brokers = ",".join(sample(set(all_brokers), self.desired_rf)).split(',')
            brokers = random_brokers
        else:
            brokers = active_brokers

        partition_scheme_json = self.create_partition_scheme(topic, partitions, brokers)
        self.logger("info", "New Partition Scheme: {0}".format(partition_scheme_json))
        CreateJson.write_json_file(data=partition_scheme_json, file_name='/tmp/reassign.json')

    def apply_rebalance(self):
        return KafkaCommands(zk=self.zookeeper_elb).apply_reassignment_json(
            file_name='/tmp/reassign.json'
        )

    def validate_rebalance(self):
        response = KafkaCommands(zk=self.zookeeper_elb).verify_reassignment(file_name='/tmp/reassign.json')
        self.logger("info", "STATUS: {0}".format(response))

        if 'is still in progress' in response:
            self.logger("info", "WAITING ON REASSIGNMENT TO COMPLETE. CURRENT STATUS: {0}".format(response))
            sleep(10)
            self.validate_rebalance()  # TODO: Add counter
        elif 'failure' in response:
            self.logger("error", "FAILURE: REBALANCE FAILED, PLEASE INVESTIGATE: {0}".format(response), 245)
        else:
            self.logger("info", "SUCCESS: REBALANCE COMPLETE!")

    def rebalance_topic(self, active_brokers, topic):
        rebalance, rf, partitions = self.compare_topic_isr_per_partition(topic=topic)
        replication_factor = int(rf)
        partition_count = int(partitions)

        self.logger("info", "REBALANCE: {0} CURRENT RF: {1} ACIVE BROKERS: {2} TOPIC: {3}".format(
            rebalance,
            rf,
            active_brokers,
            topic
            )
        )

        self.validate_safe_rf(brokers=active_brokers)

        if rebalance:

            if replication_factor <= self.min_rf or replication_factor > self.desired_rf:

                self.prepare_topic_for_rebalance(topic=topic,
                                                 all_brokers=active_brokers,
                                                 active_brokers=active_brokers[:replication_factor],
                                                 partitions=partition_count,
                                                 search_replace=True)

            elif replication_factor >= self.min_rf and len(active_brokers) < replication_factor:

                self.logger("info", "{0} >= {1} && {2} < {0}".format(replication_factor, self.min_rf, len(active_brokers)))
                self.prepare_topic_for_rebalance(topic=topic,
                                                 all_brokers=active_brokers,
                                                 active_brokers=active_brokers[:replication_factor],
                                                 partitions=partition_count,
                                                 search_replace=True)
            else:

                self.prepare_topic_for_rebalance(topic=topic, active_brokers=active_brokers, partitions=partition_count)

            self.apply_rebalance()
            self.validate_rebalance()

        else:

            self.logger("info", "NO NEED TO REBALANCE, TOPIC {0} IS HEALTHY".format(topic))

    def main(self, topic_name):
        rebalance, rf, partition_count = self.compare_topic_isr_per_partition(topic=topic_name)
        if rebalance:
            broker_ids = self.get_broker_ids()
            self.logger("info", "ACTIVE BROKER IDS: {0}".format(broker_ids))
            active_brokers = self.validate_rf_broker_ratio(brokers=broker_ids, rf=int(rf))
            self.rebalance_topic(active_brokers, topic_name)
        else:
            self.logger("info", "Topic name {0} is in full ISR, moving on...".format(topic_name))


if __name__ == '__main__':
    args = Args.get_args()
    KafkaMonitor(exhibitor=args.exhibitor, zookeeper=args.zookeeper).main(topic_name=args.topic)
