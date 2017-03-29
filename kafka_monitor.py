#!usr/bin/env python

import requests
import json
import logging

from kafka_commands import KafkaCommands
from create_json import CreateJson
from time import sleep
from random import sample
from log import SetLogging
from args import Args
from sys import exit


class KafkaMonitor(object):

    def __init__(self, stackname, domain):
        self.stackname = stackname
        self.domain = domain
        self.exhibitor_endpoint = 'http://zookeeper-internal-{0}.{1}:8080'.format(stackname, domain)
        self.zookeeper_elb = 'zookeeper-internal-{0}.{1}'.format(stackname, domain)
        self.desired_rf = 3
        self.min_rf = 2

    def get_broker_ids(self):
        response = requests.get("{0}/exhibitor/v1/explorer/node?key=/brokers/ids".format(
            self.exhibitor_endpoint)
        )

        if response.status_code == 200:

            return self.set_broker_list(payload=response.text)

        else:

            logging.error("There has been a problem with your request. Status Code: {0} -- ERROR OUTPUT: {0}".format(
                response.status_code,
                response.text)
            )
            exit(200)

    def set_broker_list(self, payload):
        json_data = json.loads(payload)
        return [x.get('title') for x in json_data]

    def get_topic_details(self, topic):
        return KafkaCommands(zk=self.zookeeper_elb).describe_kafka_topic(topic_name=topic)

    def compare_topic_isr_per_partition(self, topic):

        # If replication factor is less than EXPECTED_RF, then we need to take action

        topic_output = self.get_topic_details(topic=topic)
        topic_list = topic_output[0].replace('\t', '\n').splitlines()
        rf = next(r.split(':') for r in topic_list if 'ReplicationFactor' in r)[1]
        isr = [x.split()[1] for x in topic_list if 'Isr:' in x]

        for brokers_isr in isr:

            if int(rf) != len(brokers_isr.split(',')) and int(rf) > len(brokers_isr.split(',')):

                logging.info("Isr count: {0} does NOT match RF: {1}".format(len(brokers_isr.split(',')), rf))
                return True, rf

            elif int(rf) < self.min_rf:

                logging.error("Isr count: {0} is less than expected RF of {1}".format(rf, self.min_rf))
                return True, rf

        logging.info("Isr count: {0} matches RF: {1}".format(len(brokers_isr.split(',')), rf))
        return False, rf

    def validate_rf_broker_ratio(self, brokers, rf, count=0):
        if len(brokers) < rf:

            if count >= 1:
                logging.warning("A new broker has not come in with in the allotted timeframe, moving on...")
                return brokers

            # After a certain amount of failures, create a failed-healthcheck.txt file that the healthcheck can see
            logging.warning("Up Brokers: {0} RF: {2} -- Need broker count to match RF at a minimum".format(
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

    def write_json_file(self, data, file_name):
        with open(file_name, 'w') as f:
            f.write(data)

    def prepare_topic_for_rebalance(self, topic, active_brokers, all_brokers=None, search_replace=False):

        if search_replace:

            if len(all_brokers) < self.desired_rf:
                self.desired_rf = len(all_brokers)

            random_brokers = ",".join(sample(set(all_brokers), self.desired_rf)).split(',')
            logging.info("Randomized broker list: {0}".format(random_brokers))
            partition_scheme_json = CreateJson.generate_json_template(
                topic=topic, partition_list=50, replicas=random_brokers
            )
            logging.info("New Partition Scheme: {0}".format(partition_scheme_json))
            self.write_json_file(data=partition_scheme_json, file_name='/tmp/reassign.json')

        else:
            partition_scheme_json = CreateJson.generate_json_template(
                topic=topic, partition_list=50, replicas=active_brokers
            )
            logging.info("New Partition Scheme: {0}".format(partition_scheme_json))
            self.write_json_file(data=partition_scheme_json, file_name='/tmp/reassign.json')

    def apply_rebalance(self):
        return KafkaCommands(zk=self.zookeeper_elb).apply_reassignment_json(
            file_name='/tmp/reassign.json'
        )

    def validate_rebalance(self):
        response = KafkaCommands(zk=self.zookeeper_elb).verify_reassignment(file_name='/tmp/reassign.json')

        if 'is still in progress' in response:

            logging.info("WAITING ON REASSIGNMENT TO COMPLETE. CURRENT STATUS: {0}".format(response))
            sleep(10)
            self.validate_rebalance()  # TODO: Add counter

        else:

            logging.info("SUCCESS: REBALANCE COMPLETE!")

    def rebalance_topic(self, active_brokers, topic):
        rebalance, rf = self.compare_topic_isr_per_partition(topic=topic)
        replication_factor = int(rf)

        logging.info("REBALANCE: {0} CURRENT RF: {1} ACIVE BROKERS: {2} TOPIC: {3}".format(
            rebalance,
            rf,
            active_brokers,
            topic
            )
        )

        if replication_factor < self.min_rf or len(active_brokers) < self.min_rf:
            log_msg = "UNABLE TO PROCEED: MINIMUM REQUIREMENT OF ISR IS: {0}, AND WE HAVE {1} ACTIVE BROKERS.".format(
                self.min_rf, active_brokers)
            logging.error(log_msg)
            exit(99)

        if rebalance:

            if replication_factor <= self.min_rf or replication_factor > self.desired_rf:
                self.prepare_topic_for_rebalance(topic=topic,
                                                 all_brokers=active_brokers,
                                                 active_brokers=active_brokers[:replication_factor],
                                                 search_replace=True)

            elif replication_factor >= self.min_rf and len(active_brokers) < replication_factor:
                self.prepare_topic_for_rebalance(topic=topic,
                                                 all_brokers=active_brokers,
                                                 active_brokers=active_brokers[:replication_factor],
                                                 search_replace=True)
            else:
                self.prepare_topic_for_rebalance(topic=topic, active_brokers=active_brokers)

            self.apply_rebalance()
            self.validate_rebalance()

        else:

            logging.info("NO NEED TO REBALANCE, TOPIC {0} IS HEALTHY".format(topic))

    def main(self, topic_name):
        rebalance, rf = self.compare_topic_isr_per_partition(topic=topic_name)

        if rebalance:

            broker_ids = self.get_broker_ids()
            logging.info("ACTIVE BROKER IDS: {0}".format(broker_ids))
            active_brokers = self.validate_rf_broker_ratio(brokers=broker_ids, rf=int(rf))
            self.rebalance_topic(active_brokers, topic_name)

        else:

            logging.info("Topic name {0} is in full ISR, moving on...".format(topic_name))


if __name__ == '__main__':
    SetLogging.setup_logging()
    args = Args.get_args()
    KafkaMonitor(stackname=args.stackname, domain=args.domain).main(topic_name=args.topic)
