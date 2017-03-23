#!/usr/bin/env python

import requests
import json

from random import choice
from kafka_commands import KafkaCommands


class KafkaMonitor(object):

    def __init__(self,
                 exhibitor_endpoint='http://zookeeper-akeller.ng.cloudpassage.com:8080',
                 zookeeper_elb='zookeeper-internal-akeller.ng.cloudpassage.com'):
        self.exhibitor_endpoint = exhibitor_endpoint
        self.zookeeper_elb = zookeeper_elb

    def get_broker_ids(self):
        response = requests.get("{0}/exhibitor/v1/explorer/node?key=/brokers/ids".format(
            self.exhibitor_endpoint)
        )

        if response.status_code == 200:
            return self.set_broker_list(payload=response.text)
        else:
            print("There has been a problem with your request. Status Code: {0} -- ERROR OUTPUT: {0}".format(
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
        topic_output = self.get_topic_details(topic=topic)
        topic_list = topic_output[0].replace('\t', '\n').splitlines()
        rf = next(r.split(':') for r in topic_list if 'ReplicationFactor' in r)[1]
        isr = [x.split()[1] for x in topic_list if 'Isr:' in x]

        for brokers_isr in isr:
            if not int(rf) == len(brokers_isr.split(',')):
                print "Isr: {0} does NOT match RF: {1}".format(len(brokers_isr.split(',')), rf)
                return True, rf

        print "Isr: {0} matches RF: {1}".format(len(brokers_isr.split(',')), rf)
        return False, rf

    def validate_rf_broker_ratio(self, brokers, rf):
        """
        Ensure that there are enough active brokers based on rf and isr.

        TODO: If brokers are less then rf, and isr is less than minimum, but we have enough to balance isr,
        continue to rebalance
        """
        if len(brokers) < rf:
            # After a certain amount of failures, create a failed-healthcheck.txt file that the healthcheck can see
            print "broker ids: {0} total: {1} rf: {2} -- Need brokers to match RF at a minimum".format(brokers, len(brokers), rf)
            broker_ids = self.get_broker_ids()
            sleep(5)
            return self.validate_rf_broker_ratio(brokers=broker_ids, rf=rf)
        else:
            return brokers

    def write_json_file(self, data, file_name):
        with open(file_name, 'w') as f:
            f.write(data)

    def prepare_topic_for_rebalance(self, topic, active_brokers):
        topics_json = '{\"topics\": [{\"topic\": \"%s\" }], \"version\": 1}\n' % topic
        self.write_json_file(data=topics_json, file_name='/tmp/topics.json')
        partition_scheme_json = KafkaCommands(zk=self.zookeeper_elb).generate_partition_reassignment_json(
            file_name='/tmp/topics.json',
            topic_name=topic,
            broker_list=active_brokers
        )[0].split("\n")[4]
        self.write_json_file(data=partition_scheme_json, file_name='/tmp/reassign.json')

    def apply_rebalance(self):
        return KafkaCommands(zk=self.zookeeper_elb).apply_reassignment_json(
            file_name='/tmp/reassign.json'
        )

    def validate_rebalance(self):
        response = KafkaCommands(zk=self.zookeeper_elb).verify_reassignment(file_name='/tmp/reassign.json')
        print response
        if 'is still in progress' in response:
            print "WAITING ON REASSIGNMENT TO COMPLETE. CURRENT STATUS: {0}".format(response)
            sleep(10)
            self.validate_rebalance() #  Add counter
        else:
            print "REBALANCE COMPLETE!"

    def rebalance_topic(self, active_brokers, topic):
        print "CHECKING IF REBALANCE IS STILL NEEDED"
        rebalance, rf = self.compare_topic_isr_per_partition(topic=topic)
        print "REBALANCE: {0} CURRENT RF: {1} ACIVE BROKERS: {2} TOPIC: {3}".format(rebalance, rf, active_brokers, topic)
        if rebalance:
            self.prepare_topic_for_rebalance(topic=topic, active_brokers=active_brokers)
            self.apply_rebalance()
            self.validate_rebalance()
        else:
            print "NO NEED TO REBALANCE, TOPIC {0} IS HEALTHY".format(topic)

    def main(self, topic_name):
        rebalance, rf = self.compare_topic_isr_per_partition(topic=topic_name)
        if rebalance:
            broker_ids = self.get_broker_ids()
            print "ACTIVE BROKER IDS: {0}".format(broker_ids)
            active_brokers = self.validate_rf_broker_ratio(brokers=broker_ids, rf=rf)
            self.rebalance_topic(active_brokers, topic_name)
        else:
            print "Topic name {0} is in full ISR, moving on...".format(topic_name)


if __name__ == '__main__':
    topic = argv[1]
    KafkaMonitor().main(topic_name=topic)

