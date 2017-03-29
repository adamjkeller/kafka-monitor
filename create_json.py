#!/usr/bin/env python

import json
from ast import literal_eval


class CreateJson(object):

    @staticmethod
    def create_template():
        return {"version":1,"partitions":[]}

    @staticmethod
    def add_partitions(topic, partition, replicas):
        formatted_replicas = "[{0}]".format(",".join(replicas).replace("'",""))
        return {"topic":topic,"partition":partition,"replicas":literal_eval(formatted_replicas)}

    @staticmethod
    def generate_json_template(topic, partition_list, replicas):
        json_blob = CreateJson.create_template()
        for partition in range(partition_list):
            json_blob['partitions'].append(
                CreateJson.add_partitions(topic, partition, replicas)
            )

        return json.dumps(json_blob)

    @staticmethod
    def write_json_file(data, file_name):
        with open(file_name, 'w') as f:
            f.write(data)

if __name__ == '__main__':
    topic = ['__consumer_offsets']
    partitions = 50
    replicas = ['1001', '1002', '1004']
    print CreateJson().generate_json_template(
        topic=topic,
        partition_list=partitions,
        replicas=replicas
    )
