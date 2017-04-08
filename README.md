# Kafka Monitor

Monitors kafka topics to ensure the following:

- Desired replication factor is set and that the desired ISR count is met.
- If RF and ISR do not match, this will automatically re-balance the topic.


# Install

```
WIP: python setup.py install
```

# Usage

```
usage: kafka_monitor.py [-h] -t TOPIC -e EXHIBITOR -z ZOOKEEPER

optional arguments:
  -h, --help            show this help message and exit
  -t TOPIC, --topic TOPIC
                        Topic Name
  -e EXHIBITOR, --exhibitor EXHIBITOR
                        Exhibitor endpoint used to get broker information from
                        rest API
  -z ZOOKEEPER, --zookeeper ZOOKEEPER
                        Zookeeper endpoint (can be IP or hostname)
```

# Requirements

- Kafka (tested and actively used on 0.10.2.0)

- Zookeeper

- Exhibitor (https://github.com/soabase/exhibitor)