#!/usr/bin/env bash

topics=(__consumer_offsets
agent_alive
agent_events
agent_issue_resolver
agent_issue_updater
connections_index
critical
csm_finding_events
csm_policy_events
csm_scan_events
daily_email
default
delete_customer
delete_scan
delete_scan_finding
delete_server
delete_server_event
deprecated_facts
es_server_group_rename
event_count
events_index
facts
failed-lids
failed-register_with_salesforce
fim
fw
group_events
group_rename
heartbeat
index_copy
issue_events
issues_index
kafka-connect-configs
kafka-connect-offsets
kafka-connect-status
lids
mail_digest_alerts
mail_instant_alerts
policies_index
post_usage
register_with_salesforce
reload_agent_group_fw_state
reload_agent_group_state
reload_agent_group_state_sva
reload_agent_group_sva_state
rule_events
salesforce_aggregation_purge
salesforce_create_contact
salesforce_daily_account_update
salesforce_first_deployment
salesforce_update_account
salesforce_update_eval
salesforce_weekly_usage
sam
sca
scans_index
server_accounts_index
server_events
servers_index
shutdown
snapshot_count
sv
sva2
svm
td
update_heartbeat_interval
update_server_accounts_index
user_requests)

for topic in ${topics[@]};do
  python kafka_monitor.py $topic
done
