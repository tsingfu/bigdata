#!/usr/bin/env python

from resource_management.libraries.functions import format
from resource_management.libraries.script import Script

# server configurations
config = Script.get_config()

conf_dir = "/etc/logstash"
logstash_user = 'logstash'
logstash_group = 'logstash'
log_dir = config['configurations']['logstash-env']['logstash_log_dir']
pid_dir = '/var/run/logstash'
pid_file = format("{pid_dir}/logstash.pid")
hostname = config['hostname']
java64_home = config['hostLevelParams']['java_home']
patterns_dir='/etc/logstash/patterns'

business_map_content = config['configurations']['logstash-env']['business_map_content']
indexer_content = config['configurations']['logstash-env']['indexer_content']
jvm_content = config['configurations']['logstash-env']['jvm_content']
logstash_content = config['configurations']['logstash-env']['logstash_content']
module_map_content = config['configurations']['logstash-env']['module_map_content']
init_content = config['configurations']['logstash-env']['init_content']
patterns_content = config['configurations']['logstash-env']['patterns_content']

