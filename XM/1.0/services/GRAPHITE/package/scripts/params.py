#!/usr/bin/env python

from resource_management.libraries.script import Script
from resource_management.libraries.functions import default

# server configurations
config = Script.get_config()

graphite_user = config['configurations']['graphite-env']['graphite_user']
graphite_group = config['configurations']['graphite-env']['graphite_group']
log_dir = config['configurations']['graphite-env']['graphite_log_dir']
pid_dir = config['configurations']['graphite-env']['graphite_pid_dir']
graphite_pid_dir = config['configurations']['graphite-env']['graphite_pid_dir']
graphite_pid_file = format("{graphite_pid_dir}/graphite.pid")

hostname = config['hostname']
java64_home = config['hostLevelParams']['java_home']

graphite_api_hosts = default("/clusterHostInfo/graphite_api_hosts", [])
carbon_c_relay_hosts = default("/clusterHostInfo/carbon_c_relay_hosts", [])
kenshin_hosts = default("/clusterHostInfo/kenshin_hosts", [])
