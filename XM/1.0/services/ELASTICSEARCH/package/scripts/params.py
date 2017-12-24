#!/usr/bin/env python

from resource_management.libraries.script import Script

# server configurations
config = Script.get_config()

elastic_home = config['configurations']['elastic-sysconfig']['elastic_home']
data_dir = config['configurations']['elastic-sysconfig']['data_dir']
work_dir = config['configurations']['elastic-sysconfig']['work_dir']
conf_dir = config['configurations']['elastic-sysconfig']['conf_dir']
heap_size = config['configurations']['elastic-sysconfig']['heap_size']
max_open_files = config['configurations']['elastic-sysconfig']['max_open_files']
max_map_count = config['configurations']['elastic-sysconfig']['max_map_count']

elastic_user = config['configurations']['elastic-env']['elastic_user']
elastic_group = config['configurations']['elastic-env']['elastic_group']
log_dir = config['configurations']['elastic-env']['elastic_log_dir']
pid_dir = config['configurations']['elastic-env']['elastic_pid_dir']

hostname = config['hostname']
java64_home = config['hostLevelParams']['java_home']
elastic_env_sh_template = config['configurations']['elastic-env']['content']
sysconfig_template = config['configurations']['elastic-sysconfig']['content']

master_content = config['configurations']['elastic-env']['master_content']
data_content = config['configurations']['elastic-env']['data_content']
tribe_content = config['configurations']['elastic-env']['tribe_content']
client_content = config['configurations']['elastic-env']['client_content']

jvm_content = config['configurations']['elastic-env']['jvm_content']

cluster_name = config['configurations']['elastic-env']['cluster_name']
zen_discovery_ping_unicast_hosts = config['configurations']['elastic-env']['zen_discovery_ping_unicast_hosts']

from resource_management.libraries.functions import default
data_hosts= default("/clusterHostInfo/es_data_hosts", [])
master_hosts= default("/clusterHostInfo/es_master_hosts", [])
client_hosts= default("/clusterHostInfo/es_client_hosts", [])
tribe_hosts= default("/clusterHostInfo/es_tribe_hosts", [])

role = 'tribe'
if hostname in data_hosts:
    role = 'data'
elif hostname in master_hosts:
    role = 'master'
elif hostname in client_hosts:
    role = 'client'
else:
    role = 'tribe'

