#!/usr/bin/env python

import os
from resource_management.libraries.script.script import Script
from resource_management.libraries.functions.format import format
from resource_management.libraries.functions import get_kinit_path
from resource_management.libraries.functions.default import default

config = Script.get_config()
stack_root = Script.get_stack_root()

default_conf_file = "atlas-application.properties"

conf_file = default("/configurations/atlas-env/metadata_conf_file", default_conf_file)
conf_dir = '/etc/atlas-server'
pid_dir = default("/configurations/atlas-env/metadata_pid_dir", "/var/run/atlas")
pid_file = format("{pid_dir}/atlas.pid")

metadata_user = default("/configurations/atlas-env/metadata_user", None)
hbase_user = default("/configurations/hbase-env/hbase_user", None)
kafka_user = default("/configurations/kafka-env/kafka_user", None)

# Security related/required params
hostname = config['hostname']
security_enabled = default("/configurations/cluster-env/security_enabled", None)
kinit_path_local = get_kinit_path(default('/configurations/kerberos-env/executable_search_paths', None))
tmp_dir = Script.get_tmp_dir()

stack_name = default("/hostLevelParams/stack_name", None)
hadoop_conf_dir = '/etc/hadoop'
hadoop_bin_dir = default("/configurations/hadoop-env/install_dir" + '/bin', '/opt/hadoop/bin')
