#!/usr/bin/env python

from resource_management.libraries.functions import format
from resource_management.libraries.functions.default import default
from resource_management.libraries.functions.version import format_stack_version
from resource_management.libraries.functions import get_kinit_path
from resource_management.libraries.script.script import Script

# a map of the Ambari role to the component name
# for use with <stack-root>/current/<component>
SERVER_ROLE_DIRECTORY_MAP = {
    'HBASE_MASTER': 'hbase-master',
    'HBASE_REGIONSERVER': 'hbase-regionserver',
    'HBASE_CLIENT': 'hbase-client'
}

config = Script.get_config()

component_directory = config['configurations']['hbase-env']['install_dir']
log_dir = config['configurations']['hbase-env']['hbase_log_dir']
pid_dir = config['configurations']['hbase-env']['hbase_pid_dir']
hbase_user = config['configurations']['hbase-env']['hbase_user']

hbase_master_pid_file = format("{pid_dir}/hbase-{hbase_user}-master.pid")
regionserver_pid_file = format("{pid_dir}/hbase-{hbase_user}-regionserver.pid")
phoenix_pid_file = format("{pid_dir}/{hbase_user}-queryserver.pid")

# Security related/required params
hostname = config['hostname']
security_enabled = config['configurations']['cluster-env']['security_enabled']
kinit_path_local = get_kinit_path(
    default('/configurations/kerberos-env/executable_search_paths', None))
tmp_dir = Script.get_tmp_dir()

stack_version_unformatted = str(config['hostLevelParams']['stack_version'])
stack_version_formatted = format_stack_version(stack_version_unformatted)
stack_root = '/opt'

hbase_conf_dir = '/etc/hbase'
limits_conf_dir = "/etc/security/limits.d"
stack_name = default("/hostLevelParams/stack_name", None)
