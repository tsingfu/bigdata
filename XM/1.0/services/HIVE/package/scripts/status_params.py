#!/usr/bin/env python

from resource_management.libraries.functions import format
from resource_management.libraries.functions.version import format_stack_version
from resource_management.libraries.functions.default import default
from resource_management.libraries.functions import get_kinit_path
from resource_management.libraries.script.script import Script

# a map of the Ambari role to the component name
SERVER_ROLE_DIRECTORY_MAP = {
    'HIVE_METASTORE': 'hive-metastore',
    'HIVE_SERVER': 'hive-server2',
    'WEBHCAT_SERVER': 'hive-webhcat',
    'HIVE_CLIENT': 'hive-client',
    'HCAT': 'hive-client',
    'HIVE_SERVER_INTERACTIVE': 'hive-server2-hive2'
}

# Either HIVE_METASTORE, HIVE_SERVER, WEBHCAT_SERVER, HIVE_CLIENT, HCAT, HIVE_SERVER_INTERACTIVE
role = default("/role", None)

config = Script.get_config()

install_dir = config['configurations']['hive-env']['install_dir']
component_directory = install_dir
component_directory_interactive = install_dir

stack_root = '/opt'
stack_version_unformatted = config['hostLevelParams']['stack_version']
stack_version_formatted_major = format_stack_version(stack_version_unformatted)

hive_pid_dir = config['configurations']['hive-env']['hive_pid_dir']
hive_pid = hive_pid_dir + '/hive-server.pid'
hive_interactive_pid = hive_pid_dir + '/hive-interactive.pid'
hive_metastore_pid = hive_pid_dir + '/hive.pid'

hcat_pid_dir = config['configurations']['hive-env'][
    'hcat_pid_dir']  # hcat_pid_dir
webhcat_pid_file = format('{hcat_pid_dir}/webhcat.pid')

process_name = 'mysqld'
daemon_name = 'mysqld'

# Security related/required params
hostname = config['hostname']
security_enabled = config['configurations']['cluster-env']['security_enabled']
kinit_path_local = get_kinit_path(
    default('/configurations/kerberos-env/executable_search_paths', None))
tmp_dir = Script.get_tmp_dir()
hdfs_user = config['configurations']['hadoop-env']['hdfs_user']
hive_user = config['configurations']['hive-env']['hive_user']
webhcat_user = config['configurations']['hive-env']['webhcat_user']
hive_home_dir = config['configurations']['hive-env']['install_dir']

# default configuration directories
hadoop_conf_dir = '/etc/hadoop'
hadoop_bin_dir = config['configurations']['hadoop-env']['install_dir'] + '/bin'
hive_etc_dir_prefix = '/etc/hive'
hive_interactive_etc_dir_prefix = '/etc/hive'

hive_server_conf_dir = '/etc/hive'
hive_server_interactive_conf_dir = '/etc/hive'

webhcat_conf_dir = '/etc/hive'
hive_conf_dir = '/etc/hive'
hive_client_conf_dir = '/etc/hive'

hive_config_dir = hive_client_conf_dir

if 'role' in config and config['role'] in ["HIVE_SERVER", "HIVE_METASTORE",
                                           "HIVE_SERVER_INTERACTIVE"]:
    hive_config_dir = hive_server_conf_dir

stack_name = default("/hostLevelParams/stack_name", None)
