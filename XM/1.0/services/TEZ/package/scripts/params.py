#!/usr/bin/env python

from resource_management.libraries.resources import HdfsResource
from resource_management.libraries.functions.default import default
from resource_management.libraries.functions import get_kinit_path
from resource_management.libraries.functions.get_not_managed_resources import get_not_managed_resources
from resource_management.libraries.script.script import Script

# server configurations
config = Script.get_config()
tmp_dir = Script.get_tmp_dir()

install_dir = config['configurations']['tez-env']['install_dir']
download_url = config['configurations']['tez-env']['download_url']
filename = download_url.split('/')[-1]
version_dir = filename.replace('.tar.gz', '').replace('.tgz', '')

stack_name = default("/hostLevelParams/stack_name", None)
stack_root = '/opt'

# New Cluster Stack Version that is defined during the RESTART of a Rolling Upgrade
version = default("/commandParams/version", None)

hadoop_home = '/opt/hadoop'
hadoop_lib_home = hadoop_home + '/lib'

hadoop_bin_dir = hadoop_home + '/bin'
hadoop_conf_dir = '/etc/hadoop'
tez_etc_dir = "/etc/tez"
config_dir = "/etc/tez/conf"
tez_examples_jar = install_dir + '/tez-examples*.jar'

# Heap dump related
heap_dump_enabled = default('/configurations/tez-env/enable_heap_dump', None)
heap_dump_opts = ""  # Empty if 'heap_dump_enabled' is False.
if heap_dump_enabled:
    heap_dump_path = default('/configurations/tez-env/heap_dump_location', "/tmp")
    heap_dump_opts = " -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=" + heap_dump_path

kinit_path_local = get_kinit_path(default('/configurations/kerberos-env/executable_search_paths', None))
security_enabled = config['configurations']['cluster-env']['security_enabled']
smokeuser = config['configurations']['cluster-env']['smokeuser']
smokeuser_principal = config['configurations']['cluster-env']['smokeuser_principal_name']
smoke_user_keytab = config['configurations']['cluster-env']['smokeuser_keytab']
hdfs_user = config['configurations']['hadoop-env']['hdfs_user']
hdfs_principal_name = config['configurations']['hadoop-env']['hdfs_principal_name']
hdfs_user_keytab = config['configurations']['hadoop-env']['hdfs_user_keytab']

java64_home = config['hostLevelParams']['java_home']

tez_user = config['configurations']['tez-env']['tez_user']
user_group = config['configurations']['cluster-env']['user_group']
tez_env_sh_template = config['configurations']['tez-env']['content']

hdfs_site = config['configurations']['hdfs-site']
default_fs = config['configurations']['core-site']['fs.defaultFS']

dfs_type = default("/commandParams/dfs_type", "")

import functools

HdfsResource = functools.partial(
    HdfsResource,
    user=hdfs_user,
    hdfs_resource_ignore_file="/var/lib/ambari-agent/data/.hdfs_resource_ignore",
    security_enabled=security_enabled,
    keytab=hdfs_user_keytab,
    kinit_path_local=kinit_path_local,
    hadoop_bin_dir=hadoop_bin_dir,
    hadoop_conf_dir=hadoop_conf_dir,
    principal_name=hdfs_principal_name,
    hdfs_site=hdfs_site,
    default_fs=default_fs,
    immutable_paths=get_not_managed_resources(),
    dfs_type=dfs_type
)
