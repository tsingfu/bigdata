#!/usr/bin/env python

from resource_management.libraries.resources.hdfs_resource import HdfsResource
from resource_management.libraries.functions.default import default
from resource_management.libraries.functions.get_not_managed_resources import get_not_managed_resources
from resource_management.libraries.functions import get_kinit_path
from resource_management.libraries.script.script import Script

import functools

# for create_hdfs_directory

# server configurations
config = Script.get_config()

hadoop_bin_dir = '/opt/hadoop/bin'
hadoop_conf_dir = '/etc/hadoop'

security_enabled = config['configurations']['cluster-env']['security_enabled']
kinit_path_local = get_kinit_path(default('/configurations/kerberos-env/executable_search_paths', None))
hostname = config["hostname"]
hdfs_user_keytab = config['configurations']['hadoop-env']['hdfs_user_keytab']
hdfs_user = config['configurations']['hadoop-env']['hdfs_user']
hdfs_principal_name = config['configurations']['hadoop-env']['hdfs_principal_name']
hdfs_site = config['configurations']['hdfs-site']
default_fs = config['configurations']['core-site']['fs.defaultFS']
dfs_type = default("/commandParams/dfs_type", "")

# create partial functions with common arguments for every HdfsResource call
# to create hdfs directory we need to import this and call HdfsResource in code

HdfsResource = functools.partial(
    HdfsResource,
    hdfs_resource_ignore_file="/var/lib/ambari-agent/data/.hdfs_resource_ignore",
    security_enabled=security_enabled,
    user=hdfs_user,
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
