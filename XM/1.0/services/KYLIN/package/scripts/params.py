#!/usr/bin/env python

import functools
import os
from resource_management.libraries.functions import get_kinit_path
from resource_management.libraries.functions.default import default
from resource_management.libraries.functions.format import format
from resource_management.libraries.resources.hdfs_resource import HdfsResource
from resource_management.libraries.script.script import Script


# server configurations
config = Script.get_config()
stack_root = '/opt'

service_packagedir = os.path.realpath(__file__).split('/scripts')[0]

install_dir = config['configurations']['kylin-env']['install_dir']
download_url = config['configurations']['kylin-env']['download_url']
filename = download_url.split('/')[-1]
version_dir = filename.replace('.tar.gz','').replace('.tgz','')

spark_home = spark2_home = "/opt/spark"

# params from kylin-env
kylin_user = config['configurations']['kylin-env']['kylin_user']
kylin_group = config['configurations']['kylin-env']['kylin_group']
kylin_log_dir = config['configurations']['kylin-env']['kylin_log_dir']
kylin_pid_dir = config['configurations']['kylin-env']['kylin_pid_dir']
kylin_hdfs_user_dir = format("/user/{kylin_user}")
kylin_log_file = os.path.join(kylin_log_dir, 'setup.log')
kylin_cluster_servers = config['configurations']['kylin-env']['kylin.server.cluster-servers']
kylin_dir = install_dir
conf_dir = "/etc/kylin"
kylin_pid_file = kylin_pid_dir + '/kylin-' + kylin_user + '.pid'
server_mode = 'job'

kylin_properties_template = config['configurations']['kylin-env']['content']
kylin_env_template = config['configurations']['kylin-env']['kylin_env_content']

kylin_check_env_template = config['configurations']['kylin-env']['kylin-check-env-content']

log4j_server_props = config['configurations']['kylin-env']['kylin-server-log4j']
log4j_tool_props = config['configurations']['kylin-env']['kylin-tools-log4j']

kylin_kerberos_keytab = config['configurations']['kylin-env'][
    'kylin.server.kerberos.keytab']
kylin_kerberos_principal = config['configurations']['kylin-env'][
    'kylin.server.kerberos.principal']

# detect configs
master_configs = config['clusterHostInfo']
java64_home = config['hostLevelParams']['java_home']
ambari_host = str(master_configs['ambari_server_host'][0])

hbase_master_hosts = default("/clusterHostInfo/hbase_master_hosts", [])

hdfs_user = config['configurations']['hadoop-env']['hdfs_user']
security_enabled = config['configurations']['cluster-env']['security_enabled']
hdfs_user_keytab = config['configurations']['hadoop-env']['hdfs_user_keytab']
kinit_path_local = get_kinit_path(
    default('/configurations/kerberos-env/executable_search_paths', None))
hadoop_bin_dir = config['configurations']['hadoop-env']['install_dir'] + '/bin'
hadoop_conf_dir = '/etc/hadoop'
hdfs_principal_name = config['configurations']['hadoop-env'][
    'hdfs_principal_name']
hdfs_site = config['configurations']['hdfs-site']
default_fs = config['configurations']['core-site']['fs.defaultFS']

# create partial functions with common arguments for every HdfsResource call
# to create hdfs directory we need to call params.HdfsResource in code
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
    default_fs=default_fs)
