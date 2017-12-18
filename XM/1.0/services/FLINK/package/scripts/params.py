#!/usr/bin/env python
from resource_management import *
from resource_management.libraries.script.script import Script
import sys, os, glob
from resource_management.libraries.functions.default import default
from resource_management.libraries.resources.hdfs_resource import HdfsResource
import functools
from resource_management.libraries.functions import get_kinit_path
from resource_management.libraries.functions.get_not_managed_resources import get_not_managed_resources

config = Script.get_config()
service_packagedir = os.path.realpath(__file__).split('/scripts')[0]

# params from flink-config
flink_numcontainers = config['configurations']['flink-config'][
    'flink_numcontainers']
flink_numberoftaskslots = config['configurations']['flink-config'][
    'flink_numberoftaskslots']
flink_jobmanager_memory = config['configurations']['flink-config'][
    'flink_jobmanager_memory']
flink_container_memory = config['configurations']['flink-config'][
    'flink_container_memory']
flink_appname = config['configurations']['flink-config']['flink_appname']
flink_queue = config['configurations']['flink-config']['flink_queue']
flink_streaming = config['configurations']['flink-config']['flink_streaming']

hadoop_conf_dir = config['configurations']['flink-config']['hadoop_conf_dir']
install_dir = config['configurations']['flink-env']['install_dir']
download_url = config['configurations']['flink-env']['download_url']
filename = download_url.split('/')[-1]
version_dir = filename.replace('.tar.gz','').replace('.tgz','')

conf_dir = '/etc/flink'
bin_dir = install_dir + '/bin'

# params from flink-conf.yaml
flink_yaml_content = config['configurations']['flink-env']['content']
flink_user = config['configurations']['flink-env']['flink_user']
flink_group = config['configurations']['flink-env']['flink_group']
flink_log_dir = config['configurations']['flink-env']['flink_log_dir']
flink_log_file = os.path.join(flink_log_dir, 'flink-setup.log')
flink_dir = config['configurations']['flink-env']['flink_hdfs_dir']
flink_checkpoints_dir = config['configurations']['flink-env']['flink_checkpoints_dir']
flink_recovery_dir = config['configurations']['flink-env']['flink_recovery_dir']
flink_kerberos_keytab = config['configurations']['flink-env'][
    'flink.keytab']
hostname = config["hostname"]
flink_kerberos_principal = config['configurations']['flink-env'][
    'flink.principal'].replace('_HOST', hostname.lower())

zk_url = []
for item in config['clusterHostInfo']['zookeeper_hosts']:
    zk_url.append(item + ':2181')
zookeeper_quorum = ",".join(zk_url)
zk_jaas_file = '/etc/zookeeper/zookeeper_client_jaas.conf'


hostname = config["hostname"]
hdfs_user = default("/configurations/hadoop-env/hdfs_user", 'hdfs')
hdfs_user_keytab = default("/configurations/hadoop-env/hdfs_user_keytab", None)
hdfs_principal_name = default('/configurations/hadoop-env/hdfs_principal_name',
                              'missing_principal').replace("_HOST", hostname)
security_enabled = default("/configurations/cluster-env/security_enabled",
                           False)
kinit_path_local = get_kinit_path(
    default('/configurations/kerberos-env/executable_search_paths', None))
hdfs_site = default("/configurations/hdfs-site", [])
default_fs = default("/configurations/core-site/fs.defaultFS", 'XMcluster')
dfs_type = default("/commandParams/dfs_type", "")
hadoop_bin_dir = default("/configurations/hadoop-env/install_dir",
                         '/opt/hadoop') + '/bin'

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
    dfs_type=dfs_type)
