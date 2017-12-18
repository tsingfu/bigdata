#!/usr/bin/config python
from resource_management import *
from resource_management.libraries.functions.default import default
from resource_management.libraries.functions.format import format
from resource_management.libraries.functions.get_bare_principal import get_bare_principal
from resource_management.libraries.script.script import Script

config = Script.get_config()
tmp_dir = Script.get_tmp_dir()

# identify archive file
alluxio_archive_file = config['configurations']['alluxio-env']['archive_file']

# alluxio master address
alluxio_master = config['clusterHostInfo']['alluxio_master_hosts']

# alluxio underfs address
underfs_addr = config['configurations']['alluxio-env']['underfs_address']

# alluxio worker memory alotment 
worker_mem = config['configurations']['alluxio-env']['worker_memory']

# hadoop params
namenode_address = None
if 'dfs.namenode.rpc-address' in config['configurations']['hdfs-site']:
    namenode_rpcaddress = config['configurations']['hdfs-site']['dfs.namenode.rpc-address']
    namenode_address = format("hdfs://{namenode_rpcaddress}")
else:
    namenode_address = config['configurations']['core-site']['fs.defaultFS']

# alluxio log dir
log_dir = config['configurations']['alluxio-env']['log_dir']

# alluxio log dir
pid_dir = config['configurations']['alluxio-env']['pid_dir']
alluxio_user = config['configurations']['alluxio-env']['alluxio_user']

user_group = config['configurations']['cluster-env']['user_group']
alluxio_config_dir = '/etc/alluxio'
install_dir = config['configurations']['alluxio-env']['install_dir']
download_url = config['configurations']['alluxio-env']['download_url']
filename = download_url.split('/')[-1]
version_dir = filename.replace('.tar.gz', '').replace('.tgz', '')
alluxio_libexec_dir = install_dir + '/libexec/'

env_sh_template = config['configurations']['alluxio-env']['content']
master_pid_file = pid_dir + "/alluxio_master.pid"
work_pid_file = pid_dir + '/alluxio_worker.pid'

java64_home = config['hostLevelParams']['java_home']

cluster_name = config['clusterName']