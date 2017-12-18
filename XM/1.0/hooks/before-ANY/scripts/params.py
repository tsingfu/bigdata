
import collections
import re
import os
import ast

import ambari_simplejson as json  # simplejson is much faster comparing to Python 2.6 json module and has the same functions set.

from resource_management.libraries.script import Script
from resource_management.libraries.functions import default
from resource_management.libraries.functions import format
from resource_management.libraries.functions import format_jvm_option
from resource_management.libraries.functions.is_empty import is_empty
from resource_management.libraries.functions.version import format_stack_version
from resource_management.libraries.functions.expect import expect
from resource_management.libraries.functions.stack_features import get_stack_feature_version
from ambari_commons.constants import AMBARI_SUDO_BINARY

config = Script.get_config()
tmp_dir = Script.get_tmp_dir()

dfs_type = default("/commandParams/dfs_type", "")

artifact_dir = format("{tmp_dir}/AMBARI-artifacts/")
jdk_name = default("/hostLevelParams/jdk_name", 'jdk.tar.gz')
java_home = config['hostLevelParams']['java_home']
java_version = expect("/hostLevelParams/java_version", int)
jdk_location = config['hostLevelParams']['jdk_location']

sudo = AMBARI_SUDO_BINARY

ambari_server_hostname = config['clusterHostInfo']['ambari_server_host'][0]

stack_version_unformatted = config['hostLevelParams']['stack_version']
stack_version_formatted = format_stack_version(stack_version_unformatted)

version = default("/commandParams/version", None)

security_enabled = config['configurations']['cluster-env']['security_enabled']

# Some datanode settings
dfs_dn_addr = default('/configurations/hdfs-site/dfs.datanode.address', None)
dfs_dn_http_addr = default(
    '/configurations/hdfs-site/dfs.datanode.http.address', None)
dfs_dn_https_addr = default(
    '/configurations/hdfs-site/dfs.datanode.https.address', None)
dfs_http_policy = default('/configurations/hdfs-site/dfs.http.policy', None)
secure_dn_ports_are_in_use = False
agent_stack_retry_on_unavailability = config['hostLevelParams'][
    'agent_stack_retry_on_unavailability']
agent_stack_retry_count = expect("/hostLevelParams/agent_stack_retry_count",
                                 int)

def get_port(address):
    """
  Extracts port from the address like 0.0.0.0:1019
  """
    if address is None:
        return None
    m = re.search(r'(?:http(?:s)?://)?([\w\d.]*):(\d{1,5})', address)
    if m is not None:
        return int(m.group(2))
    else:
        return None


def is_secure_port(port):
    """
  Returns True if port is root-owned at *nix systems
  """
    if port is not None:
        return port < 1024
    else:
        return False


if 'hadoop-env' in config['configurations']:
    # hadoop default params
    hdfs_user = config['configurations']['hadoop-env']['hdfs_user']
    mapreduce_libs_path = config['configurations']['hadoop-env'][
        'install_dir'] + '/share/hadoop/mapreduce/*'

    hdfs_user_nofile_limit = default(
        "/configurations/hadoop-env/hdfs_user_nofile_limit", "1280000")
    hadoop_home = config['configurations']['hadoop-env']['install_dir']
    hadoop_libexec_dir = config['configurations']['hadoop-env'][
        'install_dir'] + '/libexec'

    hadoop_conf_empty_dir = None
    hadoop_secure_dn_user = hdfs_user
    hadoop_dir = config['configurations']['hadoop-env']['install_dir']
    #hadoop params
    hdfs_log_dir_prefix = config['configurations']['hadoop-env'][
        'hdfs_log_dir_prefix']
    hadoop_pid_dir_prefix = config['configurations']['hadoop-env'][
        'hadoop_pid_dir_prefix']
    hadoop_root_logger = config['configurations']['hadoop-env'][
        'hadoop_root_logger']
    hadoop_heapsize = config['configurations']['hadoop-env']['hadoop_heapsize']
    namenode_heapsize = config['configurations']['hadoop-env'][
        'namenode_heapsize']
    namenode_opt_newsize = config['configurations']['hadoop-env'][
        'namenode_opt_newsize']
    namenode_opt_maxnewsize = config['configurations']['hadoop-env'][
        'namenode_opt_maxnewsize']
    dtnode_heapsize = config['configurations']['hadoop-env']['dtnode_heapsize']
    nfsgateway_heapsize = config['configurations']['hadoop-env'][
        'nfsgateway_heapsize']
    hadoop_env_sh_template = config['configurations']['hadoop-env']['content']

versioned_stack_root = '/opt'
hadoop_java_io_tmpdir = os.path.join(tmp_dir, "hadoop_java_io_tmpdir")
if 'hdfs-site' in config['configurations']:
    datanode_max_locked_memory = config['configurations']['hdfs-site'][
        'dfs.datanode.max.locked.memory']
    is_datanode_max_locked_memory_set = not is_empty(config['configurations'][
        'hdfs-site']['dfs.datanode.max.locked.memory'])
    dfs_cluster_administrators_group = config['configurations']['hdfs-site'][
        "dfs.cluster.administrators"]

if not security_enabled:
    hadoop_secure_dn_user = '""'
else:
    dfs_dn_port = get_port(dfs_dn_addr)
    dfs_dn_http_port = get_port(dfs_dn_http_addr)
    dfs_dn_https_port = get_port(dfs_dn_https_addr)
    # We try to avoid inability to start datanode as a plain user due to usage of root-owned ports
    if dfs_http_policy == "HTTPS_ONLY":
        secure_dn_ports_are_in_use = is_secure_port(
            dfs_dn_port) or is_secure_port(dfs_dn_https_port)
    elif dfs_http_policy == "HTTP_AND_HTTPS":
        secure_dn_ports_are_in_use = is_secure_port(
            dfs_dn_port) or is_secure_port(dfs_dn_http_port) or is_secure_port(
                dfs_dn_https_port)
    else:  # params.dfs_http_policy == "HTTP_ONLY" or not defined:
        secure_dn_ports_are_in_use = is_secure_port(
            dfs_dn_port) or is_secure_port(dfs_dn_http_port)
    if secure_dn_ports_are_in_use:
        hadoop_secure_dn_user = hdfs_user
    else:
        hadoop_secure_dn_user = '""'

namenode_opt_permsize = format_jvm_option(
    "/configurations/hadoop-env/namenode_opt_permsize", "128m")
namenode_opt_maxpermsize = format_jvm_option(
    "/configurations/hadoop-env/namenode_opt_maxpermsize", "256m")

jtnode_opt_newsize = "200m"
jtnode_opt_maxnewsize = "200m"
jtnode_heapsize = "1024m"
ttnode_heapsize = "1024m"

mapred_pid_dir_prefix = default(
    "/configurations/mapred-env/mapred_pid_dir_prefix",
    "/var/run/hadoop-mapreduce")
mapred_log_dir_prefix = default(
    "/configurations/mapred-env/mapred_log_dir_prefix",
    "/var/log/hadoop-mapreduce")

#users and groups
if 'hbase-env' in config['configurations']:
    hbase_user = config['configurations']['hbase-env']['hbase_user']

smoke_user = config['configurations']['cluster-env']['smokeuser']

user_group = config['configurations']['cluster-env']['user_group']

namenode_host = default("/clusterHostInfo/namenode_host", [])
hbase_master_hosts = default("/clusterHostInfo/hbase_master_hosts", [])

# get the correct version to use for checking stack features
version_for_stack_feature_checks = get_stack_feature_version(config)

has_namenode = not len(namenode_host) == 0

has_hbase_masters = not len(hbase_master_hosts) == 0
stack_supports_zk_security = True

# HDFS High Availability properties
dfs_ha_enabled = False
dfs_ha_nameservices = default(
    '/configurations/hdfs-site/dfs.internal.nameservices', None)
if dfs_ha_nameservices is None:
    dfs_ha_nameservices = default('/configurations/hdfs-site/dfs.nameservices',
                                  None)
dfs_ha_namenode_ids = default(
    format("/configurations/hdfs-site/dfs.ha.namenodes.{dfs_ha_nameservices}"),
    None)
if dfs_ha_namenode_ids:
    dfs_ha_namemodes_ids_list = dfs_ha_namenode_ids.split(",")
    dfs_ha_namenode_ids_array_len = len(dfs_ha_namemodes_ids_list)
    if dfs_ha_namenode_ids_array_len > 1:
        dfs_ha_enabled = True

hadoop_conf_dir = '/etc/hadoop'

hadoop_conf_secure_dir = os.path.join(hadoop_conf_dir, "secure")

hbase_tmp_dir = "/tmp/hbase-hbase"

proxyuser_group = default("/configurations/hadoop-env/proxyuser_group",
                          "users")

sysprep_skip_create_users_and_groups = default(
    "/configurations/cluster-env/sysprep_skip_create_users_and_groups", False)
ignore_groupsusers_create = default(
    "/configurations/cluster-env/ignore_groupsusers_create", False)
fetch_nonlocal_groups = config['configurations']['cluster-env'][
    "fetch_nonlocal_groups"]

smoke_user_dirs = format(
    "/tmp/hadoop-{smoke_user},/tmp/hsperfdata_{smoke_user},/home/{smoke_user},/tmp/{smoke_user}")
if has_hbase_masters:
    hbase_user_dirs = format(
        "/home/{hbase_user},/tmp/{hbase_user},/usr/bin/{hbase_user},/var/log/{hbase_user},{hbase_tmp_dir}")
#repo params
repo_info = config['hostLevelParams']['repo_info']
service_repo_info = default("/hostLevelParams/service_repo_info", None)

user_to_groups_dict = collections.defaultdict(lambda: [user_group])
user_to_groups_dict[smoke_user] = [proxyuser_group]

ranger_user = default("/configurations/ranger-env/ranger_user", 'ranger')
ranger_group = default("/configurations/ranger-env/ranger_group", 'ranger')
ranger_admin_hosts = default("/clusterHostInfo/ranger_admin_hosts", [])
has_ranger_admin = not len(ranger_admin_hosts) == 0

falcon_user = default("/configurations/falcon-env/falcon_user", 'falcon')
falcon_server_hosts = default("/clusterHostInfo/falcon_server_hosts", [])
has_falcon_server_hosts = not len(falcon_server_hosts) == 0

if has_falcon_server_hosts:
    user_to_groups_dict[falcon_user] = [proxyuser_group]
if has_ranger_admin:
    user_to_groups_dict[ranger_user] = [ranger_group]

#Append new user-group mapping to the dict
try:
    user_group_map = ast.literal_eval(config['hostLevelParams']['user_group'])
    for key in user_group_map.iterkeys():
        user_to_groups_dict[key] = user_group_map[key]
except ValueError:
    print('User Group mapping (user_group) is missing in the hostLevelParams')
user_to_gid_dict = collections.defaultdict(lambda: user_group)

user_list = json.loads(config['hostLevelParams']['user_list'])
group_list = json.loads(config['hostLevelParams']['group_list'])
host_sys_prepped = default("/hostLevelParams/host_sys_prepped", False)

override_uid = str(default("/configurations/cluster-env/override_uid",
                           "true")).lower()

# if NN HA on secure clutser, access Zookeper securely
if stack_supports_zk_security and dfs_ha_enabled and security_enabled:
    hadoop_zkfc_opts = format(
        "-Dzookeeper.sasl.client=true -Dzookeeper.sasl.client.username=zookeeper -Djava.security.auth.login.config={hadoop_conf_secure_dir}/hdfs_jaas.conf -Dzookeeper.sasl.clientconfig=Client")
