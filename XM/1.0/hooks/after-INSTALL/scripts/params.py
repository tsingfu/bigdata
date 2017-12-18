"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

import os

from ambari_commons.constants import AMBARI_SUDO_BINARY
from resource_management.libraries.script import Script
from resource_management.libraries.functions import default
from resource_management.libraries.functions import format_jvm_option
from resource_management.libraries.functions.version import format_stack_version

config = Script.get_config()
tmp_dir = Script.get_tmp_dir()

dfs_type = default("/commandParams/dfs_type", "")

is_parallel_execution_enabled = int(
    default("/agentConfigParams/agent/parallel_execution", 0)) == 1
host_sys_prepped = default("/hostLevelParams/host_sys_prepped", False)

sudo = AMBARI_SUDO_BINARY

stack_version_unformatted = config['hostLevelParams']['stack_version']
stack_version_formatted = format_stack_version(stack_version_unformatted)

# current host stack version
current_version = default("/hostLevelParams/current_version", None)

# default hadoop params
if 'hadoop-env' in config['configurations']:
    mapreduce_libs_path = config['configurations']['hadoop-env'][
        'install_dir'] + '/share/hadoop/mapreduce/*'
    hadoop_libexec_dir = config['configurations']['hadoop-env'][
        'install_dir'] + '/libexec'
    hadoop_conf_empty_dir = config['configurations']['hadoop-env'][
        'install_dir']
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
    hdfs_user = config['configurations']['hadoop-env']['hdfs_user']

versioned_stack_root = '/opt'

#security params
security_enabled = config['configurations']['cluster-env']['security_enabled']

#java params
java_home = config['hostLevelParams']['java_home']

#hadoop params
hadoop_conf_dir = '/etc/hadoop'

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
user_group = config['configurations']['cluster-env']['user_group']

namenode_host = default("/clusterHostInfo/namenode_host", [])
has_namenode = not len(namenode_host) == 0

stack_select_lock_file = os.path.join(tmp_dir, "stack_lock_file")
