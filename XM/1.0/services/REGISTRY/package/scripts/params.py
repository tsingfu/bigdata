#!/usr/bin/env python
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
from resource_management.libraries.functions import format
from resource_management.libraries.script.script import Script
from resource_management.libraries.functions.stack_features import get_stack_feature_version
from resource_management.libraries.functions.default import default
from resource_management.libraries.functions import get_kinit_path

# server configurations
config = Script.get_config()
tmp_dir = Script.get_tmp_dir()
stack_root = '/opt'
retryAble = default("/commandParams/command_retry_enabled", False)

install_dir = config['configurations']['registry-env']['install_dir']
download_url = config['configurations']['registry-env']['download_url']
filename = download_url.split('/')[-1]
version_dir = filename.replace('.tar.gz', '').replace('.tgz', '')

# Version being upgraded/downgraded to
version = default("/commandParams/version", None)

# Version that is CURRENT.
current_version = default("/hostLevelParams/current_version", None)

upgrade_direction = default("/commandParams/upgrade_direction", None)
security_enabled = config['configurations']['cluster-env']['security_enabled']
kinit_path_local = get_kinit_path(default('/configurations/kerberos-env/executable_search_paths', None))
smokeuser = config['configurations']['cluster-env']['smokeuser']
smokeuser_principal = config['configurations']['cluster-env']['smokeuser_principal_name']
smoke_user_keytab = config['configurations']['cluster-env']['smokeuser_keytab']

# get the correct version to use for checking stack features
version_for_stack_feature_checks = get_stack_feature_version(config)

# When downgrading the 'version' and 'current_version' are both pointing to the downgrade-target version
# downgrade_from_version provides the source-version the downgrade is happening from
downgrade_from_version = default("/commandParams/downgrade_from_version", None)

hostname = config['hostname']

# default registry parameters
registry_home = install_dir
registry_bin = os.path.join(registry_home, "bin", "registry")

registry_managed_log_dir = os.path.join(registry_home, "logs")
conf_dir = '/etc/registry'

limits_conf_dir = "/etc/security/limits.d"

registry_user_nofile_limit = default('/configurations/registry-env/registry_user_nofile_limit', 65536)
registry_user_nproc_limit = default('/configurations/registry-env/registry_user_nproc_limit', 65536)

registry_user = config['configurations']['registry-env']['registry_user']
registry_log_dir = config['configurations']['registry-env']['registry_log_dir']

# This is hardcoded on the registry bash process lifecycle on which we have no control over
registry_managed_pid_dir = "/var/run/registry"
streamine_managed_log_dir = "/var/log/registry"

user_group = config['configurations']['cluster-env']['user_group']
java64_home = config['hostLevelParams']['java_home']
registry_env_sh_template = config['configurations']['registry-env']['content']
registry_jaas_conf_template = default("/configurations/registry-env/registry_jaas", None)

if security_enabled:
    _hostname_lowercase = config['hostname'].lower()
    registry_ui_keytab_path = config['configurations']['registry-env']['registry_ui_keytab']
    _registry_ui_jaas_principal_name = config['configurations']['registry-env']['registry_ui_principal_name']
    registry_ui_jaas_principal = _registry_ui_jaas_principal_name.replace('_HOST', _hostname_lowercase)
    registry_kerberos_params = " -Dzookeeper.sasl.client=true -Dzookeeper.sasl.client.username=zookeeper -Dzookeeper.sasl.clientconfig=Client -Djava.security.auth.login.config=" + conf_dir + "/registry_jaas.conf"
    registry_servlet_filter = config['configurations']['registry-common']['servlet.filter']
    registry_servlet_kerberos_name_rules = config['configurations']['registry-common']['kerberos.name.rules']
    # registry_servlet_token_validity = (config['configurations']['registry-common']['token.validity'])
    registry_servlet_token_validity = 36000

registry_log_maxbackupindex = config['configurations']['registry-log4j']['registry_log_maxbackupindex']
registry_log_maxfilesize = config['configurations']['registry-log4j']['registry_log_maxfilesize']
registry_log_template = config['configurations']['registry-log4j']['content']
registry_log_template = registry_log_template.replace('{{registry_log_dir}}', registry_log_dir)
registry_log_template = registry_log_template.replace('{{registry_log_maxbackupindex}}', registry_log_maxbackupindex)
registry_log_template = registry_log_template.replace('{{registry_log_maxfilesize}}',
                                                      ("%sMB" % registry_log_maxfilesize))

# flatten registry configs
jar_storage = config['configurations']['registry-common']['jar.storage']
registry_storage_type = str(config['configurations']['registry-common']['registry.storage.type']).lower()
registry_storage_connector_connectorURI = config['configurations']['registry-common']['registry.storage.connector.connectURI']
registry_storage_connector_user = config['configurations']['registry-common']['registry.storage.connector.user']
registry_storage_connector_password = config['configurations']['registry-common']['registry.storage.connector.password']
registry_storage_query_timeout = config['configurations']['registry-common']['registry.storage.query.timeout']
registry_storage_java_class = "com.mysql.jdbc.jdbc2.optional.MysqlDataSource"

jar_storage_type = config['configurations']['registry-common']['jar.storage.type']
jar_storage_hdfs_url = config['configurations']['registry-common']['jar.storage.hdfs.url']
jar_storage_class = "com.hortonworks.registries.common.util.LocalFileSystemStorage"
jar_remote_storage_enabled = False

if jar_storage_type != None and jar_storage_type == "hdfs":
    jar_storage_class = "com.hortonworks.registries.common.util.HdfsFileStorage"
    jar_remote_storage_enabled = True

if registry_storage_type == "postgresql":
    registry_storage_java_class = "org.postgresql.ds.PGSimpleDataSource"

registry_port = config['configurations']['registry-common']['port']
registry_admin_port = config['configurations']['registry-common']['adminPort']

registry_schema_cache_size = config['configurations']['registry-common']['registry.schema.cache.size']
registry_schema_cache_expiry_interval = config['configurations']['registry-common'][
    'registry.schema.cache.expiry.interval']

# mysql jar
jdk_location = config['hostLevelParams']['jdk_location']

check_db_connection_jar_name = "DBConnectionVerification.jar"
check_db_connection_jar = format("/usr/lib/ambari-agent/{check_db_connection_jar_name}")

# bootstrap commands
bootstrap_sh_template = config['configurations']['registry-env']['bootstrap']
bootstrap_storage_command = os.path.join(registry_home, "bootstrap", "bootstrap-storage.sh")
bootstrap_storage_run_cmd = format('source {conf_dir}/registry-env.sh ; {bootstrap_storage_command}')
bootstrap_storage_file = "/var/lib/ambari-agent/data/registry/bootstrap_storage_done"
registry_agent_dir = "/var/lib/ambari-agent/data/registry"
cluster_name = config['clusterName']