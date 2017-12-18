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
from resource_management.libraries.functions.version import format_stack_version
from resource_management.libraries.functions.stack_features import get_stack_feature_version
from resource_management.libraries.functions.default import default
from resource_management.libraries.functions import get_kinit_path
from resource_management.core.logger import Logger
from utils import get_bare_principal

# server configurations
config = Script.get_config()
tmp_dir = Script.get_tmp_dir()
stack_root = '/opt'

retryAble = default("/commandParams/command_retry_enabled", False)

install_dir = config['configurations']['streamline-env']['install_dir']
download_url = config['configurations']['streamline-env']['download_url']
filename = download_url.split('/')[-1]
version_dir = filename.replace('.tar.gz', '').replace('.tgz', '')

# Version being upgraded/downgraded to
version = default("/commandParams/version", None)

# Version that is CURRENT.
current_version = default("/hostLevelParams/current_version", None)

stack_version_unformatted = config['hostLevelParams']['stack_version']
stack_version_formatted = format_stack_version(stack_version_unformatted)
upgrade_direction = default("/commandParams/upgrade_direction", None)
security_enabled = config['configurations']['cluster-env']['security_enabled']
kinit_path_local = get_kinit_path(default('/configurations/kerberos-env/executable_search_paths', None))

# get the correct version to use for checking stack features
version_for_stack_feature_checks = get_stack_feature_version(config)

# When downgrading the 'version' and 'current_version' are both pointing to the downgrade-target version
# downgrade_from_version provides the source-version the downgrade is happening from
downgrade_from_version = default("/commandParams/downgrade_from_version", None)

hostname = config['hostname']

# default streamline parameters
streamline_home = install_dir
streamline_bin = install_dir + '/bin/streamline'

streamline_managed_log_dir = os.path.join(streamline_home, "logs")
conf_dir = '/etc/streamline'

limits_conf_dir = "/etc/security/limits.d"

streamline_user_nofile_limit = default('/configurations/streamline-env/streamline_user_nofile_limit', 65536)
streamline_user_nproc_limit = default('/configurations/streamline-env/streamline_user_nproc_limit', 65536)

streamline_user = config['configurations']['streamline-env']['streamline_user']
streamline_log_dir = config['configurations']['streamline-env']['streamline_log_dir']
streamline_log_maxbackupindex = config['configurations']['streamline-log4j']['streamline_log_maxbackupindex']
streamline_log_maxfilesize = config['configurations']['streamline-log4j']['streamline_log_maxfilesize']
streamline_log_template = config['configurations']['streamline-log4j']['content']
streamline_log_template = streamline_log_template.replace('{{streamline_log_dir}}', streamline_log_dir)
streamline_log_template = streamline_log_template.replace('{{streamline_log_maxbackupindex}}',
                                                          streamline_log_maxbackupindex)
streamline_log_template = streamline_log_template.replace('{{streamline_log_maxfilesize}}',
                                                          ("%sMB" % streamline_log_maxfilesize))

# This is hardcoded on the streamline bash process lifecycle on which we have no control over
streamline_managed_pid_dir = "/var/run/streamline"
streamine_managed_log_dir = "/var/log/streamline"

user_group = config['configurations']['cluster-env']['user_group']
java64_home = config['hostLevelParams']['java_home']
streamline_env_sh_template = config['configurations']['streamline-env']['content']
streamline_jaas_conf_template = default("/configurations/streamline_jaas_conf/content", None)

if security_enabled:
    smokeuser = config['configurations']['cluster-env']['smokeuser']
    smokeuser_principal = config['configurations']['cluster-env']['smokeuser_principal_name']
    smoke_user_keytab = config['configurations']['cluster-env']['smokeuser_keytab']
    _hostname_lowercase = config['hostname'].lower()
    _streamline_principal_name = config['configurations']['streamline-env']['streamline_principal_name']
    streamline_jaas_principal = _streamline_principal_name.replace('_HOST', _hostname_lowercase)
    streamline_bare_principal = get_bare_principal(streamline_jaas_principal)
    streamline_keytab_path = config['configurations']['streamline-env']['streamline_keytab']
    streamline_ui_keytab_path = config['configurations']['streamline-env']['streamline_ui_keytab']
    _streamline_ui_jaas_principal_name = config['configurations']['streamline-env']['streamline_ui_principal_name']
    streamline_ui_jaas_principal = _streamline_ui_jaas_principal_name.replace('_HOST', _hostname_lowercase)
    streamline_kerberos_params = " -Dzookeeper.sasl.client=true -Dzookeeper.sasl.client.username=zookeeper -Dzookeeper.sasl.clientconfig=RegistryClient -Djava.security.auth.login.config=" + conf_dir + "/streamline_jaas.conf"
    streamline_servlet_filter = config['configurations']['streamline-common']['servlet.filter']
    streamline_servlet_kerberos_name_rules = config['configurations']['streamline-common']['kerberos.name.rules']
    streamline_servlet_token_validity = config['configurations']['streamline-common']['token.validity']
    streamline_authorizer_class = config['configurations']['streamline-common']['authorizer.class.name']
    streamline_admin_principals = config['configurations']['streamline-common']['admin.principals'].replace(
        "{{streamline_bare_principal}}", streamline_bare_principal)
    streamline_kinit_cmd = config['configurations']['streamline-common']['kinit.cmd']
    streamline_ticket_renew_window_factor = config['configurations']['streamline-common']['ticket.renew.window.factor']
    streamline_ticket_renew_jitter = config['configurations']['streamline-common']['ticket.renew.jitter']
    streamline_min_time_before_login = config['configurations']['streamline-common']['min.time.before.login']
else:
    streamline_kerberos_params = ''

# flatten streamline configs

storm_client_home = config['configurations']['streamline-common']['storm.client.home']
registry_url = config['configurations']['streamline-common']['registry.url']
maven_repo_url = config['configurations']['streamline-common']['maven.repo.url']
jar_storage_type = config['configurations']['streamline-common']['jar.storage.type']
jar_storage_hdfs_url = config['configurations']['streamline-common']['jar.storage.hdfs.url']
jar_storage = config['configurations']['streamline-common']['jar.storage']
jar_storage_class = "com.hortonworks.streamline.common.util.LocalFileSystemStorage"
jar_remote_storage_enabled = False

if jar_storage_type != None and jar_storage_type == "hdfs":
    jar_storage_class = "com.hortonworks.streamline.common.util.HdfsFileStorage"
    jar_remote_storage_enabled = True

if 'topology.test.results.dir' in config['configurations']['streamline-common']:
    topology_test_results = config['configurations']['streamline-common']['topology.test.results.dir']
else:
    topology_test_results = "/streamline/topology_test_results"

streamline_dashboard_url = config['configurations']['streamline-common']['streamline.dashboard.url']

streamline_storage_type = str(config['configurations']['streamline-common']['streamline.storage.type']).lower()
streamline_storage_connector_connectorURI = config['configurations']['streamline-common']['streamline.storage.connector.connectURI']
streamline_storage_connector_user = config['configurations']['streamline-common']['streamline.storage.connector.user']
streamline_storage_connector_password = config['configurations']['streamline-common'][
    'streamline.storage.connector.password']
streamline_storage_query_timeout = config['configurations']['streamline-common']['streamline.storage.query.timeout']
streamline_storage_java_class = "com.mysql.jdbc.jdbc2.optional.MysqlDataSource"

if streamline_storage_type == "postgresql":
    streamline_storage_java_class = "org.postgresql.ds.PGSimpleDataSource"

streamline_port = config['configurations']['streamline-common']['port']
streamline_admin_port = config['configurations']['streamline-common']['adminPort']

#Http Proxy Configs
if 'httpProxyServer' in config['configurations']['streamline-common']:
  http_proxy_server = config['configurations']['streamline-common']['httpProxyServer']
else:
  http_proxy_server = None

if 'httpProxyUsername' in config['configurations']['streamline-common']:
  http_proxy_username = config['configurations']['streamline-common']['httpProxyUsername']
else:
  http_proxy_username = None

if 'httpProxyPassword' in config['configurations']['streamline-common']:
  http_proxy_password = config['configurations']['streamline-common']['httpProxyPassword']
else:
  http_proxy_password = None

streamline_catalog_root_url = 'http://{0}:{1}/api/v1/catalog'.format(hostname,streamline_port)

# mysql jar
jdk_location = config['hostLevelParams']['jdk_location']
if 'mysql' == streamline_storage_type:
    jdbc_driver_jar = default("/hostLevelParams/custom_mysql_jdbc_name", None)
    if jdbc_driver_jar == None:
        Logger.error(
            "Failed to find mysql-java-connector jar. Make sure you followed the steps to register mysql driver")
        Logger.info("Users should register the mysql java driver jar.")
        Logger.info("yum install mysql-connector-java*")
        Logger.info("sudo ambari-server setup --jdbc-db=mysql --jdbc-driver=/usr/share/java/mysql-connector-java.jar")

    connector_curl_source = format("{jdk_location}/{jdbc_driver_jar}")
    connector_download_dir = format("{streamline_home}/libs")
    connector_bootstrap_download_dir = format("{streamline_home}/bootstrap/lib")
    downloaded_custom_connector = format("{tmp_dir}/{jdbc_driver_jar}")

check_db_connection_jar_name = "DBConnectionVerification.jar"
check_db_connection_jar = format("/usr/lib/ambari-agent/{check_db_connection_jar_name}")

# bootstrap commands

bootstrap_storage_command = os.path.join(streamline_home, "bootstrap", "bootstrap-storage.sh")
bootstrap_storage_run_cmd = format('source {conf_dir}/streamline-env.sh ; {bootstrap_storage_command}')

bootstrap_command = os.path.join(streamline_home, "bootstrap", "bootstrap.sh")
bootstrap_run_cmd = format('source {conf_dir}/streamline-env.sh ; {bootstrap_command}')

bootstrap_storage_file = "/var/lib/ambari-agent/data/streamline/bootstrap_storage_done"
bootstrap_file = "/var/lib/ambari-agent/data/streamline/bootstrap_done"
streamline_agent_dir = "/var/lib/ambari-agent/data/streamline"
cluster_name = config['clusterName']