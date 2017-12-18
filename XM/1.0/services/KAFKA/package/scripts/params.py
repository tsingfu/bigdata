#!/usr/bin/env python
import os
from resource_management.libraries.functions import format
from resource_management.libraries.script.script import Script
from resource_management.libraries.functions.version import format_stack_version
from resource_management.libraries.functions.stack_features import get_stack_feature_version
from resource_management.libraries.functions.default import default
from utils import get_bare_principal
import status_params
from resource_management.libraries.resources.hdfs_resource import HdfsResource
from resource_management.libraries.functions import stack_select
from resource_management.libraries.functions import conf_select
from resource_management.libraries.functions import get_kinit_path
from resource_management.libraries.functions.get_not_managed_resources import get_not_managed_resources

# server configurations
config = Script.get_config()

install_dir = config['configurations']['kafka-env']['install_dir']
download_url = config['configurations']['kafka-env']['download_url']
filename = download_url.split('/')[-1]
# version_dir = filename[:-7]
version_dir = filename.replace('.tar.gz', '').replace('.tgz', '')

tmp_dir = Script.get_tmp_dir()
stack_root = '/opt'
stack_name = default("/hostLevelParams/stack_name", None)
retryAble = default("/commandParams/command_retry_enabled", False)

# Version being upgraded/downgraded to
version = default("/commandParams/version", None)

# Version that is CURRENT.
current_version = default("/hostLevelParams/current_version", None)

stack_version_unformatted = config['hostLevelParams']['stack_version']
stack_version_formatted = format_stack_version(stack_version_unformatted)
upgrade_direction = default("/commandParams/upgrade_direction", None)

# get the correct version to use for checking stack features
version_for_stack_feature_checks = get_stack_feature_version(config)

# When downgrading the 'version' and 'current_version' are both pointing to the downgrade-target version
# downgrade_from_version provides the source-version the downgrade is happening from
downgrade_from_version = default("/commandParams/downgrade_from_version", None)

hostname = config['hostname']

# default kafka parameters
kafka_home = install_dir
kafka_bin_dir = kafka_home + '/bin'
conf_dir = '/etc/kafka'
limits_conf_dir = "/etc/security/limits.d"

# Used while upgrading the stack in a kerberized cluster and running kafka-acls.sh
zookeeper_connect = default("/configurations/kafka-broker/zookeeper.connect",
                            None)

kafka_user_nofile_limit = default(
    '/configurations/kafka-env/kafka_user_nofile_limit', 1280000)
kafka_user_nproc_limit = default(
    '/configurations/kafka-env/kafka_user_nproc_limit', 65536)

kafka_user = config['configurations']['kafka-env']['kafka_user']
kafka_log_dir = config['configurations']['kafka-env']['kafka_log_dir']
kafka_pid_dir = status_params.kafka_pid_dir
kafka_pid_file = kafka_pid_dir + "/kafka.pid"
# This is hardcoded on the kafka bash process lifecycle on which we have no control over
kafka_managed_pid_dir = "/var/run/kafka"
kafka_managed_log_dir = "/data/log/kafka"
user_group = config['configurations']['cluster-env']['user_group']
java64_home = config['hostLevelParams']['java_home']
kafka_env_sh_template = config['configurations']['kafka-env']['content']
kafka_jaas_conf_template = default("/configurations/kafka_jaas_conf/content",None)

kafka_run_class_content_template = config['configurations']['kafka-env']['kafka-run-class-content']

kafka_client_jaas_conf_template = default(
    "/configurations/kafka_client_jaas_conf/content", None)
kafka_hosts = config['clusterHostInfo']['kafka_broker_hosts']
kafka_hosts.sort()

zookeeper_hosts = config['clusterHostInfo']['zookeeper_hosts']
zookeeper_hosts.sort()
secure_acls = default("/configurations/kafka-broker/zookeeper.set.acl", False)
kafka_security_migrator = os.path.join(kafka_home, "bin",
                                       "zookeeper-security-migration.sh")

# Kafka log4j
kafka_log_maxfilesize = default(
    '/configurations/kafka-log4j/kafka_log_maxfilesize', 256)
kafka_log_maxbackupindex = default(
    '/configurations/kafka-log4j/kafka_log_maxbackupindex', 20)
controller_log_maxfilesize = default(
    '/configurations/kafka-log4j/controller_log_maxfilesize', 256)
controller_log_maxbackupindex = default(
    '/configurations/kafka-log4j/controller_log_maxbackupindex', 20)

if (('kafka-log4j' in config['configurations']) and
        ('content' in config['configurations']['kafka-log4j'])):
    log4j_props = config['configurations']['kafka-log4j']['content']
else:
    log4j_props = None

if 'ganglia_server_host' in config['clusterHostInfo'] and \
                len(config['clusterHostInfo']['ganglia_server_host']) > 0:
    ganglia_installed = True
    ganglia_server = config['clusterHostInfo']['ganglia_server_host'][0]
    ganglia_report_interval = 60
else:
    ganglia_installed = False

metric_collector_port = ""
metric_collector_protocol = ""
metric_truststore_path = default(
    "/configurations/ams-ssl-client/ssl.client.truststore.location", "")
metric_truststore_type = default(
    "/configurations/ams-ssl-client/ssl.client.truststore.type", "")
metric_truststore_password = default(
    "/configurations/ams-ssl-client/ssl.client.truststore.password", "")

ams_collector_hosts = ",".join(
    default("/clusterHostInfo/metrics_collector_hosts", []))
has_metric_collector = not len(ams_collector_hosts) == 0

if has_metric_collector:
    if 'cluster-env' in config['configurations'] and \
                    'metrics_collector_vip_port' in config['configurations']['cluster-env']:
        metric_collector_port = config['configurations']['cluster-env'][
            'metrics_collector_vip_port']
    else:
        metric_collector_web_address = default(
            "/configurations/ams-site/timeline.metrics.service.webapp.address",
            "0.0.0.0:6188")
        if metric_collector_web_address.find(':') != -1:
            metric_collector_port = metric_collector_web_address.split(':')[1]
        else:
            metric_collector_port = '6188'
    if default("/configurations/ams-site/timeline.metrics.service.http.policy",
               "HTTP_ONLY") == "HTTPS_ONLY":
        metric_collector_protocol = 'https'
    else:
        metric_collector_protocol = 'http'
    pass

# Security-related params
security_enabled = config['configurations']['cluster-env']['security_enabled']
kafka_kerberos_enabled = (('security.inter.broker.protocol' in config['configurations']['kafka-broker']) and
                          ((config['configurations']['kafka-broker'][
                                'security.inter.broker.protocol'] == "PLAINTEXTSASL") or
                           (config['configurations']['kafka-broker'][
                                'security.inter.broker.protocol'] == "SASL_PLAINTEXT")))

if security_enabled and 'kafka_principal_name' in config['configurations']['kafka-env']:
    _hostname_lowercase = config['hostname'].lower()
    _kafka_principal_name = config['configurations']['kafka-env'][
        'kafka_principal_name']
    kafka_jaas_principal = _kafka_principal_name.replace('_HOST',
                                                         _hostname_lowercase)
    kafka_keytab_path = config['configurations']['kafka-env']['kafka_keytab']
    kafka_bare_jaas_principal = get_bare_principal(_kafka_principal_name)
    kafka_kerberos_params = " -Dzookeeper.sasl.client=true -Dzookeeper.sasl.client.username=zookeeper -Dzookeeper.sasl.clientconfig=Client -Djava.security.auth.login.config=" + conf_dir + "/kafka_jaas.conf"
else:
    kafka_kerberos_params = ''
    kafka_jaas_principal = None
    kafka_keytab_path = None

listeners = 'PLAINTEXT://' + hostname + ':6667'
if security_enabled:
    listeners = 'SASL_PLAINTEXT://' + hostname + ':6667'

jdk_location = config['hostLevelParams']['jdk_location']

namenode_hosts = default("/clusterHostInfo/namenode_host", [])
has_namenode = not len(namenode_hosts) == 0

hdfs_user = config['configurations']['hadoop-env'][
    'hdfs_user'] if has_namenode else None
hdfs_user_keytab = config['configurations']['hadoop-env'][
    'hdfs_user_keytab'] if has_namenode else None
hdfs_principal_name = config['configurations']['hadoop-env'][
    'hdfs_principal_name'] if has_namenode else None
hdfs_site = config['configurations']['hdfs-site'] if has_namenode else None
default_fs = config['configurations']['core-site'][
    'fs.defaultFS'] if has_namenode else None
hadoop_bin_dir = config['configurations']['hadoop-env'][
                     'install_dir'] + '/bin/' if has_namenode else None
hadoop_conf_dir = '/etc/hadoop' if has_namenode else None
kinit_path_local = get_kinit_path(
    default('/configurations/kerberos-env/executable_search_paths', None))

import functools

# create partial functions with common arguments for every HdfsResource call
# to create/delete hdfs directory/file/copyfromlocal we need to call params.HdfsResource in code
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
    immutable_paths=get_not_managed_resources())

# ranger kafka plugin section start
from resource_management.libraries.functions.setup_ranger_plugin_xml import get_audit_configs
from resource_management.libraries.functions.is_empty import is_empty

# ranger host
ranger_admin_hosts = default("/clusterHostInfo/ranger_admin_hosts", [])
has_ranger_admin = not len(ranger_admin_hosts) == 0

# ambari-server hostname
ambari_server_hostname = config['clusterHostInfo']['ambari_server_host'][0]

ranger_admin_log_dir = default("/configurations/ranger-env/ranger_admin_log_dir", "/var/log/ranger/admin")

# ranger kafka plugin enabled property
enable_ranger_kafka = default("configurations/ranger-kafka-plugin-properties/ranger-kafka-plugin-enabled", "No")
enable_ranger_kafka = True if enable_ranger_kafka.lower() == 'yes' else False

stack_supports_ranger_kerberos = True
stack_supports_ranger_audit_db = False
stack_supports_ranger_hive_jdbc_url_change = True
stack_supports_atlas_hook_for_hive_interactive = True
xml_configurations_supported = True

is_supported_kafka_ranger = True

# ranger kafka properties
if enable_ranger_kafka and is_supported_kafka_ranger:
    # get ranger policy url
    policymgr_mgr_url = config['configurations']['ranger-kafka-security']['ranger.plugin.kafka.policy.rest.url']

    if not is_empty(policymgr_mgr_url) and policymgr_mgr_url.endswith('/'):
        policymgr_mgr_url = policymgr_mgr_url.rstrip('/')

    # ranger audit db user
    xa_audit_db_user = default('/configurations/admin-properties/audit_db_user', 'rangerlogger')

    xa_audit_db_password = ''
    if not is_empty(config['configurations']['admin-properties'][
                        'audit_db_password']) and stack_supports_ranger_audit_db and has_ranger_admin:
        xa_audit_db_password = config['configurations']['admin-properties']['audit_db_password']

    # ranger kafka service/repository name
    repo_name = str(config['clusterName']) + '_kafka'
    repo_name_value = config['configurations']['ranger-kafka-security']['ranger.plugin.kafka.service.name']
    if not is_empty(repo_name_value) and repo_name_value != "{{repo_name}}":
        repo_name = repo_name_value

    ranger_env = config['configurations']['ranger-env']

    # create ranger-env config having external ranger credential properties
    if not has_ranger_admin and enable_ranger_kafka:
        external_admin_username = default('/configurations/ranger-env/admin_username', 'admin')
        external_admin_password = default('/configurations/ranger-env/admin_password', 'admin')
        external_ranger_admin_username = default('/configurations/ranger-env/ranger_admin_username', 'ranger_admin')
        external_ranger_admin_password = default('/configurations/ranger-env/ranger_admin_password', 'example.com')
        ranger_env = {}
        ranger_env['admin_username'] = external_admin_username
        ranger_env['admin_password'] = external_admin_password
        ranger_env['ranger_admin_username'] = external_ranger_admin_username
        ranger_env['ranger_admin_password'] = external_ranger_admin_password

    ranger_plugin_properties = config['configurations']['ranger-kafka-plugin-properties']
    ranger_kafka_audit = config['configurations']['ranger-kafka-audit']
    ranger_kafka_audit_attrs = config['configuration_attributes']['ranger-kafka-audit']
    ranger_kafka_security = config['configurations']['ranger-kafka-security']
    ranger_kafka_security_attrs = config['configuration_attributes']['ranger-kafka-security']
    ranger_kafka_policymgr_ssl = config['configurations']['ranger-kafka-policymgr-ssl']
    ranger_kafka_policymgr_ssl_attrs = config['configuration_attributes']['ranger-kafka-policymgr-ssl']

    policy_user = config['configurations']['ranger-kafka-plugin-properties']['policy_user']

    ranger_plugin_config = {
        'username': config['configurations']['ranger-env']['admin_username'],
        'password': config['configurations']['ranger-env']['admin_password'],
        'zookeeper.connect': config['configurations']['ranger-kafka-plugin-properties']['zookeeper.connect'],
        'commonNameForCertificate': config['configurations']['ranger-kafka-plugin-properties'][
            'common.name.for.certificate']
    }

    kafka_ranger_plugin_repo = {
        'isEnabled': 'true',
        'configs': ranger_plugin_config,
        'description': 'kafka repo',
        'name': repo_name,
        'repositoryType': 'kafka',
        'type': 'kafka',
        'assetType': '1'
    }

    if stack_supports_ranger_kerberos and security_enabled:
        ranger_plugin_config['policy.download.auth.users'] = kafka_user
        ranger_plugin_config['tag.download.auth.users'] = kafka_user
        ranger_plugin_config['ambari.service.check.user'] = policy_user

    downloaded_custom_connector = None
    previous_jdbc_jar_name = None
    driver_curl_source = None
    driver_curl_target = None
    previous_jdbc_jar = None

    if has_ranger_admin and stack_supports_ranger_audit_db:
        xa_audit_db_flavor = config['configurations']['admin-properties']['DB_FLAVOR']
        jdbc_jar_name, previous_jdbc_jar_name, audit_jdbc_url, jdbc_driver = get_audit_configs(config)

        downloaded_custom_connector = format("{tmp_dir}/{jdbc_jar_name}") if stack_supports_ranger_audit_db else None
        driver_curl_source = format("{jdk_location}/{jdbc_jar_name}") if stack_supports_ranger_audit_db else None
        driver_curl_target = format("{kafka_home}/libs/{jdbc_jar_name}") if stack_supports_ranger_audit_db else None
        previous_jdbc_jar = format(
            "{kafka_home}/libs/{previous_jdbc_jar_name}") if stack_supports_ranger_audit_db else None

    xa_audit_db_is_enabled = False
    if xml_configurations_supported and stack_supports_ranger_audit_db:
        xa_audit_db_is_enabled = config['configurations']['ranger-kafka-audit']['xasecure.audit.destination.db']

    xa_audit_hdfs_is_enabled = default('/configurations/ranger-kafka-audit/xasecure.audit.destination.hdfs', False)
    ssl_keystore_password = config['configurations']['ranger-kafka-policymgr-ssl'][
        'xasecure.policymgr.clientssl.keystore.password'] if xml_configurations_supported else None
    ssl_truststore_password = config['configurations']['ranger-kafka-policymgr-ssl'][
        'xasecure.policymgr.clientssl.truststore.password'] if xml_configurations_supported else None
    credential_file = format('/etc/ranger/{repo_name}/cred.jceks')

    setup_ranger_env_sh_source = format(
        '{stack_root}/ranger-kafka-plugin/install/conf.templates/enable/kafka-ranger-env.sh')
    setup_ranger_env_sh_target = format("{conf_dir}/kafka-ranger-env.sh")

    # for SQLA explicitly disable audit to DB for Ranger
    if has_ranger_admin and stack_supports_ranger_audit_db and xa_audit_db_flavor.lower() == 'sqla':
        xa_audit_db_is_enabled = False

# need this to capture cluster name from where ranger kafka plugin is enabled
cluster_name = config['clusterName']

# ranger kafka plugin section end
