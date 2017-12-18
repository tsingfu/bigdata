from resource_management.libraries.script.script import Script
from resource_management.libraries.resources.hdfs_resource import HdfsResource
from resource_management.libraries.functions.format import format
from resource_management.libraries.functions.version import format_stack_version
from resource_management.libraries.functions.default import default
from resource_management.libraries.functions import get_kinit_path

config = Script.get_config()
tmp_dir = Script.get_tmp_dir()
stack_root = '/opt'

stack_name = default("/hostLevelParams/stack_name", None)

stack_version_unformatted = str(config['hostLevelParams']['stack_version'])
stack_version_formatted = format_stack_version(stack_version_unformatted)
major_stack_version = '1.0'
full_stack_version = '1.0.0'

install_dir = config['configurations']['titan-env']['install_dir']
download_url = config['configurations']['titan-env']['download_url']
filename = download_url.split('/')[-1]
version_dir = filename[:-7]

# New Cluster Stack Version that is defined during the RESTART of a Rolling Upgrade
version = default("/commandParams/version", None)

titan_user = config['configurations']['titan-env']['titan_user']
user_group = config['configurations']['cluster-env']['user_group']
titan_log_dir = config['configurations']['titan-env']['titan_log_dir']
titan_server_port = config['configurations']['titan-env']['titan_server_port']
titan_hdfs_home_dir = config['configurations']['titan-env']['titan_hdfs_home_dir']
titan_log_file = format("{titan_log_dir}/titan-{titan_server_port}.log")
titan_err_file = format("{titan_log_dir}/titan-{titan_server_port}.err")

smokeuser = config['configurations']['cluster-env']['smokeuser']
smokeuser_principal = config['configurations']['cluster-env']['smokeuser_principal_name']

security_enabled = config['configurations']['cluster-env']['security_enabled']
smoke_user_keytab = config['configurations']['cluster-env']['smokeuser_keytab']

if security_enabled:
    _hostname_lowercase = config['hostname'].lower()
    titan_jaas_princ = config['configurations']['titan-env']['titan_principal_name'].replace('_HOST',
                                                                                             _hostname_lowercase)
    titan_keytab_path = config['configurations']['titan-env']['titan_keytab_path']

titan_bin_dir = format('{install_dir}/bin')
titan_data_dir = '/data/titan'
# titan configurations
titan_conf_dir = '/etc/titan'  # format('{install_dir}/conf')
titan_hbase_solr_props = config['configurations']['titan-hbase-solr']['content']
titan_env_props = config['configurations']['titan-env']['content']
log4j_console_props = config['configurations']['titan-log4j']['content']

# titan server configurations
titan_server_conf_dir = format('{install_dir}/conf/gremlin-server')
gremlin_server_configs = config['configurations']['gremlin-server']['content']

titan_server_sasl = str(config['configurations']['titan-env']['SimpleAuthenticator']).lower()
titan_server_simple_authenticator = ""
if titan_server_sasl == "true" and 'knox-env' not in config['configurations']:
    titan_server_simple_authenticator = """authentication: {
  className: org.apache.tinkerpop.gremlin.server.auth.SimpleAuthenticator,
  config: {
    credentialsDb: conf/tinkergraph-empty.properties,
    credentialsDbLocation: data/credentials.kryo}}"""

titan_server_ssl = str(config['configurations']['titan-env']['ssl.enabled']).lower()
titan_server_ssl_key_cert_file = default('/configurations/titan-env/ssl.keyCertChainFile', None)
if titan_server_ssl_key_cert_file:
    titan_server_ssl_key_cert_file = format(", keyCertChainFile: {titan_server_ssl_key_cert_file}")
titan_server_ssl_key_file = default('/configurations/titan-env/ssl.keyFile', None)
if titan_server_ssl_key_file:
    titan_server_ssl_key_file = format(", keyFile: {titan_server_ssl_key_file}")
titan_server_ssl_key_password = default('/configurations/titan-env/ssl.keyPassword', None)
if titan_server_ssl_key_password:
    titan_server_ssl_key_password = format(", keyPassword: {titan_server_ssl_key_password}")
titan_server_ssl_trust_cert_chain_file = default('/configurations/titan-env/ssl.trustCertChainFile', None)
# not supporting 32 bit jdk.
java64_home = config['hostLevelParams']['java_home']
hadoop_config_dir = '/etc/hadoop'
hbase_config_dir = '/etc/hbase'

# Titan SparkGraphComputer configuration
yarn_home_dir = '/opt/hadoop'
spark_home_dir = '/opt/spark'
spark_config_dir = '/etc/spark2'
titan_home_dir = format('{install_dir}')
titan_conf_hadoop_graph_dir = format('{install_dir}/conf/hadoop-graph')
hadoop_lib_native_dir = '/opt/hadoop/lib/native'
titan_hadoop_gryo_props = config['configurations']['hadoop-gryo']['content']
hadoop_hbase_read_props = config['configurations']['hadoop-hbase-read']['content']
titan_hdfs_data_dir = "/user/titan/data"
titan_hdfs_spark_lib_dir = "/user/spark/share/lib/spark"
titan_ext_spark_plugin_dir = format('{install_dir}/ext/spark-client/plugin')
titan_spark2_archive_dir = format('/apps/spark2')
titan_spark2_archive_file = format('spark2-yarn-archive.tar.gz')
local_components = default("/localComponents", [])
yarn_client_installed = ('YARN_CLIENT' in local_components)
hbase_master_installed = ('HBASE_CLIENT' in local_components)

# Titan required 'storage.hostname' which is hbase cluster in IOP 4.2.
# The host name should be zooKeeper quorum
storage_hosts = config['clusterHostInfo']['zookeeper_hosts']
zookeeper_port = config['configurations']['zoo.cfg']['clientPort']
storage_host_list = []
titan_zookeeper_solr_host_list = []
for hostname in storage_hosts:
    titan_zookeeper_solr_hostname = hostname + format(':{zookeeper_port}/solr')
    titan_zookeeper_solr_host_list.append(titan_zookeeper_solr_hostname)
    storage_host_list.append(hostname)
storage_host = ",".join(storage_host_list)
zookeeper_solr_for_titan_hostname = ",".join(titan_zookeeper_solr_host_list)
hbase_zookeeper_parent = config['configurations']['hbase-site']['zookeeper.znode.parent']

if 'titan_server_hosts' in config['clusterHostInfo'] and len(config['clusterHostInfo']['titan_server_hosts']) > 0:
    titan_host = config['clusterHostInfo']['titan_server_hosts'][0]

# jts jar should be copy to solr site
titan_dir = format('{install_dir}')
titan_ext_dir = format('{install_dir}/ext')
titan_solr_conf_dir = format('{install_dir}/conf/solr')
titan_solr_jar_file = format('{install_dir}/lib/jts-1.13.jar')
# jaas file for solr when security is enabled
titan_solr_jaas_file = format('{titan_solr_conf_dir}/titan_solr_jaas.conf')
titan_solr_client_jaas_file = format('{titan_solr_conf_dir}/titan_solr_client_jaas.conf')
titan_solr_client_jaas_config = "index.search.solr.jaas-file=" + format(
    '{titan_solr_conf_dir}/titan_solr_client_jaas.conf')
if not security_enabled:
    titan_solr_client_jaas_config = ""
# config for solr collection creation
index = 0
zookeeper_quorum = ""
for host in config['clusterHostInfo']['zookeeper_hosts']:
    zookeeper_quorum += host + ":" + str(zookeeper_port)
    index += 1
    if index < len(config['clusterHostInfo']['zookeeper_hosts']):
        zookeeper_quorum += ","
if "solr-env" in config['configurations']:
    solr_znode = default('/configurations/solr-env/solr_znode', '/solr')
infra_solr_hosts = default("/clusterHostInfo/infra_solr_hosts", [])
infra_solr_replication_factor = 2 if len(infra_solr_hosts) > 1 else 1
titan_solr_shards = 1
titan_solr_hdfs_dir = "/apps/titan"
titan_solr_hdfs_conf_dir = "/apps/titan/conf"
titan_solr_hdfs_jar = "/apps/titan/jts-1.13.jar"
titan_tmp_dir = format('{tmp_dir}/titan')
titan_solr_dir = format('{titan_tmp_dir}/solr_installed')
configuration_tags = config['configurationTags']
hdfs_user = config['configurations']['hadoop-env']['hdfs_user']
titan_hdfs_mode = 0775
solr_conf_dir = '/etc/solr'
titan_solr_configset = 'titan'
titan_solr_collection_name = 'titan'
solr_port = config['configurations']['solr-env']['solr_port']
solr_user = config['configurations']['solr-env']['solr_user']
solr_conf_trg_file = '/opt/solr/server/solr/configsets/{titan_solr_configset}/conf/solrconfig.xml'
# for create_hdfs_directory
security_enabled = config['configurations']['cluster-env']['security_enabled']
hdfs_user_keytab = config['configurations']['hadoop-env']['hdfs_user_keytab']
kinit_path_local = get_kinit_path()
hadoop_bin_dir = '/opt/hadoop/bin'
hadoop_conf_dir = '/etc/hadoop'
hdfs_site = config['configurations']['hdfs-site']
hostname = config['hostname']
hdfs_principal_name = default('/configurations/hadoop-env/hdfs_principal_name', 'missing_principal').replace("_HOST",
                                                                                                             hostname)
default_fs = config['configurations']['core-site']['fs.defaultFS']

import functools

# to create hdfs directory we need to call params.HdfsResource in code
HdfsResource = functools.partial(
    HdfsResource,
    user=hdfs_user,
    security_enabled=security_enabled,
    keytab=hdfs_user_keytab,
    kinit_path_local=kinit_path_local,
    hadoop_bin_dir=hadoop_bin_dir,
    hadoop_conf_dir=hadoop_conf_dir,
    principal_name=hdfs_principal_name,
    hdfs_site=hdfs_site,
    default_fs=default_fs
)
