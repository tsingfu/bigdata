from ambari_commons.constants import AMBARI_SUDO_BINARY
from resource_management.libraries.script.script import Script
from resource_management.libraries.functions import format
from resource_management.libraries.functions.default import default
import status_params

# server configurations
config = Script.get_config()
exec_tmp_dir = Script.get_tmp_dir()
sudo = AMBARI_SUDO_BINARY

user = config['configurations']['schema-registry-env']['app_user']
group = config['configurations']['schema-registry-env']['user_group']
data_dir = status_params.data_dir

log_dir = format("{data_dir}/log")
log_file = format("{log_dir}/schema-registry.log")
logsearch_file = format("{log_dir}/schema-registry.json")
pid_dir = status_params.pid_dir
pid_file = status_params.pid_file
schema_registry_conf_dir = config['configurations']['schema-registry-env']['conf_dir']
schema_registry_conf_file_location = schema_registry_conf_dir + "/schema-registry.properties"
schema_registry_conf = config['configurations']['schema-registry-properties']['content']
log4j_file_location = schema_registry_conf_dir + "/log4j.properties"
log4j_props = config['configurations']['schema-registry-log4j']['content']
logsearch_conf_dir = default("/configurations/logfeeder-properties/input_config_dir",
                             "/etc/ambari-logsearch-logfeeder/conf/custom")
logsearch_props_file_location = format("{logsearch_conf_dir}/input.config-schema_registry.json")
logsearch_props = config['configurations']['schema-registry-logsearch-input']['content']

port = config['configurations']['schema-registry-env']['port']
if 'zookeeper_hosts' in config['clusterHostInfo'] and \
                len(config['clusterHostInfo']['zookeeper_hosts']) > 0:

    zookeeper_hosts = config['clusterHostInfo']['zookeeper_hosts']
    zookeeper_hosts.sort()
    zk_connects_strings = []
    zk_port = config['configurations']['zoo.cfg']['clientPort']
    for zk_host in zookeeper_hosts:
        zk_connects_strings.append(zk_host + ":" + str(zk_port))
    kafkastore_connection_url = config['configurations']['schema-registry-env']['kafkastore.connection.url']
    kafkastore_connection_url = ','.join(zk_connects_strings)
else:
    kafkastore_connection_url = config['configurations']['schema-registry-env']['kafkastore.connection.url']

# metrics
host_name = config['hostname'].lower()
service_name = config['serviceName']
metric_collector_hosts = default("/clusterHostInfo/metrics_collector_hosts", [])
has_metric_collector = not len(metric_collector_hosts) == 0
if has_metric_collector:
    metric_collector_host = metric_collector_hosts[0]
    metric_collector_web_address = default("/configurations/ams-site/timeline.metrics.service.webapp.address",
                                           "0.0.0.0:6188")
    if metric_collector_web_address.find(':') != -1:
        metric_collector_port = metric_collector_web_address.split(':')[1]
    else:
        metric_collector_port = '6188'
has_metric_collector = str(has_metric_collector).lower()
