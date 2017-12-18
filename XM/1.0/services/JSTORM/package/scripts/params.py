#!/usr/bin/env python

from resource_management import *
from resource_management.libraries.script.script import Script
import status_params
from resource_management.libraries.functions import format
from resource_management.libraries.functions.default import default
from resource_management.libraries.functions.get_bare_principal import get_bare_principal
from resource_management.libraries.script import Script
from resource_management.libraries.functions.get_bare_principal import get_bare_principal

# server configurations
config = Script.get_config()

user_group = jstorm_group = jstorm_user = config['configurations']['jstorm-env']['jstorm_user']

log_dir = config['configurations']['jstorm-env']['jstorm_log_dir']
pid_dir = status_params.pid_dir

local_dir = config['configurations']['jstorm-site']['storm.local.dir']
conf_dir = config['configurations']['jstorm-site']['_storm.conf.dir']
tomcat_dir = config['configurations']['jstorm-site']['_storm.tomcat.dir']
jstorm_bin_dir = local_dir + "/bin"

java64_home = config['hostLevelParams']['java_home']
jps_binary = format("{java64_home}/bin/jps")

storm_env_sh_template = config['configurations']['jstorm-env']['content']

install_dir = config['configurations']['jstorm-env']['install_dir']
download_url = config['configurations']['jstorm-env']['download_url']
filename = download_url.split('/')[-1]
version_dir = filename.replace('.tar.gz', '').replace('.tgz', '')

if 'ganglia_server_host' in config['clusterHostInfo'] and \
                len(config['clusterHostInfo']['ganglia_server_host']) > 0:
    ganglia_installed = True
    ganglia_server = config['clusterHostInfo']['ganglia_server_host'][0]
    ganglia_report_interval = 60
else:
    ganglia_installed = False

security_enabled = config['configurations']['cluster-env']['security_enabled']

if security_enabled:
    _hostname_lowercase = config['hostname'].lower()
    _storm_principal_name = config['configurations']['jstorm-env'][
        'storm_principal_name']
    storm_jaas_principal = _storm_principal_name.replace('_HOST',
                                                         _hostname_lowercase)
    _ambari_principal_name = default(
        '/configurations/cluster-env/ambari_principal_name', None)
    storm_keytab_path = config['configurations']['jstorm-env']['storm_keytab']

    storm_ui_keytab_path = config['configurations']['jstorm-env'][
        'storm_ui_keytab']
    _storm_ui_jaas_principal_name = config['configurations']['jstorm-env'][
        'storm_ui_principal_name']
    storm_ui_jaas_principal = _storm_ui_jaas_principal_name.replace(
        '_HOST', _hostname_lowercase)
    storm_bare_jaas_principal = get_bare_principal(_storm_principal_name)
    if _ambari_principal_name:
        ambari_bare_jaas_principal = get_bare_principal(
            _ambari_principal_name)
    _nimbus_principal_name = config['configurations']['jstorm-env'][
        'nimbus_principal_name']
    nimbus_jaas_principal = _nimbus_principal_name.replace(
        '_HOST', _hostname_lowercase)
    nimbus_bare_jaas_principal = get_bare_principal(_nimbus_principal_name)
    nimbus_keytab_path = config['configurations']['jstorm-env'][
        'nimbus_keytab']
    storm_thrift_transport = config['configurations']['jstorm-site'][
        '_storm.thrift.secure.transport']
    _kafka_principal_name = default(
        "/configurations/kafka-env/kafka_principal_name", None)
    kafka_bare_jaas_principal = get_bare_principal(_kafka_principal_name)
