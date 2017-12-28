#!/usr/bin/env python
# -*- coding: utf-8 -*-

from resource_management.libraries.script.script import Script
from resource_management.libraries.functions.default import default

config = Script.get_config()

node_properties = config['configurations']['node.properties']
jvm_config = config['configurations']['jvm.config']
config_properties = config['configurations']['config.properties']

http_port = config_properties['http-server.http.port']

discovery_uri = 'http://127.0.0.1' + ':' + str(http_port)

presto_coordinator_host = default('clusterHostInfo/presto_coordinator_hosts',[])
if len(presto_coordinator_host)>0:
    discovery_uri = 'http://' + presto_coordinator_host[0] + ':' + str(http_port)

connectors_to_add = config['configurations']['connectors.properties']['connectors.to.add']
connectors_to_delete = config['configurations']['connectors.properties']['connectors.to.delete']

env_sh_template = config['configurations']['presto-env']['content']

daemon_control_script = '/etc/init.d/presto'
config_directory = '/etc/presto'

memory_configs = ['query.max-memory-per-node', 'query.max-memory']

host_info = config['clusterHostInfo']

host_level_params = config['hostLevelParams']
java_home = host_level_params['java_home']

install_dir = config['configurations']['presto-env']['install_dir']
download_url = config['configurations']['presto-env']['download_url']
filename = download_url.split('/')[-1]
version_dir = filename.replace('.tar.gz', '').replace('.tgz', '')

download_url_cli = config['configurations']['presto-env']['download_url_cli']

presto_user = config['configurations']['presto-env']['presto_user']
user_group = config['configurations']['cluster-env']['user_group']
security_enabled = config['configurations']['cluster-env']['security_enabled']
hive_metastore_uri = config['configurations']['hive-site']['hive.metastore.uris']

hostname = config["hostname"].lower()

hive_metastore_keytab_path = config['configurations']['hive-site']['hive.metastore.kerberos.keytab.file']
hive_metastore_principal = config['configurations']['hive-site']['hive.metastore.kerberos.principal']
presto_principal = default('configurations/presto-env/presto_principal_name','presto/' + hostname + '@example.com')
presto_keytab = default('configurations/presto-env/presto_keytab_path','/etc/security/keytabs/presto.service.keytab')

kafka_broker_hosts = default('clusterHostInfo/kafka_broker_hosts',[])