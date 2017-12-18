#!/usr/bin/python

from resource_management.libraries.functions.default import default
from resource_management import *
from resource_management.libraries.functions import get_kinit_path
from resource_management.libraries.script.script import Script

# server configurations
config = Script.get_config()
tmp_dir = Script.get_tmp_dir()

stack_version_unformatted = str(config['hostLevelParams']['stack_version'])

stack_name = default("/hostLevelParams/stack_name", None)

version = default("/commandParams/version", None)

install_dir = config['configurations']['eagle-env']['install_dir']
download_url = config['configurations']['eagle-env']['download_url']
filename = download_url.split('/')[-1]
version_dir = filename.replace('.tar.gz','').replace('.tgz','')

eagle_home = install_dir
eagle_bin = eagle_home + '/bin'
eagle_conf = '/etc/eagle'

user_group = eagle_user = config['configurations']['eagle-env']['eagle_user']
eagle_site = config['configurations']['eagle-env']['eagle_site']
eagle_service_pid_file = config['configurations']['eagle-env']['eagle_pid_dir'] + '/service.pid'
eagle_log_dir = config['configurations']['eagle-env']['eagle_log_dir']

eagle_env_content = config['configurations']['eagle-env']['content']
log4j_content = config['configurations']['eagle-env']['log4j']
application_content = config['configurations']['eagle-env']['application']
eagle_service_content = config['configurations']['eagle-env']['eagle_service']
eagle_scheduler_content = config['configurations']['eagle-env']['eagle_scheduler']
kafka_server_content = config['configurations']['eagle-env']['kafka_server']

eagle_kerberos_keytab = config['configurations']['eagle-env']['eagle.server.kerberos.keytab']
eagle_kerberos_principal = config['configurations']['eagle-env']['eagle.server.kerberos.principal']
hbase_zookeeper_quorum = config['configurations']['hbase-site']['hbase.zookeeper.quorum']
hbase_zookeeper_property_clientPort = config['configurations']['hbase-site']['hbase.zookeeper.property.clientPort']
