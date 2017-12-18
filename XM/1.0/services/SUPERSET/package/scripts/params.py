#!/usr/bin/env python

from resource_management.libraries.script.script import Script
from resource_management.libraries.functions import format
from resource_management.libraries.functions.default import default

import status_params

# server configurations
config = Script.get_config()
stack_root = '/usr/hdp'
stack_name = default("/hostLevelParams/stack_name", None)
user_group = config['configurations']['cluster-env']['user_group']

install_dir = config['configurations']['druid-env']['install_dir']
download_url = config['configurations']['druid-env']['download_url']
filename = download_url.split('/')[-1]
version_dir = filename.replace('.tar.gz', '').replace('.tgz', '')

# stack version
stack_version = default("/commandParams/version", None)

hostname = config['hostname']

# status params
status_pid_dir = status_params.superset_pid_dir

superset_home_dir = '/usr/hdp/2.6.1.0-129/superset'
superset_bin_dir = format("{superset_home_dir}/bin")
superset_log_dir = default("/configurations/superset-env/superset_log_dir", '/var/log/superset')
superset_pid_dir = status_params.superset_pid_dir
superset_config_dir = '/etc/superset/conf'
superset_admin_user = config['configurations']['superset-env']['superset_admin_user']
superset_admin_password = config['configurations']['superset-env']['superset_admin_password']
superset_admin_firstname = config['configurations']['superset-env']['superset_admin_firstname']
superset_admin_lastname = config['configurations']['superset-env']['superset_admin_lastname']
superset_admin_email = config['configurations']['superset-env']['superset_admin_email']
superset_env_sh_template = config['configurations']['superset-env']['content']
superset_user = config['configurations']['superset-env']['superset_user']
superset_protocol = "http"
superset_webserver_address = config['configurations']['superset']['SUPERSET_WEBSERVER_ADDRESS']
superset_webserver_port = config['configurations']['superset']['SUPERSET_WEBSERVER_PORT']
superset_timeout = config['configurations']['superset']['SUPERSET_TIMEOUT']
superset_workers = config['configurations']['superset']['SUPERSET_WORKERS']
superset_hosts = default('/clusterHostInfo/superset_hosts', None)

# superset database configs
superset_db_type = config['configurations']['superset']['SUPERSET_DATABASE_TYPE']
superset_db_name = config['configurations']['superset']['SUPERSET_DATABASE_NAME']
superset_db_password = config['configurations']['superset']['SUPERSET_DATABASE_PASSWORD']
superset_db_user = config['configurations']['superset']['SUPERSET_DATABASE_USER']
superset_db_port = config['configurations']['superset']['SUPERSET_DATABASE_PORT']
superset_db_host = config['configurations']['superset']['SUPERSET_DATABASE_HOSTNAME']

superset_db_uri = None
if superset_db_type == "sqlite":
    superset_db_uri = format("sqlite:///{superset_config_dir}/{superset_db_name}.db")
elif superset_db_type == "postgresql":
    superset_db_uri = format(
        "postgresql+pygresql://{superset_db_user}:{superset_db_password}@{superset_db_host}:{superset_db_port}/{superset_db_name}")
elif superset_db_type == "mysql":
    superset_db_uri = format(
        "mysql+pymysql://{superset_db_user}:{superset_db_password}@{superset_db_host}:{superset_db_port}/{superset_db_name}")

druid_coordinator_hosts = default("/clusterHostInfo/druid_coordinator_hosts", [])

if not len(druid_coordinator_hosts) == 0:
    druid_coordinator_host = druid_coordinator_hosts[0]
    druid_coordinator_port = config['configurations']['druid-coordinator']['druid.port']
druid_router_hosts = default("/clusterHostInfo/druid_router_hosts", [])
if not len(druid_router_hosts) == 0:
    druid_router_host = druid_router_hosts[0]
    druid_router_port = config['configurations']['druid-router']['druid.port']
