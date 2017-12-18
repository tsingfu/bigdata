from resource_management import *
from resource_management.libraries.functions.default import default
from resource_management.libraries.functions.version import format_stack_version
from resource_management.libraries.script.script import Script
import os

# temp directory
exec_tmp_dir = Script.get_tmp_dir()

# server configurations
config = Script.get_config()
stack_root = '/opt'

install_dir = config['configurations']['r4ml-env']['install_dir']
download_url = config['configurations']['r4ml-env']['download_url']
filename = download_url.split('/')[-1]
version_dir = filename[:-7]
r4ml_home = install_dir

r4ml_user = config['configurations']['r4ml-env']['r4ml_user']
user_group = config['configurations']['r4ml-env']['user_group']

stack_version_unformatted = str(config['hostLevelParams']['stack_version'])
stack_version = format_stack_version(stack_version_unformatted)

# New Cluster Stack Version that is defined during the RESTART of a Rolling Upgrade
version = default("/commandParams/version", None)
stack_name = default("/hostLevelParams/stack_name", None)

java_home = config['hostLevelParams']['java_home']
r4ml_conf_dir = "/etc/r4ml"

# environment variables
spark_home = '/opt/spark'
spark_driver_memory = "4G"
spark_submit_args = "--num-executors 4 sparkr-shell"
r4ml_auto_start = 0
Renviron_template = config['configurations']['r4ml-env']['Renviron']

# systemml jar path
systemml_jar = os.path.join(stack_root, "systemml", "lib", "systemml.jar")
if not os.path.isfile(systemml_jar) or not os.access(systemml_jar, os.R_OK):
    systemml_jar = ""

smokeuser = config['configurations']['cluster-env']['smokeuser']

yarn_keytab = config['configurations']['hadoop-env']['hdfs_user_keytab']
yarn_principal = config['configurations']['hadoop-env']['hdfs_principal_name']
