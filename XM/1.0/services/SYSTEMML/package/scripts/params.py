from resource_management.libraries.functions.default import default
from resource_management.libraries.functions.format import format
from resource_management.libraries.functions.version import format_stack_version
from resource_management.libraries.functions import stack_select
from resource_management.libraries.script.script import Script

# server configurations
config = Script.get_config()
stack_root = '/opt'

stack_version_unformatted = str(config['hostLevelParams']['stack_version'])
stack_version = format_stack_version(stack_version_unformatted)

# New Cluster Stack Version that is defined during the RESTART of a Rolling Upgrade
version = default("/commandParams/version", None)
stack_name = default("/hostLevelParams/stack_name", None)

java_home = config['hostLevelParams']['java_home']

install_dir = config['configurations']['systemml-env']['install_dir']
download_url = config['configurations']['systemml-env']['download_url']
filename = download_url.split('/')[-1]
version_dir = filename[:-7]

conf_dir = '/etc/systemml'
systemml_user = config['configurations']['systemml-env']['systemml_user']
user_group = config['configurations']['cluster-env']['user_group']

systemml_home_dir = install_dir
systemml_lib_dir = format("{systemml_home_dir}/lib")
systemml_scripts_dir = format("{systemml_home_dir}/scripts")
