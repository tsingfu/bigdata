from resource_management.libraries.script.script import Script
from resource_management.libraries import functions
from resource_management.libraries.functions import default

config = Script.get_config()
tmp_dir = Script.get_tmp_dir()

hostname = config['hostname']
kinit_path_local = functions.get_kinit_path(
    default('/configurations/kerberos-env/executable_search_paths', None))

security_enabled = config['configurations']['cluster-env']['security_enabled']

smoke_user_keytab = config['configurations']['cluster-env']['smokeuser_keytab']
smoke_user = config['configurations']['cluster-env']['smokeuser']
smoke_user_principal = config['configurations']['cluster-env'][
    'smokeuser_principal_name']
