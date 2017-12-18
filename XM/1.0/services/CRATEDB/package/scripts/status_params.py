from resource_management import *
from resource_management.libraries.script.script import Script

config = Script.get_config()

crate_pid_dir = config['configurations']['crate-env']['crate_pid_dir']
crate_pid_file = format("{crate_pid_dir}/crate.pid")
