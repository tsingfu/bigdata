from resource_management.libraries.script.script import Script
from resource_management.libraries.functions.format import format

# server configurations
config = Script.get_config()

titan_pid_dir = config['configurations']['titan-env']['titan_pid_dir']
titan_pid_file = format("{titan_pid_dir}/titan.pid")
