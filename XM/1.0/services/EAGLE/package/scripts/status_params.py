#!/usr/bin/python
from resource_management.libraries.script.script import Script

config = Script.get_config()
eagle_pid_dir = config['configurations']['eagle-env']['eagle_pid_dir']
eagle_service_pid_file = eagle_pid_dir + '/service.pid'