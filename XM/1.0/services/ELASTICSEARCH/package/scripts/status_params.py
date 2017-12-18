#!/usr/bin/env python

from resource_management.libraries.script import Script

config = Script.get_config()

elastic_pid_dir = config['configurations']['elastic-env']['elastic_pid_dir']
elastic_pid_file = format("{elastic_pid_dir}/elasticsearch.pid")
