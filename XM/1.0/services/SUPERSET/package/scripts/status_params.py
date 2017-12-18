#!/usr/bin/env python

from resource_management.libraries.script.script import Script

config = Script.get_config()

superset_pid_dir = config['configurations']['superset-env']['superset_pid_dir']
