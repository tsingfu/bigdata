#!/usr/bin/env python

from resource_management import *
from resource_management.libraries.script.script import Script

config = Script.get_config()

streamline_pid_dir = config['configurations']['streamline-env']['streamline_pid_dir']
streamline_pid_file = format("{streamline_pid_dir}/streamline.pid")
