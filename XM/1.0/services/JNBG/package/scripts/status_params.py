#!/usr/bin/env python

from resource_management.libraries.script.script import Script
from resource_management.libraries.functions.format import format

config = Script.get_config()
jkg_pid_dir = config['configurations']['jnbg-env']['jkg_pid_dir_prefix']
jkg_pid_file = format("{jkg_pid_dir}/jupyter_kernel_gateway.pid")
