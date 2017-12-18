#!/usr/bin/env python
from resource_management import *
from resource_management.libraries.script.script import Script

# server configurations
config = Script.get_config()

kudu_user = config['configurations']['kudu-env']['kudu_user']
kudu_group = config['configurations']['kudu-env']['kudu_group']
kudu_log_dir = config['configurations']['kudu-env']['kudu_log_dir']

security_enabled = config['configurations']['cluster-env']['security_enabled']