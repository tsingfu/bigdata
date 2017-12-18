#!/usr/bin/env python

from resource_management import *
from resource_management.libraries.script.script import Script
config = Script.get_config()

pid_dir = config['configurations']['jstorm-env']['jstorm_pid_dir']
pid_nimbus = format("{pid_dir}/nimbus.pid")
pid_supervisor = format("{pid_dir}/supervisor.pid")
pid_drpc = format("{pid_dir}/drpc.pid")
pid_ui = format("{pid_dir}/ui.pid")
pid_logviewer = format("{pid_dir}/logviewer.pid")
pid_files = {"logviewer":pid_logviewer,
             "ui": pid_ui,
             "nimbus": pid_nimbus,
             "supervisor": pid_supervisor,
             "drpc": pid_drpc}