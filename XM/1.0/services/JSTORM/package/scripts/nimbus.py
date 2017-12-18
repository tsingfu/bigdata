#!/usr/bin/env python

import sys
from resource_management import *
from storm import config_storm,install_storm
from service import service
from resource_management.libraries.functions.check_process_status import check_process_status
from resource_management.libraries.script.script import Script

class Nimbus(Script):
  def install(self, env):
    print 'install nimbus ...'
    self.configure(env)
    install_storm(first=True)

  def configure(self, env):
    import params
    env.set_params(params)
    config_storm()

  def start(self, env):
    import params
    env.set_params(params)
    install_storm()
    self.configure(env)

    service("nimbus", action="start")

  def stop(self, env):
    import params
    env.set_params(params)

    service("nimbus", action="stop")

  def status(self, env):
    import status_params
    env.set_params(status_params)
    check_process_status(status_params.pid_nimbus)

if __name__ == "__main__":
  Nimbus().execute()
