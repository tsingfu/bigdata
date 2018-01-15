#!/usr/bin/env python
import sys
from resource_management import *
from storm import config_storm,install_storm
from service import service

class DrpcServer(Script):
  def install(self, env):
    print 'install drpc ...'
    self.configure(env)
    install_storm()

  def configure(self, env):
    import params
    env.set_params(params)
    config_storm()

  def start(self, env):
    import params
    env.set_params(params)
    install_storm()
    self.configure(env)

    service("drpc", action="start")

  def stop(self, env):
    import params
    env.set_params(params)

    service("drpc", action="stop")

  def status(self, env):
    import status_params
    env.set_params(status_params)
    check_process_status(status_params.pid_drpc)

if __name__ == "__main__":
  DrpcServer().execute()
