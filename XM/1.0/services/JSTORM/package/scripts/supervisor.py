#!/usr/bin/env python


from resource_management import *
from storm import config_storm,install_storm
from service import service
from resource_management.libraries.functions.check_process_status import check_process_status
from resource_management.libraries.script.script import Script

class Supervisor(Script):
  def install(self, env):
    print 'install supervisor ...'
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

    service("supervisor", action="start")

  def stop(self, env):
    import params
    env.set_params(params)

    service("supervisor", action="stop")

  def status(self, env):
    import status_params
    env.set_params(status_params)

    check_process_status(status_params.pid_supervisor)


if __name__ == "__main__":
  Supervisor().execute()

