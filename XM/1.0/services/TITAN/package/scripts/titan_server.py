#!/usr/bin/env python

import sys
from resource_management import *
from resource_management.libraries.functions.check_process_status import check_process_status
from titan_service import titan_service
import titan
from titan import install_titan

class TitanServer(Script):
    def install(self, env):
        self.install_packages(env)
        install_titan(first=True)

    def configure(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        titan.titan(type='server', upgrade_type=upgrade_type)

    def pre_upgrade_restart(self, env, upgrade_type=None):
        Logger.info("Executing Stack Upgrade pre-restart")
        import params
        env.set_params(params)
        print 'todo'

    def start(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        install_titan()
        self.configure(env)
        titan_service(action='start')

    def stop(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        titan_service(action='stop')

    def status(self, env, upgrade_type=None):
        import params_server
        check_process_status(params_server.titan_pid_file)


if __name__ == "__main__":
    TitanServer().execute()
