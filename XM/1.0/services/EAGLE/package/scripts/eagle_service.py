#!/usr/bin/python

import sys

from resource_management import *
from resource_management.libraries.script.script import Script
from resource_management.core.logger import Logger
from resource_management.libraries.functions.check_process_status import check_process_status
from actions import *


class EagleService(Script):
    def install(self, env):
        Logger.info('Install the eagle service')
        import params
        env.set_params(params)
        install_eagle(first=True)
        self.configure(env)

    def configure(self, env):
        Logger.info("Configure eagle service")
        import params
        env.set_params(params)
        config_eagle()

    def pre_rolling_restart(self, env):
        Logger.info("Executing Rolling Upgrade pre-restart")
        import params
        env.set_params(params)

    def stop(self, env):
        Logger.info('Stop the eagle service')
        import params
        env.set_params(params)
        self.configure(env)
        eagle_service_exec(action='stop')

    def start(self, env):
        Logger.info('Start the eagle service')
        import params
        env.set_params(params)
        install_eagle()
        self.configure(env)
        eagle_service_exec(action='restart')

    def status(self, env):
        Logger.info('Status of the eagle service')
        import status_params
        env.set_params(status_params)
        check_process_status(status_params.eagle_service_pid_file)


if __name__ == "__main__":
    EagleService().execute()
