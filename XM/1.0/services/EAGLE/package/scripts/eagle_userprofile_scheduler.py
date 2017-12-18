#!/usr/bin/python

import sys

from resource_management import *
from resource_management.libraries.script.script import Script
from resource_management.core.logger import Logger

from actions import *


class EagleUserProfileScheduler(Script):
    def install(self, env):
        Logger.info('Install Eagle UserProfile Scheduler')
        import params
        env.set_params(params)
        install_eagle()
        self.configure(env)

    def configure(self, env):
        Logger.info("Configure Eagle UserProfile Scheduler")
        import params
        env.set_params(params)
        config_eagle()

    def pre_rolling_restart(self, env):
        Logger.info(
            "Executing rolling pre-restart Eagle UserProfile Scheduler")
        import params
        env.set_params(params)

    def stop(self, env):
        Logger.info('Stopping Eagle UserProfile Scheduler')
        import params
        env.set_params(params)
        self.configure(env)
        eagle_userprofile_scheduler_exec(action='stop')

    def start(self, env):
        Logger.info('Starting Eagle UserProfile Scheduler')
        import params
        env.set_params(params)
        install_eagle()
        self.configure(env)
        eagle_userprofile_scheduler_exec(action='start')

    def status(self, env):
        Logger.info('Checking Eagle UserProfile Scheduler')
        import params
        env.set_params(params)
        self.configure(env)
        eagle_userprofile_scheduler_exec(action='status')


if __name__ == "__main__":
    EagleUserProfileScheduler().execute()
