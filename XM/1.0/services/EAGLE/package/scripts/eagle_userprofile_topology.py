#!/usr/bin/python

import sys

from resource_management import *
from resource_management.libraries.script.script import Script
from resource_management.core.logger import Logger
from actions import *


class EagleUserProfileTopology(Script):
    def install(self, env):
        Logger.info('Install Eagle DAM UserProfile Topology')
        import params
        env.set_params(params)
        install_eagle()
        self.configure(env)

    def configure(self, env):
        Logger.info("Configure Eagle DAM UserProfile Topology")
        import params
        env.set_params(params)
        config_eagle()

    def pre_rolling_restart(self, env):
        Logger.info(
            "Executing rolling pre-restart Eagle DAM UserProfile Topology")
        import params
        env.set_params(params)

    def stop(self, env):
        Logger.info('Stopping Eagle DAM UserProfile Topology')
        import params
        env.set_params(params)
        self.configure(env)
        eagle_userprofile_topology_exec(action='stop')

    def start(self, env):
        Logger.info('Starting Eagle DAM UserProfile Topology')
        import params
        env.set_params(params)
        install_eagle()
        self.configure(env)

        eagle_userprofile_topology_exec(action='init')
        eagle_userprofile_topology_exec(action='start')

    def status(self, env):
        Logger.info('Checking Eagle DAM UserProfile Topology status')
        import params
        env.set_params(params)
        self.configure(env)
        eagle_userprofile_topology_exec(action='status')


if __name__ == "__main__":
    EagleUserProfileTopology().execute()
