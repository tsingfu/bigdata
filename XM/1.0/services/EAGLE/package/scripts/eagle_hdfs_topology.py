#!/usr/bin/python

import sys

from resource_management import *
from resource_management.libraries.script.script import Script
from resource_management.core.logger import Logger
from actions import *


class EagleHdfsTopology(Script):
    def install(self, env):
        Logger.info('Install Eagle DAM HDFS Topology')
        # self.install_packages(env)
        import params
        env.set_params(params)
        install_eagle()
        self.configure(env)
        # eagle_hdfs_topology_exec(action = 'init')

    def configure(self, env):
        Logger.info("Configure Eagle DAM HDFS Topology")
        import params
        env.set_params(params)
        config_eagle()
        # eagle_hdfs_topology_exec(action = 'init')

    def pre_rolling_restart(self, env):
        Logger.info("Executing rolling pre-restart Eagle DAM HDFS Topology")
        import params
        env.set_params(params)

    def stop(self, env):
        Logger.info('Stopping Eagle DAM HDFS Topology')
        import params
        env.set_params(params)
        self.configure(env)
        eagle_hdfs_topology_exec(action='stop')

    def start(self, env):
        Logger.info('Starting Eagle DAM HDFS Topology')
        import params
        env.set_params(params)
        install_eagle()
        self.configure(env)

        eagle_hdfs_topology_exec(action='init')
        eagle_hdfs_topology_exec(action='start')

    def status(self, env):
        Logger.info('Checking Eagle DAM HDFS Topology status')
        import params
        env.set_params(params)
        self.configure(env)
        eagle_hdfs_topology_exec(action='status')


if __name__ == "__main__":
    EagleHdfsTopology().execute()
