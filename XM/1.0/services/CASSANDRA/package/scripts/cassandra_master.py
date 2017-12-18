#!/usr/bin/env python

from resource_management import *
from resource_management.core.resources.system import Execute, File
from resource_management.libraries.script.script import Script
from resource_management.libraries.functions.format import format

from cassandra import cassandra


class Cassandra_Master(Script):
    def install(self, env):
        import params
        env.set_params(params)
        print 'Install'
        self.install_packages(env)

    def configure(self, env):
        import params
        env.set_params(params)
        print 'Install plugins'
        cassandra()

    def stop(self, env):
        import params
        env.set_params(params)
        stop_cmd = format("service cassandra stop")
        start_opscenter = format("service opscenterd stop")
        Execute(stop_cmd)
        print 'Stop the Master'

    def start(self, env):
        import params
        env.set_params(params)
        self.configure(env)
        start_cmd = format("service cassandra start")
        start_opscenter = format("service opscenterd start")
        Execute(start_cmd)
        Execute(start_opscenter)
        print 'Start the Master'

    def status(self, env):
        import params
        env.set_params(params)
        status_cmd = format("service cassandra status")
        Execute(status_cmd)
        print 'Status of the Master'


if __name__ == "__main__":
    Cassandra_Master().execute()
