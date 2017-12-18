#!/usr/bin/python

import sys
from resource_management import *
from resource_management.core.logger import Logger
from actions import *
from resource_management.libraries.script.script import Script

class EagleAuditKafka(Script):
    def install(self, env):
        Logger.info('Installing Eagle AuditK2afka Client')
        import params
        env.set_params(params)
        install_eagle()
        self.configure(env)

    def configure(self, env):
        Logger.info("Configure Eagle Audit2Kafka Client")
        import params
        env.set_params(params)
        config_eagle()

    def pre_rolling_restart(self, env):
        Logger.info("Executing rolling pre-restart  Eagle Audit2Kafka Client")
        import params
        env.set_params(params)

    def stop(self, env):
        Logger.info('Stopping Eagle Audit2Kafka Client')
        import params
        env.set_params(params)
        self.configure(env)

    def start(self, env):
        Logger.info('Starting Eagle Audit2Kafka Client')
        import params
        env.set_params(params)
        install_eagle()
        self.configure(env)

    def status(self, env):
        Logger.info('Checking  Eagle Audit2Kafka Client')
        import params
        env.set_params(params)
        self.configure(env)


if __name__ == "__main__":
    EagleAuditKafka().execute()
