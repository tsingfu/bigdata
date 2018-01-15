#!/usr/bin/env python

import sys
from resource_management import *
from hive import hive, install_hive
from resource_management.core.exceptions import ClientComponentHasNoStatus


class HiveClient(Script):
    def install(self, env):
        import params
        install_hive()
        self.configure(env)

    def status(self, env):
        raise ClientComponentHasNoStatus()

    def configure(self, env):
        import params
        env.set_params(params)
        install_hive()
        hive(name='client')

    def get_component_name(self):
        return "hadoop-client"

    def pre_upgrade_restart(self, env, upgrade_type=None):
        print 'todo'


if __name__ == "__main__":
    HiveClient().execute()
