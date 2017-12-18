#!/usr/bin/env python

from resource_management import *
from resource_management.libraries.functions.format import format
from resource_management.libraries.script.script import Script
from resource_management.core.resources.system import Directory, File, Execute


class ServiceCheck(Script):
    def service_check(self, env):
        import params
        env.set_params(params)

        Execute(format("which redis-server"))


if __name__ == "__main__":
    ServiceCheck().execute()
