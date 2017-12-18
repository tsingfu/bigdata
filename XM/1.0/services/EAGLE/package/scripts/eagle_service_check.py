#!/usr/bin/python

from resource_management import *
from actions import *
from resource_management.libraries.script.script import Script

class EagleServiceCheck(Script):
    def service_check(self, env):
        import params
        env.set_params(params)
        eagle_service_exec(action="status")


if __name__ == "__main__":
    EagleServiceCheck().execute()
