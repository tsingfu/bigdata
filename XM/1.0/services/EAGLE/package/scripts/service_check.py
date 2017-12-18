#!/usr/bin/python

from resource_management import *
from resource_management.libraries.script.script import Script
from resource_management.libraries.functions.format import format
from resource_management.core.logger import Logger
from resource_management.core.resources.system import Execute

class EagleServiceCheck(Script):
    def service_check(self, env):
        Logger.info("Checking eagle service")
        import params
        env.set_params(params)
        check_eagle_service_cmd = format(
            "ls {eagle_service_pid_file} >/dev/null 2>&1 && ps -p `cat {eagle_service_pid_file}` >/dev/null 2>&1")
        Execute(check_eagle_service_cmd, logoutput=True, try_sleep=3, tries=5)


if __name__ == "__main__":
    EagleServiceCheck().execute()
