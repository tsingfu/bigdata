#!/usr/bin/python

import sys

from resource_management import *
from resource_management.libraries.script.script import Script
from resource_management.libraries.functions import get_unique_id_and_date
from resource_management.libraries.functions.version import compare_versions, format_hdp_stack_version
from resource_management.libraries.functions.security_commons import build_expectations, \
  cached_kinit_executor, get_params_from_filesystem, validate_security_config_properties, \
  FILE_TYPE_JAAS_CONF
from resource_management.core.shell import call
from resource_management.core.logger import Logger
from resource_management.core.resources.system import Execute
from resource_management.libraries.functions.check_process_status import check_process_status
from resource_management.libraries.functions.format import format
from resource_management.libraries.functions.validate import call_and_match_output

from actions import *


class EagleTopology(Script):
    def install(self, env):
        Logger.info('Install the eagle topology')
        import params
        env.set_params(params)
        install_eagle()
        self.configure(env)

    def configure(self, env):
        Logger.info("Configure eagle topology")
        import params
        env.set_params(params)
        config_eagle()

    def pre_rolling_restart(self, env):
        Logger.info("Executing Rolling Upgrade pre-restart")
        import params
        env.set_params(params)

    def stop(self, env):
        Logger.info('Stop the eagle topology')
        import params
        env.set_params(params)
        self.configure(env)
        eagle_topology_exec(action='stop')

    def start(self, env):
        Logger.info('Start the eagle topology')
        import params
        env.set_params(params)
        install_eagle()
        self.configure(env)
        eagle_topology_exec(action='start')

    def status(self, env):
        Logger.info('Status of the eagle topology')
        import params
        env.set_params(params)
        self.configure(env)
        eagle_topology_exec(action='status')


if __name__ == "__main__":
    EagleTopology().execute()
