"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Ambari Agent

"""
# Python imports
import os
import sys

# Local imports
from resource_management.libraries.script.script import Script
from resource_management.core.exceptions import ClientComponentHasNoStatus
from yarn import yarn,install_hadoop


class MapReduce2Client(Script):
    def install(self, env):
        import params
        self.install_packages(env)
        install_hadoop()
        self.configure(env)

    def configure(self, env, config_dir=None, upgrade_type=None):
        """
    :param env: Python environment
    :param config_dir: During rolling upgrade, which config directory to save configs to.
    """
        import params
        env.set_params(params)
        yarn(config_dir=config_dir)

    def status(self, env):
        raise ClientComponentHasNoStatus()

    def stack_upgrade_save_new_config(self, env):
        print 'todo'

    def get_component_name(self):
        return "hadoop-client"

    def pre_upgrade_restart(self, env, upgrade_type=None):
        install_hadoop()


if __name__ == "__main__":
    MapReduce2Client().execute()
