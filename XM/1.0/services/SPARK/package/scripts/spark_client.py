#!/usr/bin/python
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

"""
# Python imports
import os
import sys

# Local imports
from resource_management.libraries.script.script import Script
from resource_management.core.exceptions import ClientComponentHasNoStatus
from setup_spark import setup_spark, install_spark


class SparkClient(Script):
    def install(self, env):
        install_spark()
        self.configure(env)

    def configure(self, env, config_dir=None, upgrade_type=None):
        """
    :param env: Python environment
    :param config_dir: During rolling upgrade, which config directory to save configs to.
    :param upgrade_type:  If in the middle of a stack upgrade, whether rolling or non-rolling
    """
        import params
        env.set_params(params)
        install_spark()
        setup_spark(
            env,
            'client',
            upgrade_type=upgrade_type,
            action='config',
            config_dir=config_dir)

    def status(self, env):
        raise ClientComponentHasNoStatus()

    def get_component_name(self):
        return "spark-client"

    def stack_upgrade_save_new_config(self, env):
        print 'todo'

    def pre_upgrade_restart(self, env, upgrade_type=None):
        print 'todo'


if __name__ == "__main__":
    SparkClient().execute()
