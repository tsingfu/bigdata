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

import falcon_server_upgrade

from resource_management.core.logger import Logger
from resource_management.libraries.script import Script
from resource_management.libraries.functions import check_process_status
from resource_management.libraries.functions.stack_features import check_stack_feature
from resource_management.libraries.functions.constants import StackFeature, Direction

from falcon import falcon,install_falcon


class FalconServer(Script):
    def configure(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        falcon('server', action='config', upgrade_type=upgrade_type)

    def start(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        install_falcon(first=True)
        self.configure(env, upgrade_type=upgrade_type)
        falcon('server', action='start', upgrade_type=upgrade_type)

    def stop(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        falcon('server', action='stop', upgrade_type=upgrade_type)

        # if performing an upgrade (ROLLING / NON_ROLLING), backup some directories after stopping falcon
        if upgrade_type is not None:
            falcon_server_upgrade.post_stop_backup()

    def get_component_name(self):
        return "falcon-server"

    def install(self, env):
        import params
        install_falcon()
        env.set_params(params)

    def status(self, env):
        import status_params
        env.set_params(status_params)
        check_process_status(status_params.server_pid_file)

    def pre_upgrade_restart(self, env, upgrade_type=None):
        Logger.info("Executing Stack Upgrade pre-restart")
        import params
        env.set_params(params)

        # this function should not execute if the version can't be determined or
        # the stack does not support rolling upgrade
        if not (params.version and check_stack_feature(StackFeature.ROLLING_UPGRADE, params.version)):
            return

        Logger.info("Executing Falcon Server Stack Upgrade pre-restart")
        install_falcon()

        falcon_server_upgrade.pre_start_restore()

    def get_log_folder(self):
        import params
        return params.falcon_log_dir

    def get_user(self):
        import params
        return params.falcon_user

    def get_pid_files(self):
        import status_params
        return [status_params.server_pid_file]


if __name__ == "__main__":
    FalconServer().execute()
