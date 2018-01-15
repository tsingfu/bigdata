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

from flume import flume
from flume import get_desired_state

from resource_management.libraries.script.script import Script
from resource_management.libraries.functions.flume_agent_helper import find_expected_agent_names, get_flume_status
from resource_management.core.exceptions import ComponentIsNotRunning
from resource_management.core.logger import Logger
from flume import install_flume

class FlumeHandler(Script):
    def configure(self, env):
        import params
        env.set_params(params)
        flume(action='config')

    def get_component_name(self):
        return "flume-server"

    def install(self, env):
        import params
        install_flume()
        env.set_params(params)

    def start(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        install_flume()
        self.configure(env)
        flume(action='start')

    def stop(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        flume(action='stop')

    def status(self, env):
        import params
        env.set_params(params)
        processes = get_flume_status(params.flume_conf_dir, params.flume_run_dir)
        expected_agents = find_expected_agent_names(params.flume_conf_dir)

        json = {}
        json['processes'] = processes
        self.put_structured_out(json)

        if len(expected_agents) > 0:
            for proc in processes:
                if not proc.has_key('status') or proc['status'] == 'NOT_RUNNING':
                    raise ComponentIsNotRunning()
        elif len(expected_agents) == 0 and 'INSTALLED' == get_desired_state():
            raise ComponentIsNotRunning()

    def pre_upgrade_restart(self, env, upgrade_type=None):
        import params
        env.set_params(params)

        Logger.info("Executing Flume Stack Upgrade pre-restart")
        install_flume()

    def get_log_folder(self):
        import params
        return params.flume_log_dir

    def get_user(self):
        import params
        return None  # means that is run from the same user as ambari is run

if __name__ == "__main__":
    FlumeHandler().execute()
