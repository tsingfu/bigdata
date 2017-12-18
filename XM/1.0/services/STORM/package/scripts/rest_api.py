#!/usr/bin/env python
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

import sys
from resource_management.libraries.functions import check_process_status
from resource_management.libraries.script import Script

from storm import storm, install_storm
from service import service


class StormRestApi(Script):
    """
  Storm REST API.
  It was available in HDP 2.0 and 2.1.
  In HDP 2.2, it was removed since the functionality was moved to Storm UI Server.
  """

    def get_component_name(self):
        return "storm-client"

    def install(self, env):
        install_storm(first=True)
        self.configure(env)

    def configure(self, env):
        import params
        env.set_params(params)

        storm()

    def start(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        install_storm()
        self.configure(env)

        service("rest_api", action="start")

    def stop(self, env, upgrade_type=None):
        import params
        env.set_params(params)

        service("rest_api", action="stop")

    def status(self, env):
        import status_params
        env.set_params(status_params)
        check_process_status(status_params.pid_rest_api)

    def get_log_folder(self):
        import params
        return params.log_dir

    def get_user(self):
        import params
        return params.storm_user

    def get_pid_files(self):
        import status_params
        return [status_params.pid_rest_api]


if __name__ == "__main__":
    StormRestApi().execute()
