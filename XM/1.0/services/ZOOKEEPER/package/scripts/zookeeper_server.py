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
import random

from resource_management.libraries.script.script import Script
from resource_management.libraries.functions.security_commons import build_expectations, \
  cached_kinit_executor, get_params_from_filesystem, validate_security_config_properties, \
  FILE_TYPE_JAAS_CONF
from resource_management.core import shell
from resource_management.libraries.functions.check_process_status import check_process_status
from zookeeper import zookeeper, install_zookeeper
from zookeeper_service import zookeeper_service


class ZookeeperServer(Script):
    def configure(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        zookeeper(type='server', upgrade_type=upgrade_type)

    def start(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        install_zookeeper()
        self.configure(env, upgrade_type=upgrade_type)
        zookeeper_service(action='start', upgrade_type=upgrade_type)

    def stop(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        zookeeper_service(action='stop', upgrade_type=upgrade_type)

    def get_component_name(self):
        return "zookeeper-server"

    def install(self, env):
        install_zookeeper()
        self.configure(env)

    def pre_upgrade_restart(self, env, upgrade_type=None):
        print 'todo'

    def post_upgrade_restart(self, env, upgrade_type=None):
        print 'todo'

    def status(self, env):
        import status_params
        env.set_params(status_params)
        check_process_status(status_params.zk_pid_file)

    def security_status(self, env):
        import status_params
        env.set_params(status_params)

        if status_params.security_enabled:
            # Expect the following files to be available in params.config_dir:
            #   zookeeper_jaas.conf
            #   zookeeper_client_jaas.conf
            try:
                props_value_check = None
                props_empty_check = ['Server/keyTab', 'Server/principal']
                props_read_check = ['Server/keyTab']
                zk_env_expectations = build_expectations(
                    'zookeeper_jaas', props_value_check, props_empty_check,
                    props_read_check)

                zk_expectations = {}
                zk_expectations.update(zk_env_expectations)

                security_params = get_params_from_filesystem(
                    status_params.config_dir,
                    {'zookeeper_jaas.conf': FILE_TYPE_JAAS_CONF})

                result_issues = validate_security_config_properties(
                    security_params, zk_expectations)
                if not result_issues:  # If all validations passed successfully
                    # Double check the dict before calling execute
                    if ('zookeeper_jaas' not in security_params or
                            'Server' not in security_params['zookeeper_jaas']
                            or 'keyTab' not in
                            security_params['zookeeper_jaas']['Server'] or
                            'principal' not in
                            security_params['zookeeper_jaas']['Server']):
                        self.put_structured_out({"securityState": "ERROR"})
                        self.put_structured_out(
                            {"securityIssuesFound":
                             "Keytab file or principal are not set property."})
                        return

                    cached_kinit_executor(
                        status_params.kinit_path_local, status_params.zk_user,
                        security_params['zookeeper_jaas']['Server']['keyTab'],
                        security_params['zookeeper_jaas']['Server'][
                            'principal'], status_params.hostname,
                        status_params.tmp_dir)
                    self.put_structured_out(
                        {"securityState": "SECURED_KERBEROS"})
                else:
                    issues = []
                    for cf in result_issues:
                        issues.append(
                            "Configuration file %s did not pass the validation. Reason: %s"
                            % (cf, result_issues[cf]))
                    self.put_structured_out(
                        {"securityIssuesFound": ". ".join(issues)})
                    self.put_structured_out({"securityState": "UNSECURED"})
            except Exception as e:
                self.put_structured_out({"securityState": "ERROR"})
                self.put_structured_out({"securityStateErrorInfo": str(e)})
        else:
            self.put_structured_out({"securityState": "UNSECURED"})

    def get_log_folder(self):
        import params
        return params.zk_log_dir

    def get_user(self):
        import params
        return params.zk_user

    def get_pid_files(self):
        import status_params
        return [status_params.zk_pid_file]


if __name__ == "__main__":
    ZookeeperServer().execute()
