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
from resource_management.libraries.functions import check_process_status
from resource_management.libraries.script.script import Script
from accumulo_configuration import setup_conf_dir
from accumulo_service import accumulo_service

from resource_management.core.resources.system import Directory, Execute, File
import os


def install_accumulo(first=False):
    import params
    # if first:
    #     Execute('rm -rf %s' %  '/opt/' + params.version_dir)
    #     Execute('rm -rf %s' % params.install_dir)
    #     Execute('rm -rf %s' % params.conf_dir)
    print "install dir:" + params.install_dir
    Directory(
        [params.conf_dir],
        owner=params.accumulo_user,
        group=params.user_group,
        mode=0775,
        create_parents=True)

    if not os.path.exists('/opt/' + params.version_dir) or not os.path.exists(params.install_dir):
        Execute('rm -rf %s' % '/opt/' + params.version_dir)
        Execute('rm -rf %s' % params.install_dir)
        Execute(
            'wget ' + params.download_url + ' -O /tmp/' + params.filename,
            user=params.accumulo_user)
        Execute('tar -zxf /tmp/' + params.filename + ' -C /opt')
        Execute('ln -s /opt/' + params.version_dir + ' ' + params.install_dir)
        Execute(
            ' mv ' + params.install_dir + '/conf/* ; ' + params.conf_dir + ' rm -rf ' + params.install_dir + '/conf')
        Execute('ln -s ' + params.conf_dir + ' ' + params.install_dir +
                '/conf')
        Execute("echo 'export PATH=%s/bin:$PATH'>>/etc/profile.d/hadoop.sh" %
                params.install_dir)
        Execute('chown -R %s:%s /opt/%s' %
                (params.accumulo_user, params.user_group, params.version_dir))
        Execute('chown -R %s:%s %s' %
                (params.accumulo_user, params.user_group, params.install_dir))
        Execute('/bin/rm -f /tmp/' + params.filename)


class AccumuloScript(Script):
    # a mapping between the component named used by these scripts and the name
    # which is used by <stack-selector-tool>
    COMPONENT_TO_STACK_SELECT_MAPPING = {
        "gc": "accumulo-gc",
        "master": "accumulo-master",
        "monitor": "accumulo-monitor",
        "tserver": "accumulo-tablet",
        "tracer": "accumulo-tracer"
    }

    def __init__(self, component):
        self.component = component

    def get_component_name(self):
        """
        Gets the <stack-selector-tool> component name given the script component
        :return:  the name of the component on the stack which is used by
                  <stack-selector-tool>
        """
        if self.component not in self.COMPONENT_TO_STACK_SELECT_MAPPING:
            return None

        stack_component = self.COMPONENT_TO_STACK_SELECT_MAPPING[self.component]
        return stack_component

    def install(self, env):
        install_accumulo(first=True)

    def configure(self, env):
        import params
        env.set_params(params)

        setup_conf_dir(name=self.component)

    def start(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        install_accumulo()
        self.configure(env)  # for security

        accumulo_service(self.component, action='start')

    def stop(self, env, upgrade_type=None):
        import params
        env.set_params(params)

        accumulo_service(self.component, action='stop')

    def status(self, env):
        import status_params
        env.set_params(status_params)

        pid_file = self.get_pid_files()[0]
        check_process_status(pid_file)

    def pre_upgrade_restart(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        print 'todo'

    def get_log_folder(self):
        import params
        return params.log_dir

    def get_pid_files(self):
        import status_params

        pid_file = "{pid_dir}/accumulo-{accumulo_user}-{component}.pid".format(
            pid_dir=status_params.pid_dir,
            accumulo_user=status_params.accumulo_user,
            component=self.component)
        return [pid_file]

    def get_user(self):
        import params
        return params.accumulo_user


if __name__ == "__main__":
    AccumuloScript().fail_with_error('component unspecified')
