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
from resource_management.core.exceptions import Fail
from resource_management.core.resources.system import Directory, Execute, File, Link
from resource_management.core.source import InlineTemplate
from resource_management.libraries.resources.template_config import TemplateConfig
from resource_management.libraries.functions.format import format
from resource_management.libraries.script.script import Script
from resource_management.core.source import Template
from storm_yaml_utils import yaml_config_template
from resource_management.core.source import StaticFile
from resource_management.core.resources.system import Execute
import os, sys
from ambari_commons.constants import SERVICE


def install_storm():
    import params
    Directory(
        [params.conf_dir],
        owner=params.storm_user,
        group=params.user_group,
        mode=0775,
        create_parents=True)
    if not os.path.exists('/opt/' + params.version_dir) or not os.path.exists(params.install_dir):
        Execute('rm -rf %s' %  '/opt/' + params.version_dir)
        Execute('rm -rf %s' % params.install_dir)
        Execute(
            'wget ' + params.download_url + ' -O /tmp/' + params.filename,
            user=params.storm_user)
        Execute('tar -zxf /tmp/' + params.filename + ' -C /opt')
        Execute('ln -s /opt/' + params.version_dir + ' ' + params.install_dir)
        Execute(' rm -rf ' + params.install_dir + '/conf')
        Execute('ln -s ' + params.conf_dir + ' ' + params.install_dir +
                '/conf')
        Execute("echo 'export PATH=%s/bin:$PATH'>>/etc/profile.d/hadoop.sh" %
                params.install_dir)
        Execute('chown -R %s:%s /opt/%s' %
                (params.storm_user, params.user_group, params.version_dir))
        Execute('chown -R %s:%s %s' %
                (params.storm_user, params.user_group, params.install_dir))
        Execute('/bin/rm -f /tmp/' + params.filename)


def storm(name=None):
    import params
    import os

    Directory(
        params.log_dir,
        owner=params.storm_user,
        group=params.user_group,
        mode=0777,
        create_parents=True,
        cd_access="a", )

    Directory(
        [params.pid_dir, params.local_dir],
        owner=params.storm_user,
        group=params.user_group,
        create_parents=True,
        cd_access="a",
        mode=0755, )

    Directory(
        params.conf_dir,
        group=params.user_group,
        create_parents=True,
        cd_access="a", )

    File(
        format("{conf_dir}/config.yaml"),
        content=Template("config.yaml.j2"),
        owner=params.storm_user,
        group=params.user_group)

    File(
        params.conf_dir + "/jmxetric-conf.xml",
        content=StaticFile("jmxetric-conf.xml"),
        owner=params.storm_user)
    File(
        params.storm_lib_dir + "/gmetric4j-1.0.3.jar",
        content=StaticFile("gmetric4j-1.0.3.jar"),
        owner=params.storm_user)
    File(
        params.storm_lib_dir + "/jmxetric-1.0.4.jar",
        content=StaticFile("jmxetric-1.0.4.jar"),
        owner=params.storm_user)
    File(
        params.storm_lib_dir + "/oncrpc-1.0.7.jar",
        content=StaticFile("oncrpc-1.0.7.jar"),
        owner=params.storm_user)

    configurations = params.config['configurations']['storm-site']

    File(
        format("{conf_dir}/storm.yaml"),
        content=yaml_config_template(configurations),
        owner=params.storm_user,
        group=params.user_group)

    File(
        format("{conf_dir}/storm-env.sh"),
        owner=params.storm_user,
        content=InlineTemplate(params.storm_env_sh_template))

    # Generate atlas-application.properties.xml file and symlink the hook jars
    if params.enable_atlas_hook:
        script_path = os.path.realpath(__file__).split('/services')[0] + '/hooks/before-INSTALL/scripts/atlas'
        sys.path.append(script_path)
        from setup_atlas_hook import has_atlas_in_cluster, setup_atlas_hook, setup_atlas_jar_symlinks
        atlas_hook_filepath = os.path.join(params.conf_dir, params.atlas_hook_filename)
        setup_atlas_hook(SERVICE.STORM, params.storm_atlas_application_properties, atlas_hook_filepath,
                         params.storm_user, params.user_group)
        storm_extlib_dir = os.path.join(params.storm_component_home_dir, "extlib")
        setup_atlas_jar_symlinks("storm", storm_extlib_dir)

    if params.has_metric_collector:
        File(
            format("{conf_dir}/storm-metrics2.properties"),
            owner=params.storm_user,
            group=params.user_group,
            content=Template("storm-metrics2.properties.j2"))

        # Remove symlinks. They can be there, if you doing upgrade from HDP < 2.2 to HDP >= 2.2
        Link(
            format("{storm_lib_dir}/ambari-metrics-storm-sink.jar"),
            action="delete")
        # On old HDP 2.1 versions, this symlink may also exist and break EU to newer versions
        Link(
            "/usr/lib/storm/lib/ambari-metrics-storm-sink.jar",
            action="delete")

        sink_jar = params.sink_jar

        Execute(
            format(
                "{sudo} ln -s {sink_jar} {storm_lib_dir}/ambari-metrics-storm-sink.jar"),
            not_if=format("ls {storm_lib_dir}/ambari-metrics-storm-sink.jar"),
            only_if=format("ls {sink_jar}"))

    if params.storm_logs_supported:
        Directory(
            params.log4j_dir,
            owner=params.storm_user,
            group=params.user_group,
            mode=0755,
            create_parents=True)

        File(
            format("{log4j_dir}/cluster.xml"),
            owner=params.storm_user,
            content=InlineTemplate(params.storm_cluster_log4j_content))
        File(
            format("{log4j_dir}/worker.xml"),
            owner=params.storm_user,
            content=InlineTemplate(params.storm_worker_log4j_content))

    if params.security_enabled:
        TemplateConfig(
            format("{conf_dir}/storm_jaas.conf"), owner=params.storm_user)

        TemplateConfig(
            format("{conf_dir}/client_jaas.conf"), owner=params.storm_user)
        minRuid = configurations[
            '_storm.min.ruid'] if configurations.has_key(
            '_storm.min.ruid') else ''

        min_user_ruid = int(minRuid) if minRuid.isdigit(
        ) else _find_real_user_min_uid()

        File(
            format("{conf_dir}/worker-launcher.cfg"),
            content=Template(
                "worker-launcher.cfg.j2", min_user_ruid=min_user_ruid),
            owner='root',
            group=params.user_group)


'''
Finds minimal real user UID
'''


def _find_real_user_min_uid():
    with open('/etc/login.defs') as f:
        for line in f:
            if line.strip().startswith('UID_MIN') and len(line.split(
            )) == 2 and line.split()[1].isdigit():
                return int(line.split()[1])
    raise Fail(
        "Unable to find UID_MIN in file /etc/login.defs. Expecting format e.g.: 'UID_MIN    500'")
