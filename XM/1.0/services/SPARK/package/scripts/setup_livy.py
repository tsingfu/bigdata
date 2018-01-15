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

import os
from resource_management import Directory, File, PropertiesFile, InlineTemplate, format
from resource_management.core.resources.system import Execute


def install_livy():
    import params
    Directory(
        [params.livy_conf],
        owner=params.livy_user,
        group=params.user_group,
        mode=0775,
        create_parents=True)
    if (not os.path.exists('/opt/' + params.livy_version_dir) or not os.path.exists(params.livy_install_dir)) and params.has_livyserver:
        Execute('rm -rf %s' % '/opt/' + params.livy_version_dir)
        Execute('rm -rf %s' % params.livy_install_dir)
        Execute(
            'wget ' + params.livy_download_url + ' -O /tmp/' +
            params.livy_filename,
            user=params.livy_user)
        Execute('tar -zxf /tmp/' + params.livy_filename + ' -C /opt')
        Execute('ln -s /opt/' + params.livy_version_dir + ' ' +
                params.livy_install_dir)
        Execute(' rm -rf ' + params.livy_install_dir + '/conf')
        Execute('ln -s ' + params.livy_conf + ' ' + params.livy_install_dir +
                '/conf')
        Execute('chown -R %s:%s /opt/%s' %
                (params.livy_user, params.livy_group, params.livy_version_dir))
        Execute('chown -R %s:%s %s' %
                (params.livy_user, params.livy_group, params.livy_install_dir))
        Execute('/bin/rm -f /tmp/' + params.filename)


def setup_livy(env, type, upgrade_type=None, action=None):
    import params

    Directory(
        [params.livy_pid_dir, params.livy_log_dir],
        owner=params.livy_user,
        group=params.user_group,
        mode=0775,
        create_parents=True)
    if type == 'server' and action == 'config':
        params.HdfsResource(
            params.livy_hdfs_user_dir,
            type="directory",
            action="create_on_execute",
            owner=params.livy_user,
            mode=0775)
        params.HdfsResource(None, action="execute")

        params.HdfsResource(
            params.livy_recovery_dir,
            type="directory",
            action="create_on_execute",
            owner=params.livy_user,
            mode=0700)
        params.HdfsResource(None, action="execute")

    # create livy-env.sh in etc/conf dir
    File(
        os.path.join(params.livy_conf, 'livy-env.sh'),
        owner=params.livy_user,
        group=params.livy_group,
        content=InlineTemplate(params.livy_env_sh),
        mode=0644, )

    # create livy.conf in etc/conf dir
    PropertiesFile(
        format("{livy_conf}/livy.conf"),
        properties=params.config['configurations']['livy-conf'],
        key_value_delimiter=" ",
        owner=params.livy_user,
        group=params.livy_group, )

    # create log4j.properties in etc/conf dir
    File(
        os.path.join(params.livy_conf, 'log4j.properties'),
        owner=params.livy_user,
        group=params.livy_group,
        content=params.livy_log4j_properties,
        mode=0644, )

    # create spark-blacklist.properties in etc/conf dir
    File(
        os.path.join(params.livy_conf, 'spark-blacklist.conf'),
        owner=params.livy_user,
        group=params.livy_group,
        content=params.livy_spark_blacklist_properties,
        mode=0644, )

    Directory(
        params.livy_logs_dir,
        owner=params.livy_user,
        group=params.livy_group,
        mode=0755, )
