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
import os
from resource_management.libraries.resources.xml_config import XmlConfig
from resource_management.core.source import Template, InlineTemplate
from resource_management.libraries.functions.format import format
from resource_management.core.resources.system import Directory, Execute, File


def copy_to_hdfs(name, user_group, owner, custom_source_file=None, custom_dest_file=None):
    import os
    import params
    #dest_dir = os.path.dirname(custom_dest_file)
    dest_dir = '/'.join(custom_dest_file.split('/')[:-1])
    params.HdfsResource(dest_dir,
                        type="directory",
                        action="create_on_execute",
                        owner=owner,
                        mode=0555
                        )

    params.HdfsResource(custom_dest_file,
                        type="file",
                        action="create_on_execute",
                        source=custom_source_file,
                        group=user_group,
                        owner=owner,
                        mode=0444,
                        replace_existing_files=False,
                        )
    params.HdfsResource(None, action="execute")


def install_slider(first=False):
    import params
    Directory(
        [params.slider_conf_dir],
        owner=params.user_group,
        group=params.user_group,
        mode=0775,
        create_parents=True)
    if not os.path.exists('/opt/' + params.version_dir) or not os.path.exists(params.install_dir):
        Execute('rm -rf %s' % '/opt/' + params.version_dir)
        Execute('rm -rf %s' % params.install_dir)
        Execute(
            'wget ' + params.download_url + ' -O /tmp/' + params.filename,
            user=params.user_group)
        Execute('tar -zxf /tmp/' + params.filename + ' -C /opt')
        Execute('ln -s /opt/' + params.version_dir + ' ' + params.install_dir)
        Execute(' rm -rf ' + params.install_dir + '/conf')
        Execute('ln -s ' + params.slider_conf_dir + ' ' + params.install_dir +
                '/conf')
        Execute("echo 'export PATH=%s/bin:$PATH'>>/etc/profile.d/hadoop.sh" %
                params.install_dir)
        Execute('chown -R %s:%s /opt/%s' %
                (params.user_group, params.user_group, params.version_dir))
        Execute('chown -R %s:%s %s' %
                (params.user_group, params.user_group, params.install_dir))

        Execute('wget http://yum.example.com/hadoop/hdfs/slider.tar.gz  -O /tmp/slider.tar.gz')
        copy_to_hdfs("slider", params.user_group, params.hdfs_user,
                                        custom_source_file='/tmp/slider.tar.gz',
                                        custom_dest_file='/apps/slider/slider.tar.gz')
        params.HdfsResource(None, action="execute")

        Execute('/bin/rm -f /tmp/slider.tar.gz')


def slider():
    import params

    Directory(params.slider_conf_dir,
              create_parents=True
              )

    slider_client_config = params.config['configurations'][
        'slider-client'] if 'configurations' in params.config and 'slider-client' in params.config[
        'configurations'] else {}

    XmlConfig("slider-client.xml",
              conf_dir=params.slider_conf_dir,
              configurations=slider_client_config,
              mode=0644
              )

    File(format("{slider_conf_dir}/slider-env.sh"),
         mode=0755,
         content=InlineTemplate(params.slider_env_sh_template)
         )

    File(format("{slider_conf_dir}/storm-slider-env.sh"),
         mode=0755,
         content=Template('storm-slider-env.sh.j2')
         )

    if (params.log4j_props != None):
        File(format("{params.slider_conf_dir}/log4j.properties"),
             mode=0644,
             content=params.log4j_props
             )
    elif (os.path.exists(format("{params.slider_conf_dir}/log4j.properties"))):
        File(format("{params.slider_conf_dir}/log4j.properties"),
             mode=0644
             )
    File(params.slider_tar_gz,
         owner=params.hdfs_user,
         group=params.user_group,
         )
