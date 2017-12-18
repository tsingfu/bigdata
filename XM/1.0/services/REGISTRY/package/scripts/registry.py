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
import collections
import os

from resource_management.core.resources.system import Directory, Execute, File, Link
from resource_management.core.source import StaticFile, Template, InlineTemplate, DownloadSource
from resource_management.libraries.functions import format
from resource_management.libraries.resources.template_config import TemplateConfig

def registry(env, upgrade_type=None):
    import params
    ensure_base_directories()

    File(format("{conf_dir}/registry-env.sh"),
          owner=params.registry_user,
          content=InlineTemplate(params.registry_env_sh_template)
     )

    File(params.bootstrap_storage_command,
         owner=params.registry_user,
         content=InlineTemplate(params.bootstrap_sh_template),
         mode=0755
    )

    if params.security_enabled:
        if params.registry_jaas_conf_template:
            File(format("{conf_dir}/registry_jaas.conf"),
                 owner=params.registry_user,
                 content=InlineTemplate(params.registry_jaas_conf_template))
        else:
            TemplateConfig(format("{conf_dir}/registry_jaas.conf"),
                           owner=params.registry_user)
    # On some OS this folder could be not exists, so we will create it before pushing there files
    Directory(params.limits_conf_dir,
              create_parents = True,
              owner='root',
              group='root'
    )

    Directory([params.jar_storage],
            owner=params.registry_user,
            group=params.user_group,
            create_parents = True,
            cd_access="a",
            mode=0755,
    )


    File(os.path.join(params.limits_conf_dir, 'registry.conf'),
         owner='root',
         group='root',
         mode=0644,
         content=Template("registry.conf.j2")
    )
    
    File(format("{conf_dir}/registry.yaml"),
         content=Template("registry.yaml.j2"),
         owner=params.registry_user,
         group=params.user_group,
         mode=0644
    )

    if not os.path.islink(params.registry_managed_log_dir):
      Link(params.registry_managed_log_dir,
           to=params.registry_log_dir)

def ensure_base_directories():
  import params
  import status_params
  Directory([params.registry_log_dir, status_params.registry_pid_dir, params.conf_dir, params.registry_agent_dir],
            mode=0755,
            cd_access='a',
            owner=params.registry_user,
            group=params.user_group,
            create_parents = True,
            recursive_ownership = True,
            )