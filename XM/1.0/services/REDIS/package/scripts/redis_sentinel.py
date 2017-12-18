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

from resource_management import *
from resource_management.core.resources.system import Directory, File
from resource_management.core.source import InlineTemplate, Template

def redis_sentinel():
    import params
    Directory(['/var/log/redis', '/var/run/redis',params.dbdir],
              owner=params.redis_user,
              mode=0775,
              create_parents=True,
              cd_access="a", )
    File(
        format("{conf_dir}/redis-sentinel.conf"),
        content=Template("redis-sentinel.conf.j2"),
        owner=params.redis_user,
        group=params.user_group,
        mode=0666)
