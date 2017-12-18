#!/usr/bin/env python

import urllib2

from resource_management import *
import time
from resource_management.core.source import StaticFile, Template
from resource_management.core.resources.system import File
from resource_management.core.resources.system import Execute, Directory


def webhcat_service_check():
    import params
    File(format("{tmp_dir}/templetonSmoke.sh"),
         content=StaticFile('templetonSmoke.sh'),
         mode=0755
         )

    if params.security_enabled:
        smokeuser_keytab = params.smoke_user_keytab
        smoke_user_principal = params.smokeuser_principal
    else:
        smokeuser_keytab = "no_keytab"
        smoke_user_principal = "no_principal"

    unique_name = format("{smokeuser}.{timestamp}", timestamp=time.time())
    templeton_test_script = format("idtest.{unique_name}.pig")
    templeton_test_input = format("/tmp/idtest.{unique_name}.in")
    templeton_test_output = format("/tmp/idtest.{unique_name}.out")

    File(format("{tmp_dir}/{templeton_test_script}"),
         content=Template("templeton_smoke.pig.j2", templeton_test_input=templeton_test_input,
                          templeton_test_output=templeton_test_output),
         owner=params.hdfs_user
         )

    params.HdfsResource(format("/tmp/{templeton_test_script}"),
                        action="create_on_execute",
                        type="file",
                        source=format("{tmp_dir}/{templeton_test_script}"),
                        owner=params.smokeuser
                        )

    params.HdfsResource(templeton_test_input,
                        action="create_on_execute",
                        type="file",
                        source="/etc/passwd",
                        owner=params.smokeuser
                        )

    params.HdfsResource(None, action="execute")

    cmd = format(
        "{tmp_dir}/templetonSmoke.sh {webhcat_server_host[0]} {smokeuser} {templeton_port} {templeton_test_script} {has_pig} {smokeuser_keytab}"
        " {security_param} {kinit_path_local} {smoke_user_principal}"
        " {tmp_dir}")

    Execute(cmd,
            tries=3,
            try_sleep=5,
            path='/usr/sbin:/sbin:/usr/local/bin:/bin:/usr/bin',
            logoutput=True)
