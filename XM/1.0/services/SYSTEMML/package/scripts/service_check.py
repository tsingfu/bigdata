#!/usr/bin/env python

from resource_management import *
from resource_management.libraries.functions.format import format
import subprocess
import os
from resource_management.libraries.script.script import Script


class SystemMLServiceCheck(Script):
    def service_check(self, env):
        import params
        env.set_params(params)

        if os.path.exists(params.systemml_lib_dir):
            cp = format(
                "{params.stack_root}/hadoop/*:{params.stack_root}/hadoop/share/hadoop/mapreduce/*:{params.stack_root}/hadoop/lib/*:{params.systemml_lib_dir}/systemml.jar")
            java = format("{params.java_home}/bin/java")
            command = [java, "-cp", cp, "org.apache.sysml.api.DMLScript", "-s", "print('Apache SystemML');"]
            process = subprocess.Popen(command, stdout=subprocess.PIPE)
            output = process.communicate()[0]
            print output

            if 'Apache SystemML' not in output:
                raise Fail("Expected output Apache SystemML not found.")


if __name__ == "__main__":
    SystemMLServiceCheck().execute()
