#!/usr/bin/env python

from resource_management.libraries.script.script import Script
from resource_management.libraries.functions.format import format
from resource_management.core.resources.system import Execute

def copy_to_hdfs(name, user_group, owner,custom_source_file=None, custom_dest_file=None):
    import os
    import params
    dest_dir = os.path.dirname(custom_dest_file)
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

class SliderServiceCheck(Script):
    def service_check(self, env):
        import params
        env.set_params(params)

        Execute('wget http://yum.example.com/hadoop/hdfs/slider.tar.gz  -O /tmp/slider.tar.gz')
        copy_to_hdfs("slider", params.user_group, params.hdfs_user,
                                        custom_source_file='/tmp/slider.tar.gz',
                                        custom_dest_file='/apps/slider/slider.tar.gz')
        params.HdfsResource(None, action="execute")

        Execute('/bin/rm -f /tmp/slider.tar.gz')

        smokeuser_kinit_cmd = format(
            "{kinit_path_local} -kt {smokeuser_keytab} {smokeuser_principal};") if params.security_enabled else ""

        servicecheckcmd = format("{smokeuser_kinit_cmd} {slider_cmd} list")

        Execute(servicecheckcmd,
                tries=3,
                try_sleep=5,
                user=params.smokeuser,
                logoutput=True
                )


if __name__ == "__main__":
    SliderServiceCheck().execute()
