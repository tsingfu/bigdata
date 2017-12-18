import os
from resource_management.libraries.script import Script
from resource_management.libraries.resources.execute_hadoop import ExecuteHadoop
from resource_management.libraries.functions import format
from resource_management.core.resources.system import File, Execute

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

class TezServiceCheck(Script):
    def service_check(self, env):
        import params
        env.set_params(params)
        path_to_tez_jar = format(params.tez_examples_jar)
        wordcount_command = format(
            "jar {path_to_tez_jar} orderedwordcount /tmp/tezsmokeinput/sample-tez-test /tmp/tezsmokeoutput/")
        test_command = format("fs -test -e /tmp/tezsmokeoutput/_SUCCESS")

        File(format("{tmp_dir}/sample-tez-test"),
             content="foo\nbar\nfoo\nbar\nfoo",
             mode=0755
             )

        params.HdfsResource("/tmp/tezsmokeoutput",
                            action="delete_on_execute",
                            type="directory"
                            )

        params.HdfsResource("/tmp/tezsmokeinput",
                            action="create_on_execute",
                            type="directory",
                            owner=params.smokeuser,
                            )
        params.HdfsResource("/tmp/tezsmokeinput/sample-tez-test",
                            action="create_on_execute",
                            type="file",
                            owner=params.smokeuser,
                            source=format("{tmp_dir}/sample-tez-test"),
                            )

        Execute('wget http://yum.example.com/hadoop/hdfs/tez.tar.gz  -O /tmp/tez.tar.gz')
        copy_to_hdfs("tez", params.user_group, params.hdfs_user,
                                        custom_source_file='/tmp/tez.tar.gz',
                                        custom_dest_file='/apps/tez/tez.tar.gz')
        params.HdfsResource(None, action="execute")

        Execute('/bin/rm -f /tmp/tez.tar.gz')

        if params.security_enabled:
            kinit_cmd = format("{kinit_path_local} -kt {smoke_user_keytab} {smokeuser_principal};")
            Execute(kinit_cmd,
                    user=params.smokeuser
                    )

        ExecuteHadoop(wordcount_command,
                      tries=3,
                      try_sleep=5,
                      user=params.smokeuser,
                      environment={'JAVA_HOME': params.java64_home},
                      conf_dir=params.hadoop_conf_dir,
                      bin_dir=params.hadoop_bin_dir
                      )

        ExecuteHadoop(test_command,
                      tries=10,
                      try_sleep=6,
                      user=params.smokeuser,
                      environment={'JAVA_HOME': params.java64_home},
                      conf_dir=params.hadoop_conf_dir,
                      bin_dir=params.hadoop_bin_dir
                      )


if __name__ == "__main__":
    TezServiceCheck().execute()
