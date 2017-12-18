from resource_management import *

from resource_management.core.resources.system import Directory, File, Link
import os
from resource_management.core.source import InlineTemplate
from resource_management.core.resources.system import Execute
from resource_management.core import sudo
from resource_management.libraries.functions.check_process_status import check_process_status
from resource_management.libraries.functions.format import format
from resource_management.libraries.script.script import Script
from client import install_flink

class Master(Script):
    def install(self, env):
        install_flink(first=True)
        self.configure(env, True)

    def configure(self, env, isInstall=False):
        import params
        import status_params
        env.set_params(params)
        env.set_params(status_params)
        Directory(
            [status_params.flink_pid_dir, params.flink_log_dir,
             params.install_dir],
            owner=params.flink_user,
            group=params.flink_group)

        File(
            params.flink_log_file,
            mode=0644,
            owner=params.flink_user,
            group=params.flink_group,
            content='')
        Directory(
            [params.conf_dir],
            owner=params.flink_user,
            group=params.flink_group)
        #write out nifi.properties
        properties_content = InlineTemplate(params.flink_yaml_content)
        File(
            format("{conf_dir}/flink-conf.yaml"),
            content=properties_content,
            owner=params.flink_user)

    def stop(self, env):
        import params
        import status_params
        pid = str(sudo.read_file(status_params.flink_pid_file))
        Execute(
            params.hadoop_bin_dir + '/yarn application -kill ' + pid,
            user=params.flink_user)
        Execute('rm ' + status_params.flink_pid_file, ignore_failures=True)

    def start(self, env):
        import params
        import status_params
        install_flink()
        self.configure(env)
        params.HdfsResource(
            params.flink_dir,
            type="directory",
            action="create_on_execute",
            owner=params.flink_user,
            mode=0755)
        params.HdfsResource(
            params.flink_checkpoints_dir,
            type="directory",
            action="create_on_execute",
            owner=params.flink_user,
            mode=0755)
        params.HdfsResource(
            params.flink_recovery_dir,
            type="directory",
            action="create_on_execute",
            owner=params.flink_user,
            mode=0755)
        params.HdfsResource(None, action="execute")

        if params.security_enabled:
            flink_kinit_cmd = format(
                "{kinit_path_local} -kt {flink_kerberos_keytab} {flink_kerberos_principal}; ")
            Execute(flink_kinit_cmd, user=params.flink_user)
            
        Execute('echo bin dir ' + params.bin_dir)
        Execute('echo pid file ' + status_params.flink_pid_file)
        cmd = format(
            "export HADOOP_CONF_DIR={hadoop_conf_dir}; {bin_dir}/yarn-session.sh -n {flink_numcontainers} -s {flink_numberoftaskslots} -jm {flink_jobmanager_memory} -tm {flink_container_memory} -qu {flink_queue} -nm {flink_appname} -d")
        if params.flink_streaming:
            cmd = cmd + ' -st '
        Execute(cmd + format(" >> {flink_log_file}"), user=params.flink_user)
        Execute(
            params.hadoop_bin_dir +
            "/yarn application -list 2>/dev/null | awk '/" +
            params.flink_appname + "/ {print $1}' | head -n1 > " +
            status_params.flink_pid_file,
            user=params.flink_user)

    def status(self, env):
        import status_params
        env.set_params(status_params)
        check_process_status(status_params.flink_pid_file)


if __name__ == "__main__":
    Master().execute()
