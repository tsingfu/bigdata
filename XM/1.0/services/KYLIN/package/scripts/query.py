import glob
import os
from resource_management.core.resources import Directory
from resource_management.core.resources.system import Execute, File
from resource_management.core.source import InlineTemplate
from resource_management.core.logger import Logger
from resource_management.libraries.functions.check_process_status import check_process_status
from resource_management.libraries.functions.format import format
from resource_management.libraries.script.script import Script
from resource_management.libraries import XmlConfig
from job import install_kylin

class Query(Script):

    def install(self, env):
        import params
        env.set_params(params)
        install_kylin(first=True)
        self.create_kylin_log_dir(env)
        #self.create_kylin_dir()

    def create_kylin_dir(self):
        import params
        params.HdfsResource(
            format("/user/{kylin_user}"),
            type="directory",
            action="create_on_execute",
            owner=params.kylin_user,
            group=params.kylin_group,
            recursive_chown=True,
            recursive_chmod=True)
        params.HdfsResource(
            format("/user/{kylin_user}/spark-history"),
            type="directory",
            action="create_on_execute",
            owner=params.kylin_user,
            group=params.kylin_group,
            recursive_chown=True,
            recursive_chmod=True)
        params.HdfsResource(None, action="execute")

    def create_kylin_log_dir(self, env):
        import params
        env.set_params(params)
        Directory(
            [params.kylin_log_dir],
            owner=params.kylin_user,
            group=params.kylin_group,
            cd_access="a",
            create_parents=True,
            mode=0755)

    def chown_kylin_pid_dir(self, env):
        import params
        env.set_params(params)
        Execute(
            ("chown", "-R", format("{kylin_user}") + ":" +
             format("{kylin_group}"), params.kylin_pid_dir),
            sudo=True)

    def configure(self, env):
        import params
        env.set_params(params)
        self.create_kylin_log_dir(env)
        params.server_mode = 'query'

        # create the pid and kylin dirs
        Directory(
            [params.kylin_pid_dir, params.kylin_dir, params.conf_dir],
            owner=params.kylin_user,
            group=params.kylin_group,
            cd_access="a",
            create_parents=True,
            mode=0755)
        self.chown_kylin_pid_dir(env)

        File(
            os.path.join(params.conf_dir, "kylin.properties"),
            content=InlineTemplate(params.kylin_properties_template),
            owner=params.kylin_user,
            group=params.kylin_group)

        File(
            os.path.join(params.kylin_dir, "bin/check-env.sh"),
            mode=0755,
            content=InlineTemplate(params.kylin_check_env_template),
            owner=params.kylin_user,
            group=params.kylin_group)

        File(
            os.path.join(params.kylin_dir, "bin/kylin-env.sh"),
            mode=0644,
            content=InlineTemplate(params.kylin_env_template),
            owner=params.kylin_user,
            group=params.kylin_group)
        XmlConfig(
            "kylin_hive_conf.xml",
            conf_dir=params.conf_dir,
            configurations=params.config['configurations']['kylin_hive_conf'],
            owner=params.kylin_user, group=params.kylin_group)
        XmlConfig(
            "kylin_job_conf.xml",
            conf_dir=params.conf_dir,
            configurations=params.config['configurations']['kylin_job_conf'],
            owner=params.kylin_user, group=params.kylin_group)
        XmlConfig(
            "kylin_job_conf_inmem.xml",
            conf_dir=params.conf_dir,
            configurations=params.config['configurations']['kylin_job_conf_inmem'],
            owner=params.kylin_user, group=params.kylin_group)
        XmlConfig(
            "kylin-kafka-consumer.xml",
            conf_dir=params.conf_dir,
            configurations=params.config['configurations']['kylin-kafka-consumer'],
            owner=params.kylin_user, group=params.kylin_group)
        File(
            os.path.join(params.conf_dir, "kylin-server-log4j.properties"),
            mode=0755,
            group=params.kylin_group,
            owner=params.kylin_user,
            content=InlineTemplate(params.log4j_server_props))
        File(
            os.path.join(params.conf_dir, "kylin-tools-log4j.properties"),
            mode=0644,
            group=params.kylin_group,
            owner=params.kylin_user,
            content=InlineTemplate(params.log4j_tool_props))

    def stop(self, env, upgrade_type=None):
        import params
        self.create_kylin_log_dir(env)
        self.chown_kylin_pid_dir(env)
        Execute(
            params.kylin_dir + '/bin/kylin.sh stop >> ' +
            params.kylin_log_file,
            user=params.kylin_user)

    def start(self, env, upgrade_type=None):
        import params
        install_kylin()
        self.configure(env)

        if params.security_enabled:
            kylin_kinit_cmd = format(
                "{kinit_path_local} -kt {kylin_kerberos_keytab} {kylin_kerberos_principal}; ")
            Execute(kylin_kinit_cmd, user=params.kylin_user)

        Execute( ' source '+ params.kylin_dir + '/bin/kylin-env.sh ;' +
            params.kylin_dir + '/bin/kylin.sh start >> ' +
            params.kylin_log_file,
            user=params.kylin_user)
        pidfile = params.kylin_pid_file
        Logger.info(format("Pid file is: {pidfile}"))

    def status(self, env):
        import status_params
        env.set_params(status_params)
        check_process_status(status_params.kylin_pid_file)

    def get_pid_files(self):
        import params
        return [params.kylin_pid_file]


if __name__ == "__main__":
    Query().execute()