from resource_management import *
from resource_management.core.resources.system import Directory, Execute, File
from resource_management.libraries.script.script import Script
from resource_management.core.source import Template, InlineTemplate
from resource_management.libraries.resources.xml_config import XmlConfig
from resource_management.libraries.functions.check_process_status import check_process_status
import os


def create_hdfs_dir():
    import params
    params.HdfsResource(params.history_log_dir,
                        type="directory",
                        action="create_on_execute",
                        owner=params.xlearning_user,
                        mode=0555
                        )
    params.HdfsResource(params.tf_board_history_dir,
                        type="directory",
                        action="create_on_execute",
                        owner=params.xlearning_user,
                        mode=0555
                        )
    params.HdfsResource(params.xlearning_staging_dir,
                        type="directory",
                        action="create_on_execute",
                        owner=params.xlearning_user,
                        mode=0555
                        )
    params.HdfsResource(None, action="execute")


def install_xlearning():
    import params
    Directory(
        [params.conf_dir, params.xlearning_pid_dir, params.xlearning_log_dir],
        owner=params.xlearning_user,
        group=params.xlearning_group,
        mode=0775,
        create_parents=True)
    if not os.path.exists('/opt/' + params.version_dir) or not os.path.exists(params.install_dir):
        Execute('rm -rf %s' % '/opt/' + params.version_dir)
        Execute('rm -rf %s' % params.install_dir)
        Execute(
            'wget ' + params.download_url + ' -O /tmp/' + params.filename,
            user=params.xlearning_user)
        Execute('tar -zxf /tmp/' + params.filename + ' -C /opt')
        Execute('ln -s /opt/' + params.version_dir + ' ' + params.install_dir)
        Execute('chown -R %s:%s /opt/%s' %
                (params.xlearning_user, params.xlearning_group, params.version_dir))
        Execute('chown -R %s:%s %s' %
                (params.xlearning_user, params.xlearning_group, params.install_dir))
        Execute('/bin/rm -f /tmp/' + params.filename)


class Master(Script):
    def install(self, env):
        self.install_packages(env)
        install_xlearning()
        self.configure(env)
        create_hdfs_dir()

    def configure(self, env):
        import params
        env.set_params(params)
        Directory(params.conf_dir,
                  owner=params.xlearning_user,
                  group=params.xlearning_group,
                  create_parents=True)
        File(format("{conf_dir}/xlearning-env.sh"),
             content=InlineTemplate(params.env_content),
             mode=0755,
             owner=params.xlearning_user,
             group=params.xlearning_group
             )
        File(format("{conf_dir}/log4j.properties"),
             content=InlineTemplate(params.log_content),
             mode=0755,
             owner=params.xlearning_user,
             group=params.xlearning_group
             )
        XmlConfig("xlearning-site.xml",
                  conf_dir=params.conf_dir,
                  configurations=params.config['configurations']['xlearning-site'],
                  configuration_attributes=params.config['configuration_attributes']['xlearning-site'],
                  owner=params.xlearning_user,
                  group=params.xlearning_group,
                  mode=0664)

    def stop(self, env):
        pass

    def start(self, env):
        install_xlearning()
        self.configure(env)
        import params
        Execute("source %s/xlearning-env.sh; %s/sbin/start-history-server.sh" % (params.conf_dir, params.install_dir))

    def status(self, env):
        import params
        env.set_params(params)
        check_process_status(params.xlearning_pid_file)


if __name__ == "__main__":
    Master().execute()
