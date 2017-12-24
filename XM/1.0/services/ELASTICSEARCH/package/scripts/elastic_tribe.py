from resource_management.core.resources.system import Execute
from resource_management.libraries.script import Script
from resource_management.core.logger import Logger
from resource_management.libraries.functions.check_process_status import check_process_status

from resource_management.core.resources.system import Directory
from resource_management.core.resources.system import File
from resource_management.core.source import InlineTemplate


def elastic():
    import params

    params.data_dir = params.data_dir.replace('"', '')
    data_path = params.data_dir.replace(' ', '').split(',')
    data_path[:] = [x.replace('"', '') for x in data_path]

    params.data_dir = [params.log_dir, params.pid_dir, params.conf_dir]

    Directory(params.data_dir,
              create_parents=True,
              mode=0755,
              owner=params.elastic_user,
              group=params.elastic_group
              )

    File("{0}/elastic-env.sh".format(params.conf_dir),
         owner=params.elastic_user,
         group=params.elastic_group,
         content=InlineTemplate(params.elastic_env_sh_template)
         )

    File(format(params.conf_dir + "/elasticsearch.yml"),
         content=InlineTemplate(params.tribe_content),
         mode=0755,
         owner=params.elastic_user,
         group=params.elastic_group
         )

    File(format(params.conf_dir + "/jvm.options"),
         content=InlineTemplate(params.jvm_content),
         mode=0755,
         owner=params.elastic_user,
         group=params.elastic_group
         )

    File("/etc/sysconfig/elasticsearch",
         owner=params.elastic_user,
         group=params.elastic_group,
         mode=0777,
         content=InlineTemplate(params.sysconfig_template)
         )


class Elasticsearch(Script):
    def install(self, env):
        import params
        env.set_params(params)
        Logger.info('Install ES Tribe Node')
        self.install_packages(env)
        Execute('echo "* soft nproc 8192" > /etc/security/limits.d/es.conf')
        Execute('mkdir -p /etc/sysctl.d && echo "vm.max_map_count=655360" > /etc/sysctl.d/11-es.conf ')

    def configure(self, env, upgrade_type=None, config_dir=None):
        import params
        env.set_params(params)

        elastic()

    def stop(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        stop_cmd = "source " + params.conf_dir + "/elastic-env.sh;service elasticsearch stop"
        Execute(stop_cmd)

    def start(self, env, upgrade_type=None):
        import params
        env.set_params(params)

        self.configure(env)
        start_cmd = "source " + params.conf_dir + "/elastic-env.sh;service elasticsearch start"
        Execute(start_cmd)

    def status(self, env):
        import status_params
        check_process_status(status_params.elastic_pid_file)

    def restart(self, env):
        import params
        env.set_params(params)
        self.configure(env)
        restart_cmd = "source " + params.conf_dir + "/elastic-env.sh;service elasticsearch restart"
        Execute(restart_cmd)


if __name__ == "__main__":
    Elasticsearch().execute()
