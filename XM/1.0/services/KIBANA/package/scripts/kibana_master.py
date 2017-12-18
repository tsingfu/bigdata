import errno
import os

from resource_management.core.logger import Logger
from resource_management.core.resources.system import Directory
from resource_management.core.resources.system import Execute
from resource_management.core.resources.system import File
from resource_management.core.source import InlineTemplate
from resource_management.libraries.functions.format import format as ambari_format
from resource_management.libraries.script import Script


class Kibana(Script):
    def install(self, env):
        import params
        env.set_params(params)
        Logger.info("Install Kibana Master")
        self.install_packages(env)

    def configure(self, env, upgrade_type=None, config_dir=None):
        import params
        env.set_params(params)

        Logger.info("Configure Kibana for Metron")

        directories = [params.log_dir, params.pid_dir, params.conf_dir]
        Directory(directories,
                  # recursive=True,
                  mode=0755,
                  owner=params.kibana_user,
                  group=params.kibana_user
                  )

        File("{0}/kibana.yml".format(params.conf_dir),
             owner=params.kibana_user,
             content=InlineTemplate(params.kibana_yml_template)
             )

    def stop(self, env, upgrade_type=None):
        import params
        env.set_params(params)

        Logger.info("Stop Kibana Master")

        Execute("service kibana stop")

    def start(self, env, upgrade_type=None):
        import params
        env.set_params(params)

        self.configure(env)

        Logger.info("Start the Master")


        Execute("service kibana start")

    def restart(self, env):
        import params
        env.set_params(params)

        self.configure(env)

        Logger.info("Restarting the Master")

        Execute("service kibana restart")

    def status(self, env):
        import params
        env.set_params(params)

        Logger.info("Status of the Master")

        Execute("service kibana status")

    def load_template(self, env):
        from dashboard.dashboardindex import DashboardIndex

        import params
        env.set_params(params)

        hostname = ambari_format("{es_host}")
        port = int(ambari_format("{es_port}"))

        Logger.info("Connecting to Elasticsearch on host: %s, port: %s" % (hostname, port))
        di = DashboardIndex(host=hostname, port=port)

        # Loads Kibana Dashboard definition from disk and replaces .kibana on index
        templateFile = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dashboard', 'dashboard.p')
        if not os.path.isfile(templateFile):
            raise IOError(
                errno.ENOENT, os.strerror(errno.ENOENT), templateFile)

        Logger.info("Deleting .kibana index from Elasticsearch")

        di.es.indices.delete(index='.kibana', ignore=[400, 404])

        Logger.info("Loading .kibana index from %s" % templateFile)

        di.put(data=di.load(filespec=templateFile))


if __name__ == "__main__":
    Kibana().execute()
