
from resource_management.core.exceptions import ComponentIsNotRunning
from resource_management.core.resources.system import File
from resource_management.core.source import Template
from resource_management.libraries.functions.format import format
from resource_management.libraries.script import Script
from resource_management.core.logger import Logger

from enrichment_commands import EnrichmentCommands
from metron_security import storm_security_setup
import metron_service
import metron_security
from metron_service import install_metron
class Enrichment(Script):

    def install(self, env):
        import params
        env.set_params(params)
        self.install_packages(env)
        install_metron()

    def configure(self, env, upgrade_type=None, config_dir=None):
        import params
        env.set_params(params)

        Logger.info("Running enrichment configure")
        File(format("{metron_config_path}/enrichment.properties"),
             content=Template("enrichment.properties.j2"),
             owner=params.metron_user,
             group=params.metron_group
             )

        if not metron_service.is_zk_configured(params):
          metron_service.init_zk_config(params)
          metron_service.set_zk_configured(params)
        metron_service.refresh_configs(params)

        Logger.info("Calling security setup")
        storm_security_setup(params)

    def start(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        self.configure(env)
        commands = EnrichmentCommands(params)

        if params.security_enabled:
            metron_security.kinit(params.kinit_path_local,
                                  params.metron_keytab_path,
                                  params.metron_principal_name,
                                  execute_user=params.metron_user)

        if not commands.is_kafka_configured():
            commands.init_kafka_topics()
        if params.security_enabled and not commands.is_kafka_acl_configured():
            commands.init_kafka_acls()
        if not commands.is_hbase_configured():
            commands.create_hbase_tables()
        if params.security_enabled and not commands.is_hbase_acl_configured():
            commands.set_hbase_acls()
        if not commands.is_geo_configured():
            commands.init_geo()

        commands.start_enrichment_topology(env)

    def stop(self, env, upgrade_type=None):
        import params

        env.set_params(params)
        commands = EnrichmentCommands(params)

        if params.security_enabled:
            metron_security.kinit(params.kinit_path_local,
                                  params.metron_keytab_path,
                                  params.metron_principal_name,
                                  execute_user=params.metron_user)

        commands.stop_enrichment_topology(env)

    def status(self, env):
        import status_params
        env.set_params(status_params)
        commands = EnrichmentCommands(status_params)

        if status_params.security_enabled:
            metron_security.kinit(status_params.kinit_path_local,
                                  status_params.metron_keytab_path,
                                  status_params.metron_principal_name,
                                  execute_user=status_params.metron_user)

        if not commands.is_topology_active(env):
            raise ComponentIsNotRunning()

    def restart(self, env):
        import params
        env.set_params(params)
        self.configure(env)
        commands = EnrichmentCommands(params)
        commands.restart_enrichment_topology(env)


if __name__ == "__main__":
    Enrichment().execute()
