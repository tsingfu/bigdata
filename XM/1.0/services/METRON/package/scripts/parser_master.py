from resource_management.core.exceptions import ComponentIsNotRunning
from resource_management.core.logger import Logger
from resource_management.libraries.script import Script

from metron_security import storm_security_setup
import metron_service
from parser_commands import ParserCommands
from metron_service import install_metron

class ParserMaster(Script):
    def get_component_name(self):
        pass

    def install(self, env):
        import params
        env.set_params(params)
        self.install_packages(env)
        install_metron()

    def configure(self, env, upgrade_type=None, config_dir=None):
        import params
        env.set_params(params)

        if not metron_service.is_zk_configured(params):
            metron_service.init_zk_config(params)
            metron_service.set_zk_configured(params)
        metron_service.refresh_configs(params)
        commands = ParserCommands(params)
        if not commands.is_configured():
            commands.init_parsers()
            commands.init_kafka_topics()
            commands.set_configured()
        if params.security_enabled and not commands.is_acl_configured():
            commands.init_kafka_acls()
            commands.set_acl_configured()

        Logger.info("Calling security setup")
        storm_security_setup(params)

    def start(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        self.configure(env)
        commands = ParserCommands(params)
        commands.start_parser_topologies(env)

    def stop(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        commands = ParserCommands(params)
        commands.stop_parser_topologies(env)

    def status(self, env):
        import status_params
        env.set_params(status_params)
        commands = ParserCommands(status_params)
        if not commands.topologies_running(env):
            raise ComponentIsNotRunning()

    def restart(self, env):
        import params
        env.set_params(params)
        self.configure(env)
        commands = ParserCommands(params)
        commands.restart_parser_topologies(env)

    def servicechecktest(self, env):
        import params
        env.set_params(params)
        from service_check import ServiceCheck
        service_check = ServiceCheck()
        Logger.info('Service Check Test')
        service_check.service_check(env)


if __name__ == "__main__":
    ParserMaster().execute()
