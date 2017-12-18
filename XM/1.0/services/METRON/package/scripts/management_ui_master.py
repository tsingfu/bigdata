from resource_management.core.exceptions import ComponentIsNotRunning
from resource_management.core.exceptions import ExecutionFailed
from resource_management.core.resources.system import Directory
from resource_management.core.resources.system import File
from resource_management.core.source import Template
from resource_management.libraries.functions.format import format
from resource_management.libraries.functions.get_user_call_output import get_user_call_output
from resource_management.libraries.script import Script
from resource_management.core.resources.system import Execute

from resource_management.core.logger import Logger

from management_ui_commands import ManagementUICommands
from metron_service import install_metron

class ManagementUIMaster(Script):

    def install(self, env):
        import params
        env.set_params(params)
        self.install_packages(env)
        install_metron()
        Execute('wget http://yum.example.com/hadoop/metron-management-ui -O /etc/init.d/metron-management-ui')
        Execute('chmod a+x /etc/init.d/metron-management-ui')

    def configure(self, env, upgrade_type=None, config_dir=None):
        print 'configure managment_ui'
        import params
        env.set_params(params)
        File(format("/etc/sysconfig/metron"),
             content=Template("metron.j2")
             )

        File(format("{metron_config_path}/management_ui.yml"),
             mode=0755,
             content=Template("management_ui.yml.j2"),
             owner=params.metron_user,
             group=params.metron_group
             )

        Directory('/var/run/metron',
                  create_parents=False,
                  mode=0755,
                  owner=params.metron_user,
                  group=params.metron_group
                  )

    def start(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        self.configure(env)
        commands = ManagementUICommands(params)
        commands.start_management_ui()

    def stop(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        commands = ManagementUICommands(params)
        commands.stop_management_ui()

    def status(self, env):
        import status_params
        env.set_params(status_params)
        cmd = format('curl --max-time 3 {hostname}:{metron_management_ui_port}')
        try:
            get_user_call_output(cmd, user=status_params.metron_user)
        except ExecutionFailed:
            raise ComponentIsNotRunning()

    def restart(self, env):
        import params
        env.set_params(params)
        self.configure(env)
        commands = ManagementUICommands(params)
        commands.restart_management_ui(env)


if __name__ == "__main__":
    ManagementUIMaster().execute()
