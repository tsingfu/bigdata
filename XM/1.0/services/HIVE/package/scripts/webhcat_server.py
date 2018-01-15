from resource_management import *
from resource_management.libraries.functions.security_commons import build_expectations, \
  cached_kinit_executor, get_params_from_filesystem, validate_security_config_properties, \
  FILE_TYPE_XML
from webhcat import webhcat
from webhcat_service import webhcat_service
from hive import install_hive
from resource_management.libraries.functions import check_process_status


class WebHCatServer(Script):
    def install(self, env):
        install_hive()

    def start(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        install_hive()
        self.configure(env)  # FOR SECURITY
        webhcat_service(action='start', upgrade_type=upgrade_type)

    def stop(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        webhcat_service(action='stop')

    def configure(self, env):
        import params
        env.set_params(params)
        webhcat()

    def get_component_name(self):
        return "hive-webhcat"

    def status(self, env):
        import status_params
        env.set_params(status_params)
        check_process_status(status_params.webhcat_pid_file)

    def pre_upgrade_restart(self, env, upgrade_type=None):
        print 'todo'

    def get_log_folder(self):
        import params
        return params.hcat_log_dir

    def get_user(self):
        import params
        return params.webhcat_user

    def get_pid_files(self):
        import status_params
        return [status_params.webhcat_pid_file]

if __name__ == "__main__":
    WebHCatServer().execute()
