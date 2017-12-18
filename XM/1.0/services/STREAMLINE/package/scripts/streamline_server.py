from resource_management import *
from resource_management.libraries.functions.show_logs import show_logs
import os, time
from streamline import ensure_base_directories
from streamline import streamline, wait_until_server_starts
from resource_management.libraries.functions.check_process_status import check_process_status
from resource_management.libraries.functions.format import format
from resource_management.libraries.script.script import Script
from resource_management.core.resources.system import Directory, Execute, File, Link
import os


def install_streamline(first=False):
    import params
    Directory(
        [params.conf_dir],
        owner=params.streamline_user,
        group=params.user_group,
        mode=0775,
        create_parents=True)
    if not os.path.exists('/opt/' + params.version_dir) or not os.path.exists(params.install_dir):
        Execute('rm -rf %s' % '/opt/' + params.version_dir)
        Execute('rm -rf %s' % params.install_dir)
        Execute(
            'wget ' + params.download_url + ' -O /tmp/' + params.filename,
            user=params.streamline_user)
        Execute('tar -zxf /tmp/' + params.filename + ' -C /opt')
        Execute('ln -s /opt/' + params.version_dir + ' ' + params.install_dir)
        Execute('/bin/rm -rf ' + params.install_dir + '/conf')
        Execute('ln -s ' + params.conf_dir + ' ' + params.install_dir + '/conf')
        Execute('chown -R %s:%s /opt/%s' %
                (params.streamline_user, params.user_group, params.version_dir))
        Execute('chown -R %s:%s %s' %
                (params.streamline_user, params.user_group, params.install_dir))
        Execute('/bin/rm -f /tmp/' + params.filename)


class StreamlineServer(Script):
    def get_component_name(self):
        return "streamline"

    def install(self, env):
        install_streamline(first=True)

    def configure(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        streamline(env, upgrade_type=None)

    def pre_upgrade_restart(self, env, upgrade_type=None):
        import params
        env.set_params(params)

    def start(self, env, upgrade_type=None):
        import params
        import status_params
        env.set_params(params)
        install_streamline()
        self.configure(env)

        if not os.path.isfile(params.bootstrap_storage_file):
            try:
                Execute(params.bootstrap_storage_run_cmd,
                        user="root")
                File(params.bootstrap_storage_file,
                     owner=params.streamline_user,
                     group=params.user_group,
                     mode=0644)
            except:
                show_logs(params.streamline_log_dir, params.streamline_user)
                raise

        daemon_cmd = format('source {params.conf_dir}/streamline-env.sh ; {params.streamline_bin} start')
        no_op_test = format(
            'ls {status_params.streamline_pid_file} >/dev/null 2>&1 && ps -p `cat {status_params.streamline_pid_file}` >/dev/null 2>&1')
        try:
            Execute(daemon_cmd,
                    user="root",
                    not_if=no_op_test
                    )
        except:
            show_logs(params.streamline_log_dir, params.streamline_user)
            raise

        if not os.path.isfile(params.bootstrap_file):
            try:
                if params.security_enabled:
                    kinit_cmd = format(
                        "{kinit_path_local} -kt {params.streamline_keytab_path} {params.streamline_jaas_principal};")
                    return_code, out = Execute(kinit_cmd, user=params.streamline_user)
                wait_until_server_starts()
                Execute(params.bootstrap_run_cmd,
                        user=params.streamline_user)
                File(params.bootstrap_file,
                     owner=params.streamline_user,
                     group=params.user_group,
                     mode=0644)
            except:
                show_logs(params.streamline_log_dir, params.streamline_user)
                raise

    def stop(self, env, upgrade_type=None):
        import params
        import status_params
        env.set_params(params)
        ensure_base_directories()
        daemon_cmd = format('source {params.conf_dir}/streamline-env.sh; {params.streamline_bin} stop')
        try:
            Execute(daemon_cmd,
                    user=params.streamline_user,
                    )
        except:
            show_logs(params.streamline_log_dir, params.streamline_user)
            raise
        File(status_params.streamline_pid_file,
             action="delete"
             )

    def status(self, env):
        import status_params
        check_process_status(status_params.streamline_pid_file)

    def get_log_folder(self):
        import params
        return params.streamline_log_dir

    def get_user(self):
        import params
        return params.streamline_user

    def get_pid_files(self):
        import status_params
        return [status_params.streamline_pid_file]


if __name__ == "__main__":
    StreamlineServer().execute()
