from resource_management.libraries.script.script import Script
from resource_management.libraries.functions.check_process_status import check_process_status
from resource_management.libraries.functions.security_commons import build_expectations, \
    cached_kinit_executor, get_params_from_filesystem, validate_security_config_properties, \
    FILE_TYPE_XML
from resource_management.core.resources.system import Execute
from yarn import yarn, install_hadoop
from service import service


def copy_to_hdfs(name, user_group, owner, custom_source_file=None, custom_dest_file=None):
    import os
    import params
    dest_dir = os.path.dirname(custom_dest_file)
    params.HdfsResource(dest_dir,
                        type="directory",
                        action="create_on_execute",
                        owner=owner,
                        mode=0555
                        )

    params.HdfsResource(custom_dest_file,
                        type="file",
                        action="create_on_execute",
                        source=custom_source_file,
                        group=user_group,
                        owner=owner,
                        mode=0444,
                        replace_existing_files=False,
                        )
    params.HdfsResource(None, action="execute")


class HistoryServer(Script):
    def install(self, env):
        self.install_packages(env)
        install_hadoop(first=True)

    def stop(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        service('historyserver', action='stop', serviceName='mapreduce')

    def configure(self, env):
        import params
        env.set_params(params)
        yarn(name="historyserver")

    def get_component_name(self):
        return "hadoop-mapreduce-historyserver"

    def pre_upgrade_restart(self, env, upgrade_type=None):
        install_hadoop()
        import params

        Execute('wget http://yum.example.com/hadoop/hdfs/tez.tar.gz  -O /tmp/tez.tar.gz')
        copy_to_hdfs("tez", params.user_group, params.hdfs_user,
                     custom_source_file='/tmp/tez.tar.gz',
                     custom_dest_file='/apps/tez/tez.tar.gz')
        params.HdfsResource(None, action="execute")
        Execute('/bin/rm -f /tmp/tez.tar.gz')

        Execute('wget http://yum.example.com/hadoop/hdfs/slider.tar.gz  -O /tmp/slider.tar.gz')
        copy_to_hdfs("slider", params.user_group, params.hdfs_user,
                     custom_source_file='/tmp/slider.tar.gz',
                     custom_dest_file='/apps/slider/slider.tar.gz')
        params.HdfsResource(None, action="execute")
        Execute('/bin/rm -f /tmp/slider.tar.gz')

    def start(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        self.configure(env)  # FOR SECURITY
        Execute('wget http://yum.example.com/hadoop/hdfs/tez.tar.gz  -O /tmp/tez.tar.gz')
        copy_to_hdfs("tez", params.user_group, params.hdfs_user,
                     custom_source_file='/tmp/tez.tar.gz',
                     custom_dest_file='/apps/tez/tez.tar.gz')
        params.HdfsResource(None, action="execute")
        Execute('/bin/rm -f /tmp/tez.tar.gz')

        Execute('wget http://yum.example.com/hadoop/hdfs/slider.tar.gz  -O /tmp/slider.tar.gz')
        copy_to_hdfs("slider", params.user_group, params.hdfs_user,
                     custom_source_file='/tmp/slider.tar.gz',
                     custom_dest_file='/apps/slider/slider.tar.gz')
        params.HdfsResource(None, action="execute")
        Execute('/bin/rm -f /tmp/slider.tar.gz')

        service('historyserver', action='start', serviceName='mapreduce')

    def status(self, env):
        import status_params
        env.set_params(status_params)
        check_process_status(status_params.mapred_historyserver_pid_file)

    def security_status(self, env):
        import status_params
        env.set_params(status_params)
        if status_params.security_enabled:
            expectations = {}
            expectations.update(
                build_expectations('mapred-site', None, [
                    'mapreduce.jobhistory.keytab',
                    'mapreduce.jobhistory.principal',
                    'mapreduce.jobhistory.webapp.spnego-keytab-file',
                    'mapreduce.jobhistory.webapp.spnego-principal'
                ], None))

            security_params = get_params_from_filesystem(
                status_params.hadoop_conf_dir,
                {'mapred-site.xml': FILE_TYPE_XML})
            result_issues = validate_security_config_properties(
                security_params, expectations)
            if not result_issues:  # If all validations passed successfully
                try:
                    # Double check the dict before calling execute
                    if ('mapred-site' not in security_params or
                                'mapreduce.jobhistory.keytab' not in
                                security_params['mapred-site'] or
                                'mapreduce.jobhistory.principal' not in
                                security_params['mapred-site'] or
                                'mapreduce.jobhistory.webapp.spnego-keytab-file'
                            not in security_params['mapred-site'] or
                                'mapreduce.jobhistory.webapp.spnego-principal'
                            not in security_params['mapred-site']):
                        self.put_structured_out({"securityState": "UNSECURED"})
                        self.put_structured_out(
                            {"securityIssuesFound":
                                 "Keytab file or principal not set."})
                        return

                    cached_kinit_executor(
                        status_params.kinit_path_local,
                        status_params.mapred_user, security_params[
                            'mapred-site']['mapreduce.jobhistory.keytab'],
                        security_params['mapred-site'][
                            'mapreduce.jobhistory.principal'],
                        status_params.hostname, status_params.tmp_dir)
                    cached_kinit_executor(
                        status_params.kinit_path_local,
                        status_params.mapred_user,
                        security_params['mapred-site'][
                            'mapreduce.jobhistory.webapp.spnego-keytab-file'],
                        security_params['mapred-site'][
                            'mapreduce.jobhistory.webapp.spnego-principal'],
                        status_params.hostname, status_params.tmp_dir)
                    self.put_structured_out(
                        {"securityState": "SECURED_KERBEROS"})
                except Exception as e:
                    self.put_structured_out({"securityState": "ERROR"})
                    self.put_structured_out({"securityStateErrorInfo": str(e)})
            else:
                issues = []
                for cf in result_issues:
                    issues.append(
                        "Configuration file %s did not pass the validation. Reason: %s"
                        % (cf, result_issues[cf]))
                self.put_structured_out(
                    {"securityIssuesFound": ". ".join(issues)})
                self.put_structured_out({"securityState": "UNSECURED"})
        else:
            self.put_structured_out({"securityState": "UNSECURED"})

    def get_log_folder(self):
        import params
        return params.mapred_log_dir

    def get_user(self):
        import params
        return params.mapred_user

    def get_pid_files(self):
        import status_params
        return [status_params.mapred_historyserver_pid_file]


if __name__ == "__main__":
    HistoryServer().execute()
