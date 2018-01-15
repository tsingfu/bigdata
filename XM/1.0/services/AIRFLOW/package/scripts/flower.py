from resource_management.core.resources.system import Directory, Execute, File
from resource_management.libraries.script.script import Script
from resource_management.core.source import Template, InlineTemplate
from resource_management.libraries.functions.check_process_status import check_process_status
import os


def install_airflow():
    import params
    Directory(
        [params.conf_dir, '/var/run/airflow', params.airflow_base_log_folder, params.airflow_dags_folder],
        owner=params.airflow_user,
        group=params.airflow_group,
        mode=0775,
        create_parents=True)
    if not os.path.exists('/usr/local/lib/python2.7'):
        Execute(
            'wget http://yum.example.com/hadoop/python2.7.tar.gz  -O /tmp/python2.7.tar.gz',
            user=params.airflow_user)
        Execute('tar -zxf /tmp/python2.7.tar.gz -C /usr/local/lib')
    if not os.path.exists('/opt/' + params.version_dir) or not os.path.exists(params.install_dir):
        Execute('rm -rf %s' % '/opt/' + params.version_dir)
        Execute('rm -rf %s' % params.install_dir)
        Execute(
            'wget ' + params.download_url + ' -O /tmp/' + params.filename,
            user=params.airflow_user)
        Execute('tar -zxf /tmp/' + params.filename + ' -C /opt')
        Execute('ln -s /opt/' + params.version_dir + ' ' + params.install_dir)
        Execute('chown -R %s:%s /opt/%s' %
                (params.airflow_user, params.airflow_group, params.version_dir))
        Execute('chown -R %s:%s %s' %
                (params.airflow_user, params.airflow_group, params.install_dir))
        Execute('/bin/rm -f /tmp/' + params.filename)


class Scheduler(Script):
    def install(self, env):
        print "Installing Airflow"
        install_airflow()
        Execute("yum install -y redis")

    def configure(self, env):
        import params
        env.set_params(params)
        File(
            params.airflow_config_path,
            content=InlineTemplate(params.airflow_conf),
            mode=0755)
        File(params.airflow_env_path, content=Template("airflow"), mode=0755)
        Execute('source ' + params.airflow_env_path + ';' + params.install_dir + "/bin/airflow initdb")

    def start(self, env):
        install_airflow()
        self.configure(env)
        import params
        Execute(" rm -rf " + params.flower_pid , user=params.airflow_user)
        Execute(
            "source " + params.airflow_env_path + "; source " + params.install_dir + "/bin/activate; nohup " + params.install_dir + "/bin/airflow flower --pid " + params.flower_pid + ' -l ' + params.airflow_base_log_folder + '/flower.log &',
            user=params.airflow_user)

    def restart(self, env):
        self.stop(env)
        self.start(env)

    def stop(self, env):
        import params
        Execute(" kill -9 `cat " + params.flower_pid + "`", user=params.airflow_user)

    def status(self, env):
        import params
        env.set_params(params)
        Execute("echo `ps aux|grep flower|grep -v grep|awk '{print $2}'` > " + params.flower_pid)
        check_process_status(params.flower_pid)


if __name__ == "__main__":
    Scheduler().execute()
