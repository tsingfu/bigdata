import os
from resource_management.core.resources.system import Directory, Execute, File
from resource_management.core.source import Template, InlineTemplate
from resource_management.libraries.functions.format import format
from resource_management.libraries.script.script import Script
from resource_management.libraries.functions.check_process_status import check_process_status


def install_doctor():
    import params
    Directory(
        [params.conf_dir],
        owner=params.doctor_user,
        group=params.user_group,
        mode=0775,
        create_parents=True)
    if not os.path.exists('/opt/' + params.version_dir) or not os.path.exists(params.install_dir):
        Execute('rm -rf %s' % '/opt/' + params.version_dir)
        Execute('rm -rf %s' % params.install_dir)
        Execute(
            'wget ' + params.download_url + ' -O /tmp/' + params.filename,
            user=params.doctor_user)
        Execute('tar -zxf /tmp/' + params.filename + ' -C /opt')
        Execute('ln -s /opt/' + params.version_dir + ' ' + params.install_dir)
        Execute(
            ' cp -r ' + params.install_dir + '/conf/* ' + params.conf_dir + ' && rm -rf ' + params.install_dir + '/conf')
        Execute('ln -s ' + params.conf_dir + ' ' + params.install_dir +
                '/conf')
        Execute("echo 'export PATH=%s/bin:$PATH'>>/etc/profile.d/hadoop.sh" %
                params.install_dir)
        Execute('chown -R %s:%s /opt/%s' %
                (params.doctor_user, params.user_group, params.version_dir))
        Execute('chown -R %s:%s %s' %
                (params.doctor_user, params.user_group, params.install_dir))
        Execute('/bin/rm -f /tmp/' + params.filename)


class Doctor(Script):
    def get_component_name(self):
        return "doctor"

    def pre_upgrade_restart(self, env, upgrade_type=None):
        print 'todo'

    def install(self, env):
        import params
        env.set_params(params)
        install_doctor()

    def configure(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        File(format("{install_dir}/app-conf/elephant.conf"),
             owner=params.doctor_user,
             content=InlineTemplate(params.app_conf_template)
             )

        File(format("{conf_dir}/drelephant-env.sh"),
             owner=params.doctor_user,
             content=InlineTemplate(params.drelephant_env_template),
             mode=0755
             )
        Directory(['/opt/logs/elephant'],
                  owner=params.doctor_user,
                  create_parents=True
                  )

    def start(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        install_doctor()
        self.configure(env, upgrade_type)
        Execute('source ' + params.conf_dir + '/drelephant-env.sh;' + params.install_dir + "/bin/start.sh",
                user=params.doctor_user)

    def stop(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        Execute('source ' + params.conf_dir + '/drelephant-env.sh;' + params.install_dir + "/bin/stop.sh",
                user=params.doctor_user)

    def status(self, env):
        import params
        env.set_params(params)
        check_process_status(params.install_dir + '/RUNNING_PID')


if __name__ == "__main__":
    Doctor().execute()
