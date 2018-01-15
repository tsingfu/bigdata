from resource_management import *

from resource_management.core.resources.system import Directory, File, Link
from resource_management.core.source import InlineTemplate
from resource_management.core.resources.system import Execute
from resource_management.core.exceptions import ClientComponentHasNoStatus
import os
from resource_management.libraries.script.script import Script


def install_flink():
    import params
    if not os.path.exists('/opt/' + params.version_dir) or not os.path.exists(params.install_dir):
        Execute('rm -rf %s' % '/opt/' + params.version_dir)
        Execute('rm -rf %s' % params.install_dir, ignore_failures=True)
        Execute(
            'wget ' + params.download_url + ' -O /tmp/' +
            params.filename,
            user=params.flink_user)
        Execute('tar -zxf /tmp/' + params.filename + ' -C /opt')
        Execute('ln -s /opt/' + params.version_dir + ' ' +
                params.install_dir)
        Execute(' rm -rf ' + params.install_dir + '/conf')
        Execute('ln -s ' + params.conf_dir + ' ' + params.install_dir +
                '/conf')
        Execute('mkdir ' + params.install_dir + '/logs && chmod 777 '
                + params.install_dir + '/logs')
        Execute("echo 'export PATH=%s/bin:$PATH'>>/etc/profile.d/hadoop.sh"
                % params.install_dir)
        Execute('chown -R %s:%s /opt/%s' % (
            params.flink_user, params.flink_group, params.version_dir))
        Execute('chown -R %s:%s %s' %
                (params.flink_user, params.flink_group,
                 params.install_dir))
        Execute('/bin/rm -f /tmp/' + params.filename)


class Client(Script):
    def install(self, env):
        install_flink()
        self.configure(env, True)

    def configure(self, env, isInstall=False):
        import params
        import status_params
        env.set_params(params)
        env.set_params(status_params)
        Directory(
            [params.conf_dir],
            owner=params.flink_user,
            group=params.flink_group)
        # write out nifi.properties
        properties_content = InlineTemplate(params.flink_yaml_content)
        File(
            format("{conf_dir}/flink-conf.yaml"),
            content=properties_content,
            owner=params.flink_user)

    def stop(self, env):
        self.configure(env)

    def start(self, env):
        install_flink()
        self.configure(env)

    def status(self, env):
        raise ClientComponentHasNoStatus()


if __name__ == "__main__":
    Client().execute()
