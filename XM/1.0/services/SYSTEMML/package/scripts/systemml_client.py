#!/usr/bin/python
import sys
from resource_management import *
from resource_management.libraries.script.script import Script

from resource_management.core.resources.system import Execute, File
import os


def install_systemml():
    import params
    if not os.path.exists('/opt/' + params.version_dir) or not os.path.exists(params.install_dir):
        Execute('rm -rf %s' % '/opt/' + params.version_dir)
        Execute('rm -rf %s' % params.install_dir)
        Execute(
            'wget ' + params.download_url + ' -O /tmp/' + params.filename,
            user=params.systemml_user)
        Execute('tar -zxf /tmp/' + params.filename + ' -C /opt')
        Execute('ln -s /opt/' + params.version_dir + ' ' + params.install_dir)
        Execute(' rm -rf ' + params.install_dir + '/conf')
        Execute('ln -s ' + params.conf_dir + ' ' + params.install_dir +
                '/conf')
        Execute("echo 'export PATH=%s/bin:$PATH'>>/etc/profile.d/hadoop.sh" %
                params.install_dir)
        Execute('chown -R %s:%s /opt/%s' % (
            params.systemml_user, params.user_group, params.version_dir))
        Execute('chown -R %s:%s %s' % (
            params.systemml_user, params.user_group, params.install_dir))
        Execute('/bin/rm -f /tmp/' + params.filename)


class SystemMLClient(Script):
    def pre_upgrade_restart(self, env, upgrade_type=None):
        import params
        env.set_params(params)

    def install(self, env):
        install_systemml()

    def status(self, env):
        raise ClientComponentHasNoStatus()


if __name__ == "__main__":
    SystemMLClient().execute()
