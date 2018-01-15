#!/usr/bin/python

import os
from resource_management import *
from resource_management.core.exceptions import ClientComponentHasNoStatus
from resource_management.core.logger import Logger
from resource_management.libraries.script.script import Script
from resource_management.core.resources.system import Execute, File
from resource_management.core.source import InlineTemplate, StaticFile
from resource_management.core.resources import Directory


def install_r4ml():
    import params
    if not os.path.exists('/opt/' + params.version_dir) or not os.path.exists(params.install_dir):
        Execute('rm -rf %s' % '/opt/' + params.version_dir)
        Execute('rm -rf %s' % params.install_dir)
        Execute(
            'wget ' + params.download_url + ' -O /tmp/' + params.filename,
            user=params.r4ml_user)
        Execute('tar -zxf /tmp/' + params.filename + ' -C /opt')
        Execute('ln -s /opt/' + params.version_dir + ' ' + params.install_dir)
        Execute(' rm -rf ' + params.install_dir + '/conf')
        Execute('ln -s ' + params.r4ml_conf_dir + ' ' + params.install_dir +
                '/conf')
        Execute("echo 'export PATH=%s/bin:$PATH'>>/etc/profile.d/hadoop.sh" %
                params.install_dir)
        Execute('chown -R %s:%s /opt/%s' % (
            params.r4ml_user, params.user_group, params.version_dir))
        Execute('chown -R %s:%s %s' % (
            params.r4ml_user, params.user_group, params.install_dir))
        Execute('/bin/rm -f /tmp/' + params.filename)


class R4MLClient(Script):
    def configure(selfself, env):
        import params
        env.set_params(params)

    def pre_upgrade_restart(self, env, upgrade_type=None):
        import params
        env.set_params(params)

    def stack_upgrade_save_new_config(self, env):
        import params
        env.set_params(params)
        self.configure(env)

    def install(self, env):
        import params
        env.set_params(params)

        # install R and R4ML
        self.install_packages(env)
        install_r4ml()

        Execute("yum install R -y")

        # install the dependent packages
        packages = ["R6", "uuid", "survival"]

        # set up configuration file
        Directory(params.r4ml_conf_dir,
                  create_parents=True,
                  action="create",
                  mode=0755)

        File(format("{r4ml_conf_dir}/Renviron"),
             mode=0755,
             content=InlineTemplate(params.Renviron_template))

        # install R4ML package to /usr/iop/current/r4ml-client/R/lib directory
        Directory(format(params.r4ml_home + "/R/lib"),
                  action="create",
                  create_parents=True,
                  mode=0755)

        Execute(format(
            "sudo R_LIBS={spark_home}/R/lib R CMD INSTALL --install-tests --library={r4ml_home}/R/lib {r4ml_home}/R4ML_*.tar.gz"))

    def status(self, env):
        raise ClientComponentHasNoStatus()


if __name__ == "__main__":
    R4MLClient().execute()
