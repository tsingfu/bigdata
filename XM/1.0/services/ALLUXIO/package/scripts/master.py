from resource_management import *
from resource_management.libraries.script.script import Script
from resource_management.core.resources.system import Directory, Execute, File
from resource_management.core.source import InlineTemplate, StaticFile
from resource_management.libraries.functions import check_process_status
import os


def install_alluxio(first=False):
    import params
    # if first:
    #     Execute('rm -rf %s' %  '/opt/' + params.version_dir)
    #     Execute('rm -rf %s' % params.install_dir)
    print "install dir:" + params.install_dir
    Directory(
        [params.alluxio_config_dir, params.pid_dir],
        owner=params.alluxio_user,
        group=params.user_group,
        mode=0775,
        create_parents=True)
    if not os.path.exists('/opt/' + params.version_dir) or not os.path.exists(params.install_dir):
        Execute('rm -rf %s' % '/opt/' + params.version_dir)
        Execute('rm -rf %s' % params.install_dir)
        Execute(
            'wget ' + params.download_url + ' -O /tmp/' + params.filename,
            user=params.alluxio_user)
        Execute('tar -zxf /tmp/' + params.filename + ' -C /opt')
        Execute('ln -s /opt/' + params.version_dir + ' ' + params.install_dir)
        Execute(
            ' mv ' + params.install_dir + '/conf/* ; ' + params.alluxio_config_dir + ' rm -rf ' + params.install_dir + '/conf')
        Execute('ln -s ' + params.alluxio_config_dir + ' ' + params.install_dir +
                '/conf')
        Execute('chown -R %s:%s /opt/%s' %
                (params.alluxio_user, params.user_group, params.version_dir))
        Execute('chown -R %s:%s %s' %
                (params.alluxio_user, params.user_group, params.install_dir))
        Execute('/bin/rm -f /tmp/' + params.filename)


class Master(Script):
    def install(self, env):
        install_alluxio()
        self.configure(env)

    def configure(self, env):
        import params
        env.set_params(params)
        File(format("{alluxio_config_dir}/alluxio-env.sh"),
             mode=0644,
             group=params.user_group,
             owner=params.alluxio_user,
             content=InlineTemplate(params.env_sh_template)
             )

    def start(self, env):
        import params
        install_alluxio()
        # call format
        cmd = params.install_dir + '/bin/alluxio ' + 'format'

        Execute('echo "Running cmd: ' + cmd + '"')
        Execute(cmd)

        # execute the startup script
        cmd = params.install_dir + '/bin/alluxio-start.sh ' + 'master'

        Execute('echo "Running cmd: ' + cmd + '"')
        Execute(cmd)

        cmd = "echo `ps -A -o pid,command | grep -i \"[j]ava\" | grep AlluxioMaster | awk '{print $1}'`> " + params.master_pid_file
        Execute(cmd)

    def stop(self, env):
        import params
        # execure the startup script
        cmd = params.install_dir + '/bin/alluxio-stop.sh'
        Execute('echo "Running cmd: ' + cmd + '"')
        Execute(cmd)

    def status(self, env):
        import params
        check_process_status(params.master_pid_file)


if __name__ == "__main__":
    Master().execute()
