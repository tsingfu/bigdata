from resource_management import *
from resource_management.libraries.script.script import Script
from resource_management.core.resources.system import Directory, Execute, File
from resource_management.core.source import InlineTemplate, StaticFile
from resource_management.libraries.functions import check_process_status
from master import install_alluxio

class Slave(Script):
    # Call setup.sh to install the service
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

    # Call start.sh to start the service
    def start(self, env):
        import params
        install_alluxio()
        # Mount ramfs
        cmd = params.install_dir + '/bin/alluxio-start.sh ' + 'worker' + ' Mount'
        Execute('echo "Running cmd: ' + cmd + '"')
        Execute(cmd)
        cmd = "echo `ps -A -o pid,command | grep -i \"[j]ava\" | grep AlluxioWorker | awk '{print $1}'`> " + params.work_pid_file
        Execute(cmd)

    # Called to stop the service using the pidfile
    def stop(self, env):
        import params

        # execure the startup script
        cmd = params.install_dir + '/bin/alluxio-stop.sh'
        Execute('echo "Running cmd: ' + cmd + '"')
        Execute(cmd)

    # Check pid file using Ambari check_process_status
    def status(self, env):
        import params
        check_process_status(params.work_pid_file)


if __name__ == "__main__":
    Slave().execute()
