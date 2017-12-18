from resource_management import *
from resource_management.core.resources.system import File, Execute, Directory
from resource_management.core.source import StaticFile, Template, DownloadSource, InlineTemplate
from resource_management.libraries.script.script import Script


def cratedb():
    import params

    params.path_data = params.path_data.replace('"', '')
    data_path = params.path_data.replace(' ', '').split(',')
    data_path[:] = [x.replace('"', '') for x in data_path]

    directories = [params.log_dir, params.pid_dir, params.conf_dir]
    directories = directories + data_path

    Directory(directories,
              owner=params.crate_user,
              group=params.crate_user,
              create_parents=True
              )

    File(format("{conf_dir}/elastic-env.sh"),
         owner=params.crate_user,
         content=InlineTemplate(params.crate_env_sh_template)
         )

    configurations = params.config['configurations']['crate-site']

    File(format("{conf_dir}/crate.yml"),
         content=Template(
             "crate.yaml.j2",
             configurations=configurations),
         owner=params.crate_user,
         group=params.crate_user
         )

    File(format("/etc/sysconfig/crate"),
         content=Template(
             "sysconfig.j2",
             configurations=configurations),
         owner="root",
         group="root"
         )


class CrateDB(Script):
    def install(self, env):
        import params
        env.set_params(params)
        Execute('yum install -y crate')

    def configure(self, env):
        import params
        env.set_params(params)
        cratedb()

    def stop(self, env):
        import params
        env.set_params(params)
        Execute("service crate stop")

    def start(self, env):
        import params
        env.set_params(params)
        self.configure(env)
        Execute("service crate start")

    def status(self, env):
        import params
        env.set_params(params)
        Execute("service crate status")


if __name__ == "__main__":
    CrateDB().execute()
