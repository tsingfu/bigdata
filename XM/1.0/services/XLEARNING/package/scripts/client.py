from resource_management import *
from resource_management.core.resources.system import Directory, Execute, File
from resource_management.libraries.script.script import Script
from resource_management.core.source import Template, InlineTemplate
from xlearning import install_xlearning
from resource_management.core.exceptions import ClientComponentHasNoStatus
from resource_management.libraries.resources.xml_config import XmlConfig


class Master(Script):
    def install(self, env):
        self.install_packages(env)
        install_xlearning()
        self.configure(env)

    def configure(self, env):
        import params
        env.set_params(params)
        Directory(params.conf_dir,
                  owner=params.xlearning_user,
                  group=params.xlearning_group,
                  create_parents=True)
        File(format("{conf_dir}/xlearning-env.sh"),
             content=InlineTemplate(params.env_content),
             mode=0755,
             owner=params.xlearning_user,
             group=params.xlearning_group
             )
        File(format("{conf_dir}/log4j.properties"),
             content=InlineTemplate(params.log_content),
             mode=0755,
             owner=params.xlearning_user,
             group=params.xlearning_group
             )
        XmlConfig("xlearning-site.xml",
                  conf_dir=params.conf_dir,
                  configurations=params.config['configurations']['xlearning-site'],
                  configuration_attributes=params.config['configuration_attributes']['xlearning-site'],
                  owner=params.xlearning_user,
                  group=params.xlearning_group,
                  mode=0664)

    def status(self, env):
        raise ClientComponentHasNoStatus()


if __name__ == "__main__":
    Master().execute()
