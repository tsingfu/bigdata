#!/usr/bin/env python

from resource_management.libraries.script.script import Script
from slider import slider,install_slider
from resource_management.core.exceptions import ClientComponentHasNoStatus


class SliderClient(Script):
    def status(self, env):
        raise ClientComponentHasNoStatus()

    def get_component_name(self):
        return "slider-client"

    def pre_upgrade_restart(self, env, upgrade_type=None):
        import params
        env.set_params(params)

    def install(self, env):
        install_slider(first=True)
        self.configure(env)

    def configure(self, env):
        import params
        env.set_params(params)
        install_slider()
        slider()


if __name__ == "__main__":
    SliderClient().execute()
