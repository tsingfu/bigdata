#!/usr/bin/python

import sys
from resource_management import *
from resource_management.libraries.script.script import Script
from actions import *

class EagleClient(Script):
    def install(self, env):
        print 'Install the eagle client'
        install_eagle()

    def configure(self, env):
        print 'Configure the eagle client'
        install_eagle()
        config_eagle()


if __name__ == "__main__":
    EagleClient().execute()
