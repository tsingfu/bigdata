#!/usr/bin/env python
from resource_management import *
from resource_management.libraries.script.script import Script

class AlluxioServiceCheck(Script):
    # Service check for Alluxio service
    def service_check(self, env):
        import params
        env.set_params(params)

if __name__ == "__main__":
    AlluxioServiceCheck().execute()
