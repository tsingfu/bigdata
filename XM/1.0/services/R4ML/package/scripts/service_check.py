#!/usr/bin/env python

from resource_management import *
from resource_management.libraries.functions.format import format
import os
from resource_management.libraries.script.script import Script
from resource_management.core.resources.system import Execute, File
from resource_management.core.source import InlineTemplate, StaticFile

class R4MLServiceCheck(Script):
    def service_check(self, env):
        import params
        env.set_params(params)

        # generate the service check file
        scR = os.path.join(params.exec_tmp_dir, "ServiceCheck.R")
        File(format(scR),
             content = StaticFile("ServiceCheck.R"),
             mode = 0755)

        Execute(("Rscript", scR),
                tries=120,
                try_sleep=20,
                path='/usr/sbin:/sbin:/usr/local/bin:/bin:/usr/bin',
                logoutput=True,
                user=params.smokeuser)

if __name__ == "__main__":
    R4MLServiceCheck().execute()