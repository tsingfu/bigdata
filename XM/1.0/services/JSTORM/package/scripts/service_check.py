#!/usr/bin/env python

from resource_management import *
from resource_management.libraries.functions import get_unique_id_and_date
from resource_management.libraries.script.script import Script
from resource_management.core.resources.system import Execute, File

class ServiceCheck(Script):
  def service_check(self, env):
    import params
    env.set_params(params)

    unique = get_unique_id_and_date()

    File("/tmp/wordCount.jar",
         content=StaticFile("wordCount.jar")
    )

    cmd = format("env JAVA_HOME={java64_home} storm jar /tmp/wordCount.jar storm.starter.WordCountTopology WordCount{unique} -c nimbus.host={nimbus_host}")

    Execute(cmd,
            logoutput=True,
            path=params.jstorm_bin_dir
    )

    Execute(format("env JAVA_HOME={java64_home} storm kill WordCount{unique}"),
            path=params.jstorm_bin_dir)

if __name__ == "__main__":
  ServiceCheck().execute()
