#!/usr/bin/env python

from __future__ import print_function
from resource_management import *
import sys
from resource_management.libraries.script.script import Script


class ServiceCheck(Script):
    def service_check(self, env):
        import params
        env.set_params(params)

        print("Running CrateDB service check", file=sys.stdout)
        payload = {'name': 'Buddy.  Dont Worry, I am Fine '}
        pass
        # r = requests.get('http://localhost:' + params.http_port + '/', params=payload)
        #
        # if r.status_code == 200:
        #     print(r.json(), file=sys.stdout)
        #     sys.exit(0)
        # else:
        #     print("CrateDB is not running", file=sys.stderr)
        #     sys.exit(1)


if __name__ == "__main__":
    ServiceCheck().execute()
