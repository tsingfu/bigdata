#!/usr/bin/env python

from __future__ import print_function

from resource_management.libraries.script import Script

from indexing_commands import IndexingCommands
from parser_commands import ParserCommands


class ServiceCheck(Script):
    def service_check(self, env):
        import params
        parsercommands = ParserCommands(params)
        indexingcommands = IndexingCommands(params)
        all_found = parsercommands.topologies_running(env) and indexingcommands.is_topology_active(env)
        if all_found:
            exit(0)
        else:
            exit(1)


if __name__ == "__main__":
    ServiceCheck().execute()
