#!/usr/bin/env python

from resource_management.libraries.functions import get_kinit_path
from resource_management.libraries.functions.default import default
from resource_management.libraries.functions.format import format
from resource_management.libraries.script.script import Script

config = Script.get_config()

solr_port = default('configurations/solr-env/solr_port', '8983')
solr_piddir = default('configurations/solr-env/solr_pid_dir', '/var/run/solr')
solr_pidfile = format("{solr_piddir}/solr-{solr_port}.pid")

security_enabled = config['configurations']['cluster-env']['security_enabled']
kinit_path_local = get_kinit_path(default('/configurations/kerberos-env/executable_search_paths', None))
