#!/usr/bin/env python

from resource_management.libraries.functions import get_kinit_path
from resource_management.libraries.functions.default import default
from resource_management.libraries.functions.format import format
from resource_management.libraries.script.script import Script

config = Script.get_config()

infra_solr_metrics_piddir = default('configurations/infra-solr-metrics-env/infra_solr_metrics_pid_dir',
                                    '/var/run/ambari-infra-solr-metrics')
infra_solr_metrics_pidfile = format("{infra_solr_metrics_piddir}/infra-solr-metrics.pid")

security_enabled = config['configurations']['cluster-env']['security_enabled']
kinit_path_local = get_kinit_path(default('/configurations/kerberos-env/executable_search_paths', None))
