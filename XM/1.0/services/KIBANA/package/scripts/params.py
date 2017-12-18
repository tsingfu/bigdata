#!/usr/bin/env python

from urlparse import urlparse

from resource_management.libraries.functions import format
from resource_management.libraries.script import Script

# server configurations
config = Script.get_config()

kibana_home = '/usr/share/kibana/'
kibana_bin = '/usr/share/kibana/bin/'

conf_dir = "/etc/kibana"
kibana_user = config['configurations']['kibana-env']['kibana_user']
kibana_group = config['configurations']['kibana-env']['kibana_group']
log_dir = config['configurations']['kibana-env']['kibana_log_dir']
pid_dir = config['configurations']['kibana-env']['kibana_pid_dir']
pid_file = format("{pid_dir}/kibanasearch.pid")
es_url = config['configurations']['kibana-env']['kibana_es_url']
parsed = urlparse(es_url)
es_host = parsed.netloc.split(':')[0]
es_port = parsed.netloc.split(':')[1]
kibana_port = config['configurations']['kibana-env']['kibana_server_port']
kibana_default_application = config['configurations']['kibana-env']['kibana_default_application']
hostname = config['hostname']
java64_home = config['hostLevelParams']['java_home']
kibana_yml_template = config['configurations']['kibana-site']['content']

