#!/usr/bin/env python

from resource_management import *
from yaml_utils import escape_yaml_propetry
import os
from ambari_agent.AgentException import AgentException
from resource_management.core.resources import Directory
from resource_management.core.resources.system import Execute, File

from resource_management.core.source import InlineTemplate, Template
from resource_management.libraries.functions.format import format
from resource_management.libraries.resources.template_config import TemplateConfig


def install_storm():
    import params
    if not os.path.exists('/opt/' + params.version_dir) or not os.path.exists(params.install_dir):
        Execute('rm -rf %s' % '/opt/' + params.version_dir)
        Execute('rm -rf %s' % params.install_dir)
        Execute(
            'wget ' + params.download_url + ' -O /tmp/' + params.filename,
            user=params.jstorm_user)
        Execute('unzip /tmp/' + params.filename + ' -C /opt')
        Execute('ln -s /opt/' + params.version_dir + ' ' + params.install_dir)
        Execute(' mv ' + params.install_dir + '/conf/*  ' + params.conf_dir)
        Execute(' rm -rf ' + params.install_dir + '/conf')
        Execute('ln -s ' + params.conf_dir + ' ' + params.install_dir +
                '/conf')
        Execute("echo 'export PATH=%s/bin:$PATH'>>/etc/profile.d/hadoop.sh" %
                params.install_dir)
        Execute('chown -R %s:%s /opt/%s' % (
            params.jstorm_user, params.jstorm_group, params.version_dir))
        Execute('chown -R %s:%s %s' % (
            params.jstorm_user, params.jstorm_group, params.install_dir))
        Execute('/bin/rm -f /tmp/' + params.filename)


def config_storm():
    import params

    Directory(params.log_dir,
              owner=params.jstorm_user,
              group=params.user_group,
              mode=0775,
              recursive=True
              )

    Directory([params.pid_dir, params.local_dir, params.conf_dir],
              owner=params.jstorm_user,
              group=params.user_group,
              recursive=True
              )

    configurations = params.config['configurations']['jstorm-site']

    File(format("{conf_dir}/storm.yaml"),
         content=Template(
             "storm.yaml.j2",
             extra_imports=[escape_yaml_propetry],
             configurations=configurations),
         owner=params.jstorm_user,
         group=params.user_group
         )

    File(format("{conf_dir}/jstorm-env.sh"),
         owner=params.jstorm_user,
         content=InlineTemplate(params.storm_env_sh_template)
         )

    if params.security_enabled:
        TemplateConfig(
            format("{conf_dir}/storm_jaas.conf"), owner=params.jstorm_user)

        TemplateConfig(
            format("{conf_dir}/client_jaas.conf"), owner=params.jstorm_user)
        minRuid = configurations[
            '_storm.min.ruid'] if configurations.has_key(
            '_storm.min.ruid') else ''

        min_user_ruid = int(minRuid) if minRuid.isdigit(
        ) else _find_real_user_min_uid()

        File(
            format("{conf_dir}/worker-launcher.cfg"),
            content=Template(
                "worker-launcher.cfg.j2", min_user_ruid=min_user_ruid),
            owner='root',
            group=params.user_group)


'''
Finds minimal real user UID
'''


def _find_real_user_min_uid():
    with open('/etc/login.defs') as f:
        for line in f:
            if line.strip().startswith('UID_MIN') and len(line.split()) == 2 and line.split()[1].isdigit():
                return int(line.split()[1])
    raise AgentException("Unable to find UID_MIN in file /etc/login.defs. Expecting format e.g.: 'UID_MIN    500'")
