#!/usr/bin/env python

import collections
import os

from resource_management.libraries.resources.template_config import TemplateConfig
from resource_management.core.resources.system import Directory, Execute, File, Link
from resource_management.core.source import StaticFile, Template, InlineTemplate, DownloadSource
from resource_management.libraries.functions import format
from resource_management.libraries.functions.get_user_call_output import get_user_call_output
from resource_management.core.exceptions import ExecutionFailed
from resource_management.core.logger import Logger
from resource_management.libraries.functions.show_logs import show_logs
import urllib2, time, json


def streamline(env, upgrade_type=None):
    import params
    ensure_base_directories()
    # Logger.info(format("Effective stack version: {effective_version}"))

    File(format("{conf_dir}/streamline-env.sh"),
         owner=params.streamline_user,
         content=InlineTemplate(params.streamline_env_sh_template)
         )

    # On some OS this folder could be not exists, so we will create it before pushing there files
    Directory(params.limits_conf_dir,
              create_parents=True,
              owner='root',
              group='root'
              )

    Directory([params.jar_storage],
              owner=params.streamline_user,
              group=params.user_group,
              create_parents=True,
              cd_access="a",
              mode=0755,
              )

    # this is hard-coded as we are not accepting
    # the registry local-jars
    # should be removed from future releases
    Directory("/tmp/schema-registry/local-jars",
              owner=params.streamline_user,
              group=params.user_group,
              create_parents=True,
              cd_access="a",
              mode=0755)

    Directory([params.topology_test_results],
              owner=params.streamline_user,
              group=params.user_group,
              create_parents=True,
              cd_access="a",
              mode=0755,
              )

    File(os.path.join(params.limits_conf_dir, 'streamline.conf'),
         owner='root',
         group='root',
         mode=0644,
         content=Template("streamline.conf.j2")
         )

    File(format("{conf_dir}/streamline.yaml"),
         content=Template("streamline.yaml.j2"),
         owner=params.streamline_user,
         group=params.user_group,
         mode=0644
         )

    if params.security_enabled:
        if params.streamline_jaas_conf_template:
            File(format("{conf_dir}/streamline_jaas.conf"),
                 owner=params.streamline_user,
                 content=InlineTemplate(params.streamline_jaas_conf_template))
        else:
            TemplateConfig(format("{conf_dir}/streamline_jaas.conf"),
                           owner=params.streamline_user)

    if not os.path.islink(params.streamline_managed_log_dir):
        Link(params.streamline_managed_log_dir,
             to=params.streamline_log_dir)


def ensure_base_directories():
    import params
    import status_params
    Directory(
        [params.streamline_log_dir, status_params.streamline_pid_dir, params.conf_dir, params.streamline_agent_dir],
        mode=0755,
        cd_access='a',
        owner=params.streamline_user,
        group=params.user_group,
        create_parents=True,
        recursive_ownership=True,
        )


def wait_until_server_starts():
    import params
    streamline_api = format("http://{params.hostname}:{params.streamline_port}/api/v1/config/streamline")
    Logger.info(streamline_api)
    max_retries = 6
    success = False
    curl_connection_timeout = '5'
    for num in range(0, max_retries):
        try:
            Logger.info(format("Making http requests to {streamline_api}"))

            if params.security_enabled:
                get_app_info_cmd = "curl --negotiate -u : -ks --location-trusted --connect-timeout " + curl_connection_timeout + " " + streamline_api
                return_code, stdout, _ = get_user_call_output(get_app_info_cmd, user=params.streamline_user,
                                                              path='/usr/sbin:/sbin:/usr/local/bin:/bin:/usr/bin', )
                try:
                    json_response = json.loads(stdout)
                    success = True
                    Logger.info(format("Successfully made a API request to SAM. {stdout}"))
                    break
                except Exception as e:
                    Logger.error(format("Response from SAM API was not a valid JSON. Response: {stdout}"))
            else:
                response = urllib2.urlopen(streamline_api)
                api_response = response.read()
                response_code = response.getcode()
                Logger.info(format("SAM response http status {response}"))
                if response.getcode() != 200:
                    Logger.error(format("Failed to fetch response for {streamline_api}"))
                    show_logs(params.streamline_log_dir, params.streamline_user)
                    raise
                else:
                    success = True
                    Logger.info(format("Successfully made a API request to SAM. {api_response}"))
                    break
        except (urllib2.URLError, ExecutionFailed) as e:
            Logger.error(format(
                "Failed to make API request to SAM server at {streamline_api},retrying.. {num} out of {max_retries}"))
            time.sleep(num * 5)  # exponential back-off
            continue

    if success != True:
        Logger.error(format("Failed to make API request to  SAM server at {streamline_api} after {max_retries}"))
        raise
