#!/usr/bin/env python

from resource_management import *
from resource_management.libraries.script.script import Script
from resource_management.core.logger import Logger
from resource_management.libraries.functions.format import format
from resource_management.libraries.functions.show_logs import show_logs
from resource_management.libraries.functions.get_user_call_output import get_user_call_output
from resource_management.core.resources.system import Execute
import urllib2, time, json

CURL_CONNECTION_TIMEOUT = '5'


class ServiceCheck(Script):
    def service_check(self, env):
        import params
        env.set_params(params)
        Logger.info("Registry check passed")
        registry_api = format("http://{params.hostname}:{params.registry_port}/api/v1/schemaregistry/schemaproviders")
        Logger.info(registry_api)
        max_retries = 3
        success = False

        if params.security_enabled:
            kinit_cmd = format("{kinit_path_local} -kt {params.smoke_user_keytab} {params.smokeuser_principal};")
            Execute(kinit_cmd, user=params.smokeuser)

        for num in range(0, max_retries):
            try:
                Logger.info(format("Making http requests to {registry_api}"))
                if params.security_enabled:
                    get_app_info_cmd = "curl --negotiate -u : -ks --location-trusted --connect-timeout " + CURL_CONNECTION_TIMEOUT + " " + registry_api
                    return_code, stdout, _ = get_user_call_output(get_app_info_cmd, user=params.smokeuser,
                                                                  path='/usr/sbin:/sbin:/usr/local/bin:/bin:/usr/bin', )
                    try:
                        json_response = json.loads(stdout)
                        success = True
                        Logger.info(format("Successfully made a API request to registry. {stdout}"))
                        break
                    except Exception as e:
                        Logger.error(format("Response from REGISTRY API was not a valid JSON. Response: {stdout}"))
                else:
                    response = urllib2.urlopen(registry_api)
                    api_response = response.read()
                    response_code = response.getcode()
                    Logger.info(format("registry response http status {response_code}"))
                    if response.getcode() != 200:
                        Logger.error(format("Failed to fetch response for {registry_api}"))
                        show_logs(params.registry_log_dir, params.registry_user)
                        raise
                    else:
                        success = True
                        Logger.info(format("Successfully made a API request to registry. {api_response}"))
                        break
            except urllib2.URLError as e:
                Logger.error(format(
                    "Failed to make API request to Registry server at {registry_api},retrying.. {num} out {max_retries}"))
                time.sleep(num * 10)  # exponential back off
                continue

        if success != True:
            raise Fail(format("Failed to make API request to Registry server at {registry_api} after {max_retries}"))


if __name__ == "__main__":
    ServiceCheck().execute()
