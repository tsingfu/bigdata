#!/usr/bin/env python

import os

from resource_management.core.logger import Logger
from resource_management.core.resources import File, Execute
from resource_management.core.source import StaticFile
from resource_management.libraries.functions.format import format
from resource_management.core.resources.system import Directory


def setup_ranger_solr():
    import params

    if params.has_ranger_admin and params.security_enabled:

        import sys, os
        script_path = os.path.realpath(__file__).split('/services')[0] + '/hooks/before-INSTALL/scripts/ranger'
        sys.path.append(script_path)
        
        from setup_ranger_plugin_xml import setup_ranger_plugin

        if params.retryAble:
            Logger.info("Solr: Setup ranger: command retry enables thus retrying if ranger admin is down !")
        else:
            Logger.info("Solr: Setup ranger: command retry not enabled thus skipping if ranger admin is down !")

        if params.xml_configurations_supported and params.enable_ranger_solr and params.xa_audit_hdfs_is_enabled:
            if params.has_namenode:
                params.HdfsResource("/ranger/audit",
                                    type="directory",
                                    action="create_on_execute",
                                    owner=params.hdfs_user,
                                    group=params.hdfs_user,
                                    mode=0755,
                                    recursive_chmod=True
                                    )
                params.HdfsResource("/ranger/audit/solr",
                                    type="directory",
                                    action="create_on_execute",
                                    owner=params.solr_user,
                                    group=params.solr_user,
                                    mode=0700,
                                    recursive_chmod=True
                                    )
                params.HdfsResource(None, action="execute")

        File(os.path.join(params.stack_root, "ranger-solr-plugin", "ranger_credential_helper.py"),
             mode=0755)

        jar_files = os.listdir(os.path.join(params.stack_root,"ranger-solr-plugin/lib"))

        for jar_file in jar_files:
            plugin_dir = os.path.join(params.stack_root, "ranger-solr-plugin/lib", jar_file)

            Execute(('ln', '-sf', plugin_dir,
                     os.path.join(params.stack_root, "solr/server/solr-webapp/webapp/WEB-INF/lib",
                                  jar_file)),
                    only_if=format('ls {plugin_dir}'),
                    sudo=True)

        setup_ranger_plugin('solr-server', 'solr', params.previous_jdbc_jar,
                            params.downloaded_custom_connector, params.driver_curl_source,
                            params.driver_curl_target, params.java64_home,
                            params.repo_name, params.solr_ranger_plugin_repo,
                            params.ranger_env, params.ranger_plugin_properties,
                            params.policy_user, params.policymgr_mgr_url,
                            params.enable_ranger_solr, conf_dict=params.solr_conf,
                            component_user=params.solr_user, component_group=params.user_group,
                            cache_service_list=['solr'],
                            plugin_audit_properties=params.ranger_solr_audit,
                            plugin_audit_attributes=params.ranger_solr_audit_attrs,
                            plugin_security_properties=params.ranger_solr_security,
                            plugin_security_attributes=params.ranger_solr_security_attrs,
                            plugin_policymgr_ssl_properties=params.ranger_solr_policymgr_ssl,
                            plugin_policymgr_ssl_attributes=params.ranger_solr_policymgr_ssl_attrs,
                            component_list=['solr'], audit_db_is_enabled=params.xa_audit_db_is_enabled,
                            credential_file=params.credential_file, xa_audit_db_password=params.xa_audit_db_password,
                            ssl_truststore_password=params.ssl_truststore_password,
                            ssl_keystore_password=params.ssl_keystore_password,
                            api_version='v2', skip_if_rangeradmin_down=not params.retryAble,
                            is_security_enabled=params.security_enabled,
                            is_stack_supports_ranger_kerberos=params.stack_supports_ranger_kerberos,
                            component_user_principal=params.solr_kerberos_principal if params.security_enabled else None,
                            component_user_keytab=params.solr_kerberos_keytab if params.security_enabled else None)

        properties_files = os.listdir(format('/etc/solr'))

        if params.security_enabled and params.enable_ranger_solr:
            solr_classes_dir = os.path.join(params.stack_root,
                                            "solr/server/solr-webapp/webapp/WEB-INF/classes")
            Directory(solr_classes_dir,
                      owner=params.solr_user,
                      group=params.user_group,
                      ignore_failures=True
                      )

            Execute(format('mkdir {solr_classes_dir}'),
                    not_if=format('ls {solr_classes_dir}'))

            for properties_file in properties_files:
                Execute(('ln', '-sf', format('/etc/solr/{properties_file}'),
                         os.path.join(params.stack_root,
                                      "solr/server/solr-webapp/webapp/WEB-INF/classes", properties_file)),
                        only_if=format('ls /etc/solr/{properties_file}'),
                        sudo=True)

        zk_port = ":" + params.zookeeper_port + ","

        if params.enable_ranger_solr:
            zookeeper_hosts_ip = zk_port.join(params.zookeeper_hosts_list) + ":" + params.zookeeper_port
            zookeeper_script = os.path.join(params.stack_root,
                                            "solr/server/scripts/cloud-scripts/zkcli.sh")

            set_solr_ranger_authorizer = format('{zookeeper_script} -zkhost {zookeeper_hosts_ip} ' +
                                                '-cmd put /solr/security.json \'{{\"authentication":{{\"class\":\"org.apache.solr.security.KerberosPlugin\"}},\"authorization\":{{\"class\": ' +
                                                '\"org.apache.ranger.authorization.solr.authorizer.RangerSolrAuthorizer\"}}}}\'')

            Execute(set_solr_ranger_authorizer)

    else:
        Logger.info('Ranger admin not installed')
