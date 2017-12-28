# -*- coding: utf-8 -*-

import uuid
import os.path as path

from resource_management.core.resources.system import Directory, Execute, File, Link
from common import create_connectors, delete_connectors
from presto_coordinator import install_presto
from resource_management.core.source import StaticFile, Template, InlineTemplate
from resource_management.libraries.script.script import Script


class Worker(Script):
    def install(self, env):
        install_presto(first=True)
        self.configure(env)

    def stop(self, env):
        from params import daemon_control_script, presto_user
        Execute('{0} stop'.format(daemon_control_script), user=presto_user)

    def start(self, env):
        from params import daemon_control_script, presto_user
        install_presto()
        self.configure(self)
        Execute('{0} start'.format(daemon_control_script), user=presto_user)

    def status(self, env):
        from params import daemon_control_script, presto_user
        Execute('{0} status'.format(daemon_control_script), user=presto_user)

    def restart(self, env):
        self.stop(env)
        self.start(env)

    def configure(self, env):
        import params
        Directory(
            [params.config_directory, params.node_properties['node.data-dir']],
            owner=params.presto_user,
            group=params.user_group,
            mode=0775,
            create_parents=True)
        File(params.daemon_control_script,
             owner=params.presto_user,
             content=InlineTemplate(params.env_sh_template),
             mode=0775
             )
        from params import node_properties, jvm_config, config_properties, \
            config_directory, memory_configs, connectors_to_add, connectors_to_delete
        key_val_template = '{0}={1}\n'

        with open(path.join(config_directory, 'node.properties'), 'w') as f:
            for key, value in node_properties.iteritems():
                f.write(key_val_template.format(key, value))
            f.write(key_val_template.format('node.id', str(uuid.uuid4())))

        with open(path.join(config_directory, 'jvm.config'), 'w') as f:
            f.write(jvm_config['jvm.config'])

        with open(path.join(config_directory, 'config.properties'), 'w') as f:
            for key, value in config_properties.iteritems():
                if key == 'query.queue-config-file' and value.strip() == '':
                    continue
                if key in memory_configs:
                    value += 'GB'
                f.write(key_val_template.format(key, value))
            f.write(key_val_template.format('coordinator', 'false'))
        security_list = []
        security = ''
        if params.security_enabled:
            security_list.append("'hive.metastore.authentication.type=KERBEROS'")
            security_list.append("'hive.metastore.service.principal=" + params.hive_metastore_principal + "'")
            security_list.append("'hive.metastore.client.principal=" + params.presto_principal + "'")
            security_list.append("'hive.metastore.client.keytab=" + params.presto_keytab + "'")
            security_list.append("'hive.hdfs.authentication.type=KERBEROS'")
            security_list.append("'hive.hdfs.impersonation.enabled=true'")
            security_list.append("'hive.hdfs.presto.principal=" + params.presto_principal + "'")
            security_list.append("'hive.hdfs.presto.keytab=" + params.presto_keytab + "'")
            security = ','.join(security_list)

        create_connectors(node_properties, connectors_to_add)
        delete_connectors(node_properties, connectors_to_delete)
        create_connectors(node_properties,
                          "{'hive': ['connector.name=hive-hadoop2', 'hive.metastore.uri=" + params.hive_metastore_uri + "','hive.config.resources=/etc/hadoop/core-site.xml,/etc/hadoop/hdfs-site.xml','hive.allow-drop-table=true'," + security + "]}")

        if len(params.kafka_broker_hosts) > 0:
            create_connectors(node_properties,
                              "{'kafka': ['connector.name=kafka', 'kafka.table-names=*','kafka.nodes=" + ','.join(
                                  params.kafka_broker_hosts) + "']}")


if __name__ == '__main__':
    Worker().execute()
