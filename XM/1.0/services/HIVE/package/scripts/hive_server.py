#!/usr/bin/env python

from resource_management.libraries.script.script import Script
from resource_management.libraries.functions.check_process_status import check_process_status
from ambari_commons.constants import UPGRADE_TYPE_ROLLING
from setup_ranger_hive import setup_ranger_hive
import hive_server_upgrade
from hive import hive, install_hive
from hive_service import hive_service
from resource_management.core.resources.zkmigrator import ZkMigrator
from resource_management.core.logger import Logger
from resource_management.core.resources.system import File, Execute, Directory

def copy_to_hdfs(name, user_group, owner, custom_source_file=None, custom_dest_file=None):
    import os
    import params
    dest_dir = os.path.dirname(custom_dest_file)
    params.HdfsResource(dest_dir,
                        type="directory",
                        action="create_on_execute",
                        owner=owner,
                        mode=0555
                        )

    params.HdfsResource(custom_dest_file,
                        type="file",
                        action="create_on_execute",
                        source=custom_source_file,
                        group=user_group,
                        owner=owner,
                        mode=0444,
                        replace_existing_files=False,
                        )
    params.HdfsResource(None, action="execute")

class HiveServer(Script):
    def install(self, env):
        install_hive()

    def configure(self, env):
        import params
        env.set_params(params)
        hive(name='hiveserver2')

    def get_component_name(self):
        return "hive-server2"

    def start(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        install_hive()
        self.configure(env)  # FOR SECURITY
        setup_ranger_hive(upgrade_type=upgrade_type)
        hive_service('hiveserver2', action='start', upgrade_type=upgrade_type)

        # only perform this if upgrading and rolling; a non-rolling upgrade doesn't need
        # to do this since hive is already down
        if upgrade_type == UPGRADE_TYPE_ROLLING:
            hive_server_upgrade.deregister()

    def stop(self, env, upgrade_type=None):
        import params
        env.set_params(params)

        # always de-register the old hive instance so that ZK can route clients
        # to the newly created hive server
        try:
            if upgrade_type is not None:
                hive_server_upgrade.deregister()
        except Exception as exception:
            Logger.exception(str(exception))

        # even during rolling upgrades, Hive Server will be stopped - this is because Ambari will
        # not support the "port-change/deregister" workflow as it would impact Hive clients
        # which do not use ZK discovery.
        hive_service('hiveserver2', action='stop')

    def status(self, env):
        import status_params
        env.set_params(status_params)

        # Recursively check all existing gmetad pid files
        check_process_status(status_params.hive_pid)

    def pre_upgrade_restart(self, env, upgrade_type=None):
        Logger.info("Executing Hive Server Stack Upgrade pre-restart")
        import params
        env.set_params(params)

        Execute('wget http://yum.example.com/hadoop/hdfs/tez.tar.gz -O ' + params.tez_tar_source)

        copy_to_hdfs(
            "tez",
            params.user_group,
            params.hdfs_user,
            custom_source_file=params.tez_tar_source,
            custom_dest_file=params.tez_tar_dest_file)
        params.HdfsResource(None, action="execute")

        Execute('/bin/rm -f ' + params.tez_tar_source)

    def _base_node(self, path):
        if not path.startswith('/'):
            path = '/' + path
        try:
            return '/' + path.split('/')[1]
        except IndexError:
            return path

    def disable_security(self, env):
        import params
        zkmigrator = ZkMigrator(params.hive_zookeeper_quorum, params.java_exec, params.java64_home, params.jaas_file,
                                params.hive_user)
        if params.hive_cluster_token_zkstore:
            zkmigrator.set_acls(self._base_node(params.hive_cluster_token_zkstore), 'world:anyone:crdwa')
        if params.hive_zk_namespace:
            zkmigrator.set_acls(
                params.hive_zk_namespace if params.hive_zk_namespace.startswith(
                    '/') else '/' + params.hive_zk_namespace,
                'world:anyone:crdwa')

    def get_log_folder(self):
        import params
        return params.hive_log_dir

    def get_user(self):
        import params
        return params.hive_user

    def get_pid_files(self):
        import status_params
        return [status_params.hive_pid]


if __name__ == "__main__":
    HiveServer().execute()
