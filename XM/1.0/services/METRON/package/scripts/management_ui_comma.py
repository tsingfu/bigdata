#!/usr/bin/env python

from resource_management.core.logger import Logger
from resource_management.core.resources.system import Execute, File

# Wrap major operations and functionality in this class
class ManagementUICommands:
    __params = None

    def __init__(self, params):
        if params is None:
            raise ValueError("params argument is required for initialization")
        self.__params = params

    def start_management_ui(self):
        Logger.info('Starting Management UI')
        Execute("service metron-management-ui start")
        Logger.info('Done starting Management UI')

    def stop_management_ui(self):
        Logger.info('Stopping Management UI')
        Execute("service metron-management-ui stop")
        Logger.info('Done stopping Management UI')

    def restart_management_ui(self, env):
        Logger.info('Restarting the Management UI')
        Execute('service metron-management-ui restart')
        Logger.info('Done restarting the Management UI')
