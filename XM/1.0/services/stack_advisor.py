#!/usr/bin/env ambari-python-wrap
from resource_management.core.logger import Logger
import json
from resource_management.libraries.functions import format
from resource_management.libraries.functions.get_bare_principal import get_bare_principal
from stack_advisor import DefaultStackAdvisor
from math import ceil, floor, log
import re
import os
import sys
import socket
import logging
import random
import string
import math

def getUserOperationContext(services, contextName):
    if services:
        if 'user-context' in services.keys():
            userContext = services["user-context"]
            if contextName in userContext:
                return userContext[contextName]
    return None


# if serviceName is being added
def isServiceBeingAdded(services, serviceName):
    if services:
        if 'user-context' in services.keys():
            userContext = services["user-context"]
            if DefaultStackAdvisor.OPERATION in userContext and \
                            'AddService' == userContext[DefaultStackAdvisor.OPERATION] and \
                            DefaultStackAdvisor.OPERATION_DETAILS in userContext:
                if -1 != userContext["operation_details"].find(serviceName):
                    return True
    return False


# Validation helper methods
def getSiteProperties(configurations, siteName):
    siteConfig = configurations.get(siteName)
    if siteConfig is None:
        return None
    return siteConfig.get("properties")


def getServicesSiteProperties(services, siteName):
    configurations = services.get("configurations")
    if not configurations:
        return None
    siteConfig = configurations.get(siteName)
    if siteConfig is None:
        return None
    return siteConfig.get("properties")


def round_to_n(mem_size, n=128):
    return int(round(mem_size / float(n))) * int(n)


def checkXmxValueFormat(value):
    p = re.compile('-Xmx(\d+)(b|k|m|g|p|t|B|K|M|G|P|T)?')
    matches = p.findall(value)
    return len(matches) == 1


def getXmxSize(value):
    p = re.compile("-Xmx(\d+)(.?)")
    result = p.findall(value)[0]
    if len(result) > 1:
        # result[1] - is a space or size formatter (b|k|m|g etc)
        return result[0] + result[1].lower()
    return result[0]


def to_number(s):
    try:
        return int(re.sub("\D", "", s))
    except ValueError:
        return None


def formatXmxSizeToBytes(value):
    value = value.lower()
    if len(value) == 0:
        return 0
    modifier = value[-1]

    if modifier == ' ' or modifier in "0123456789":
        modifier = 'b'
    m = {
        modifier == 'b': 1,
        modifier == 'k': 1024,
        modifier == 'm': 1024 * 1024,
        modifier == 'g': 1024 * 1024 * 1024,
        modifier == 't': 1024 * 1024 * 1024 * 1024,
        modifier == 'p': 1024 * 1024 * 1024 * 1024 * 1024
    }[1]
    return to_number(value) * m


def getPort(address):
    """
    Extracts port from the address like 0.0.0.0:1019
    """
    if address is None:
        return None
    m = re.search(r'(?:http(?:s)?://)?([\w\d.]*):(\d{1,5})', address)
    if m is not None:
        return int(m.group(2))
    else:
        return None


def isSecurePort(port):
    """
    Returns True if port is root-owned at *nix systems
    """
    if port is not None:
        return port < 1024
    else:
        return False


def getMountPointForDir(dir, mountPoints):
    """
    :param dir: Directory to check, even if it doesn't exist.
    :return: Returns the closest mount point as a string for the directory.
    if the "dir" variable is None, will return None.
    If the directory does not exist, will return "/".
    """
    bestMountFound = None
    if dir:
        dir = re.sub("^file://", "", dir, count=1).strip().lower()

        # If the path is "/hadoop/hdfs/data", then possible matches for mounts could be
        # "/", "/hadoop/hdfs", and "/hadoop/hdfs/data".
        # So take the one with the greatest number of segments.
        for mountPoint in mountPoints:
            # Ensure that the mount path and the dir path ends with "/"
            # The mount point "/hadoop" should not match with the path "/hadoop1"
            if os.path.join(dir, "").startswith(os.path.join(mountPoint, "")):
                if bestMountFound is None:
                    bestMountFound = mountPoint
                elif os.path.join(bestMountFound, "").count(os.path.sep) < os.path.join(mountPoint, "").count(
                        os.path.sep):
                    bestMountFound = mountPoint

    return bestMountFound


class XM10StackAdvisor(DefaultStackAdvisor):
    CLUSTER_CREATE_OPERATION = "ClusterCreate"
    ADD_SERVICE_OPERATION = "AddService"
    EDIT_CONFIG_OPERATION = "EditConfig"
    RECOMMEND_ATTRIBUTE_OPERATION = "RecommendAttribute"
    OPERATION = "operation"
    OPERATION_DETAILS = "operation_details"

    ADVISOR_CONTEXT = "advisor_context"
    CALL_TYPE = "call_type"

    def __init__(self):
        self.allRequestedProperties = {}
        super(XM10StackAdvisor, self).__init__()
        Logger.initialize_logger()
        self.initialize_logger('XM10StackAdvisor')

    def getConfigurationsValidationItems(self, services, hosts):
        """Returns array of Validation objects about issues with configuration values provided in services"""
        items = []

        recommendations = self.recommendConfigurations(services, hosts)
        recommendedDefaults = recommendations["recommendations"]["blueprint"]["configurations"]
        configurations = services["configurations"]
        #
        # for service in services["services"]:
        #     items.extend(
        #         self.getConfigurationsValidationItemsForService(configurations, recommendedDefaults, service, services,
        #                                                         hosts))

        clusterWideItems = self.validateClusterConfigurations(configurations, services, hosts)
        items.extend(clusterWideItems)
        return items

    def initialize_logger(self, name='XM10StackAdvisor', logging_level=logging.INFO,
                          format='%(asctime)s %(levelname)s %(name)s %(funcName)s: - %(message)s'):
        # set up logging (two separate loggers for stderr and stdout with different loglevels)
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging_level)
        formatter = logging.Formatter(format)
        chout = logging.StreamHandler(sys.stdout)
        chout.setLevel(logging_level)
        chout.setFormatter(formatter)
        cherr = logging.StreamHandler(sys.stderr)
        cherr.setLevel(logging.ERROR)
        cherr.setFormatter(formatter)
        self.logger.handlers = []
        self.logger.addHandler(cherr)
        self.logger.addHandler(chout)

    def getServiceConfigurationRecommenderDict(self):
        parentRecommendConfDict = super(
            XM10StackAdvisor, self).getServiceConfigurationRecommenderDict()
        childRecommendConfDict = {
            "HDFS": self.recommendHDFSConfigurations,
            "HIVE": self.recommendHIVEConfigurations,
            "HBASE": self.recommendHBASEConfigurations,
            "YARN": self.recommendYARNConfigurations,
            "KAFKA": self.recommendKAFKAConfigurations,
            "MAPREDUCE2": self.recommendMapReduce2Configurations,
            "STORM": self.recommendStormConfigurations,
            "AMBARI_METRICS": self.recommendAmsConfigurations,
            "ZOOKEEPER": self.recommendZookeeperConfigurations,
            "SPARK2": self.recommendSpark2Configurations,
            "SPARK": self.recommendSparkConfigurations,
            "ZEPPELIN": self.recommendZeppelinConfigurations,
            "DRUID": self.recommendDruidConfigurations,
            "RANGER": self.recommendRangerConfigurations,
            "ATLAS": self.recommendAtlasConfigurations,
            "FALCON": self.recommendFalconConfigurations,
            "SOLR": self.recommendSolrConfigurations,
            "TITAN": self.recommendTitanConfigurations
        }
        parentRecommendConfDict.update(childRecommendConfDict)
        return parentRecommendConfDict

    def constructAtlasRestAddress(self, services, hosts):
        """
        :param services: Collection of services in the cluster with configs
        :param hosts: Collection of hosts in the cluster
        :return: The suggested property for atlas.rest.address if it is valid, otherwise, None
        """
        atlas_rest_address = None
        services_list = [service["StackServices"]["service_name"] for service in services["services"]]
        is_atlas_in_cluster = "ATLAS" in services_list

        atlas_server_hosts_info = self.getHostsWithComponent("ATLAS", "ATLAS_SERVER", services, hosts)
        if is_atlas_in_cluster and atlas_server_hosts_info and len(atlas_server_hosts_info) > 0:
            # Multiple Atlas Servers can exist, so sort by hostname to create deterministic csv
            atlas_host_names = [e['Hosts']['host_name'] for e in atlas_server_hosts_info]
            if len(atlas_host_names) > 1:
                atlas_host_names = sorted(atlas_host_names)

            scheme = "http"
            metadata_port = "21000"
            atlas_server_default_https_port = "21443"
            tls_enabled = "false"
            if 'application-properties' in services['configurations']:
                if 'atlas.enableTLS' in services['configurations']['application-properties']['properties']:
                    tls_enabled = services['configurations']['application-properties']['properties']['atlas.enableTLS']
                if 'atlas.server.http.port' in services['configurations']['application-properties']['properties']:
                    metadata_port = str(
                        services['configurations']['application-properties']['properties']['atlas.server.http.port'])

                if str(tls_enabled).lower() == "true":
                    scheme = "https"
                    if 'atlas.server.https.port' in services['configurations']['application-properties']['properties']:
                        metadata_port = str(services['configurations']['application-properties']['properties'][
                                                'atlas.server.https.port'])
                    else:
                        metadata_port = atlas_server_default_https_port

            atlas_rest_address_list = ["{0}://{1}:{2}".format(scheme, hostname, metadata_port) for hostname in
                                       atlas_host_names]
            atlas_rest_address = ",".join(atlas_rest_address_list)
            Logger.info("Constructing atlas.rest.address=%s" % atlas_rest_address)
        return atlas_rest_address

    def recommendAtlasConfigurations(self, configurations, clusterData, services, hosts):
        putAtlasApplicationProperty = self.putProperty(configurations, "application-properties", services)
        putAtlasRangerPluginProperty = self.putProperty(configurations, "ranger-atlas-plugin-properties", services)
        putAtlasEnvProperty = self.putProperty(configurations, "atlas-env", services)

        servicesList = [service["StackServices"]["service_name"] for service in services["services"]]

        # Generate atlas.rest.address since the value is always computed
        atlas_rest_address = self.constructAtlasRestAddress(services, hosts)
        if atlas_rest_address is not None:
            putAtlasApplicationProperty("atlas.rest.address", atlas_rest_address)

        if "AMBARI_INFRA" in servicesList and 'infra-solr-env' in services['configurations']:
            if 'infra_solr_znode' in services['configurations']['infra-solr-env']['properties']:
                infra_solr_znode = services['configurations']['infra-solr-env']['properties']['infra_solr_znode']
            else:
                infra_solr_znode = None

            zookeeper_hosts = self.getHostNamesWithComponent("ZOOKEEPER", "ZOOKEEPER_SERVER", services)
            zookeeper_host_arr = []

            zookeeper_port = self.getZKPort(services)
            for i in range(len(zookeeper_hosts)):
                zookeeper_host = zookeeper_hosts[i] + ':' + zookeeper_port
                if infra_solr_znode is not None:
                    zookeeper_host += infra_solr_znode
                zookeeper_host_arr.append(zookeeper_host)

            solr_zookeeper_url = ",".join(zookeeper_host_arr)

            putAtlasApplicationProperty('atlas.graph.index.search.solr.zookeeper-url', solr_zookeeper_url)
        else:
            putAtlasApplicationProperty('atlas.graph.index.search.solr.zookeeper-url', "")

        # Kafka section
        if "KAFKA" in servicesList and 'kafka-broker' in services['configurations']:
            kafka_hosts = self.getHostNamesWithComponent("KAFKA", "KAFKA_BROKER", services)

            if 'port' in services['configurations']['kafka-broker']['properties']:
                kafka_broker_port = services['configurations']['kafka-broker']['properties']['port']
            else:
                kafka_broker_port = '6667'

            if 'kafka-broker' in services['configurations'] and 'listeners' in \
                    services['configurations']['kafka-broker']['properties']:
                kafka_server_listeners = services['configurations']['kafka-broker']['properties']['listeners']
            else:
                kafka_server_listeners = 'PLAINTEXT://localhost:6667'

            security_enabled = self.isSecurityEnabled(services)

            if ',' in kafka_server_listeners and len(kafka_server_listeners.split(',')) > 1:
                for listener in kafka_server_listeners.split(','):
                    listener = listener.strip().split(':')
                    if len(listener) == 3:
                        if 'SASL' in listener[0] and security_enabled:
                            kafka_broker_port = listener[2]
                            break
                        elif 'SASL' not in listener[0] and not security_enabled:
                            kafka_broker_port = listener[2]
            else:
                listener = kafka_server_listeners.strip().split(':')
                if len(listener) == 3:
                    kafka_broker_port = listener[2]

            kafka_host_arr = []
            for i in range(len(kafka_hosts)):
                kafka_host_arr.append(kafka_hosts[i] + ':' + kafka_broker_port)

            kafka_bootstrap_servers = ",".join(kafka_host_arr)

            if 'zookeeper.connect' in services['configurations']['kafka-broker']['properties']:
                kafka_zookeeper_connect = services['configurations']['kafka-broker']['properties']['zookeeper.connect']
            else:
                kafka_zookeeper_connect = None

            putAtlasApplicationProperty('atlas.kafka.bootstrap.servers', kafka_bootstrap_servers)
            putAtlasApplicationProperty('atlas.kafka.zookeeper.connect', kafka_zookeeper_connect)
        else:
            putAtlasApplicationProperty('atlas.kafka.bootstrap.servers', "")
            putAtlasApplicationProperty('atlas.kafka.zookeeper.connect', "")

        if "HBASE" in servicesList and 'hbase-site' in services['configurations']:
            if 'hbase.zookeeper.quorum' in services['configurations']['hbase-site']['properties']:
                hbase_zookeeper_quorum = services['configurations']['hbase-site']['properties'][
                    'hbase.zookeeper.quorum']
            else:
                hbase_zookeeper_quorum = ""

            putAtlasApplicationProperty('atlas.graph.storage.hostname', hbase_zookeeper_quorum)
            putAtlasApplicationProperty('atlas.audit.hbase.zookeeper.quorum', hbase_zookeeper_quorum)
        else:
            putAtlasApplicationProperty('atlas.graph.storage.hostname', "")
            putAtlasApplicationProperty('atlas.audit.hbase.zookeeper.quorum', "")

        if "ranger-env" in services["configurations"] and "ranger-atlas-plugin-properties" in services[
            "configurations"] and \
                        "ranger-atlas-plugin-enabled" in services["configurations"]["ranger-env"]["properties"]:
            ranger_atlas_plugin_enabled = services["configurations"]["ranger-env"]["properties"][
                "ranger-atlas-plugin-enabled"]
            putAtlasRangerPluginProperty('ranger-atlas-plugin-enabled', ranger_atlas_plugin_enabled)

        ranger_atlas_plugin_enabled = ''
        if 'ranger-atlas-plugin-properties' in configurations and 'ranger-atlas-plugin-enabled' in \
                configurations['ranger-atlas-plugin-properties']['properties']:
            ranger_atlas_plugin_enabled = configurations['ranger-atlas-plugin-properties']['properties'][
                'ranger-atlas-plugin-enabled']
        elif 'ranger-atlas-plugin-properties' in services['configurations'] and 'ranger-atlas-plugin-enabled' in \
                services['configurations']['ranger-atlas-plugin-properties']['properties']:
            ranger_atlas_plugin_enabled = services['configurations']['ranger-atlas-plugin-properties']['properties'][
                'ranger-atlas-plugin-enabled']

        if ranger_atlas_plugin_enabled and (ranger_atlas_plugin_enabled.lower() == 'Yes'.lower()):
            putAtlasApplicationProperty('atlas.authorizer.impl', 'ranger')
        else:
            putAtlasApplicationProperty('atlas.authorizer.impl', 'simple')

        # atlas server memory settings
        if 'atlas-env' in services['configurations']:
            atlas_server_metadata_size = 50000
            if 'atlas_server_metadata_size' in services['configurations']['atlas-env']['properties']:
                atlas_server_metadata_size = float(
                    services['configurations']['atlas-env']['properties']['atlas_server_metadata_size'])

            atlas_server_xmx = 2048

            if 300000 <= atlas_server_metadata_size < 500000:
                atlas_server_xmx = 1024 * 5
            if 500000 <= atlas_server_metadata_size < 1000000:
                atlas_server_xmx = 1024 * 10
            if atlas_server_metadata_size >= 1000000:
                atlas_server_xmx = 1024 * 16

            atlas_server_max_new_size = (atlas_server_xmx / 100) * 30

            putAtlasEnvProperty("atlas_server_xmx", atlas_server_xmx)
            putAtlasEnvProperty("atlas_server_max_new_size", atlas_server_max_new_size)
        knox_host = 'localhost'
        knox_port = '8443'
        if 'KNOX' in servicesList:
            knox_hosts = self.getComponentHostNames(services, "KNOX", "KNOX_GATEWAY")
            if len(knox_hosts) > 0:
                knox_hosts.sort()
                knox_host = knox_hosts[0]
            if 'gateway-site' in services['configurations'] and 'gateway.port' in \
                    services['configurations']["gateway-site"]["properties"]:
                knox_port = services['configurations']["gateway-site"]["properties"]['gateway.port']
            putAtlasApplicationProperty('atlas.sso.knox.providerurl',
                                        'https://{0}:{1}/gateway/knoxsso/api/v1/websso'.format(knox_host, knox_port))

    def recommendDruidConfigurations(self, configurations, clusterData,
                                     services, hosts):

        # druid is not in list of services to be installed
        if 'druid-common' not in services['configurations']:
            return

        componentsListList = [service["components"]
                              for service in services["services"]]
        componentsList = [item["StackServiceComponents"]
                          for sublist in componentsListList
                          for item in sublist]
        servicesList = [service["StackServices"]["service_name"]
                        for service in services["services"]]
        putCommonProperty = self.putProperty(configurations, "druid-common",
                                             services)

        putCommonProperty('druid.zk.service.host',
                          self.getZKHostPortString(services))
        self.recommendDruidMaxMemoryLimitConfigurations(
            configurations, clusterData, services, hosts)

        # recommending the metadata storage uri
        database_name = services['configurations']["druid-common"][
            "properties"]["database_name"]
        metastore_hostname = services['configurations']["druid-common"][
            "properties"]["metastore_hostname"]
        database_type = services['configurations']["druid-common"][
            "properties"]["druid.metadata.storage.type"]
        metadata_storage_port = "1527"
        mysql_module_name = "mysql-metadata-storage"
        postgres_module_name = "postgresql-metadata-storage"
        extensions_load_list = services['configurations']['druid-common'][
            'properties']['druid.extensions.loadList']
        putDruidCommonProperty = self.putProperty(configurations,
                                                  "druid-common", services)

        extensions_load_list = self.removeFromList(extensions_load_list,
                                                   mysql_module_name)
        extensions_load_list = self.removeFromList(extensions_load_list,
                                                   postgres_module_name)

        if database_type == 'mysql':
            metadata_storage_port = "3306"
            extensions_load_list = self.addToList(extensions_load_list,
                                                  mysql_module_name)

        if database_type == 'postgresql':
            extensions_load_list = self.addToList(extensions_load_list,
                                                  postgres_module_name)
            metadata_storage_port = "5432"

        putDruidCommonProperty('druid.metadata.storage.connector.port',
                               metadata_storage_port)
        putDruidCommonProperty(
            'druid.metadata.storage.connector.connectURI',
            self.getMetadataConnectionString(database_type).format(
                metastore_hostname, database_name, metadata_storage_port))
        # HDFS is installed
        if "HDFS" in servicesList and "hdfs-site" in services[
            "configurations"]:
            # recommend HDFS as default deep storage
            extensions_load_list = self.addToList(extensions_load_list,
                                                  "druid-hdfs-storage")
            putCommonProperty("druid.storage.type", "hdfs")
            putCommonProperty("druid.storage.storageDirectory",
                              "/user/druid/data")
            # configure indexer logs configs
            putCommonProperty("druid.indexer.logs.type", "hdfs")
            putCommonProperty("druid.indexer.logs.directory",
                              "/user/druid/logs")

        if "KAFKA" in servicesList:
            extensions_load_list = self.addToList(
                extensions_load_list, "druid-kafka-indexing-service")

        if 'AMBARI_METRICS' in servicesList:
            extensions_load_list = self.addToList(extensions_load_list,
                                                  "ambari-metrics-emitter")

        putCommonProperty('druid.extensions.loadList', extensions_load_list)

        # JVM Configs go to env properties
        putEnvProperty = self.putProperty(configurations, "druid-env",
                                          services)

        # processing thread pool Config
        for component in ['DRUID_HISTORICAL', 'DRUID_BROKER']:
            component_hosts = self.getHostsWithComponent("DRUID", component,
                                                         services, hosts)
            nodeType = self.DRUID_COMPONENT_NODE_TYPE_MAP[component]
            putComponentProperty = self.putProperty(
                configurations, format("druid-{nodeType}"), services)
            if (component_hosts is not None and len(component_hosts) > 0):
                totalAvailableCpu = self.getMinCpu(component_hosts)
                processingThreads = 1
                if totalAvailableCpu > 1:
                    processingThreads = totalAvailableCpu - 1
                putComponentProperty('druid.processing.numThreads',
                                     processingThreads)
                putComponentProperty('druid.server.http.numThreads', max(10, (
                    totalAvailableCpu * 17) / 16 + 2) + 30)

        # superset is in list of services to be installed
        if 'druid-superset' in services['configurations']:
            # Recommendations for Superset
            superset_database_type = services['configurations'][
                "druid-superset"]["properties"]["SUPERSET_DATABASE_TYPE"]
            putSupersetProperty = self.putProperty(configurations,
                                                   "druid-superset", services)

            if superset_database_type == "mysql":
                putSupersetProperty("SUPERSET_DATABASE_PORT", "3306")
            elif superset_database_type == "postgresql":
                putSupersetProperty("SUPERSET_DATABASE_PORT", "5432")

    def recommendDruidMaxMemoryLimitConfigurations(
            self, configurations, clusterData, services, hosts):
        putEnvPropertyAttribute = self.putPropertyAttribute(configurations,
                                                            "druid-env")
        for component in ["DRUID_HISTORICAL", "DRUID_MIDDLEMANAGER",
                          "DRUID_BROKER", "DRUID_OVERLORD",
                          "DRUID_COORDINATOR"]:
            component_hosts = self.getHostsWithComponent("DRUID", component,
                                                         services, hosts)
            if component_hosts is not None and len(component_hosts) > 0:
                totalAvailableMem = self.getMinMemory(
                    component_hosts) / 1024  # In MB
                nodeType = self.DRUID_COMPONENT_NODE_TYPE_MAP[component]
                putEnvPropertyAttribute(
                    format('druid.{nodeType}.jvm.heap.memory'), 'maximum',
                    max(totalAvailableMem, 1024))

    DRUID_COMPONENT_NODE_TYPE_MAP = {
        'DRUID_BROKER': 'broker',
        'DRUID_COORDINATOR': 'coordinator',
        'DRUID_HISTORICAL': 'historical',
        'DRUID_MIDDLEMANAGER': 'middlemanager',
        'DRUID_OVERLORD': 'overlord',
        'DRUID_ROUTER': 'router'
    }

    def getZKHostPortString(self, services, include_port=True):
        """
    Returns the comma delimited string of zookeeper server host with the configure port installed in a cluster
    Example: zk.host1.org:2181,zk.host2.org:2181,zk.host3.org:2181
    include_port boolean param -> If port is also needed.
    """
        servicesList = [service["StackServices"]["service_name"]
                        for service in services["services"]]
        include_zookeeper = "ZOOKEEPER" in servicesList
        zookeeper_host_port = ''

        if include_zookeeper:
            zookeeper_hosts = self.getHostNamesWithComponent(
                "ZOOKEEPER", "ZOOKEEPER_SERVER", services)
            zookeeper_host_port_arr = []

            if include_port:
                zookeeper_port = self.getZKPort(services)
                for i in range(len(zookeeper_hosts)):
                    zookeeper_host_port_arr.append(zookeeper_hosts[i] + ':' +
                                                   zookeeper_port)
            else:
                for i in range(len(zookeeper_hosts)):
                    zookeeper_host_port_arr.append(zookeeper_hosts[i])

            zookeeper_host_port = ",".join(zookeeper_host_port_arr)
        return zookeeper_host_port

    def getZKPort(self, services):
        zookeeper_port = '2181'  # default port
        if 'zoo.cfg' in services['configurations'] and (
                    'clientPort' in
                    services['configurations']['zoo.cfg']['properties']):
            zookeeper_port = services['configurations']['zoo.cfg'][
                'properties']['clientPort']
        return zookeeper_port

    def getPreferredMountPoints(self, hostInfo):
        # '/etc/resolv.conf', '/etc/hostname', '/etc/hosts' are docker specific mount points
        undesirableMountPoints = ["/", "/home", "/etc/resolv.conf", "/etc/hosts",
                                  "/etc/hostname", "/tmp"]
        undesirableFsTypes = ["devtmpfs", "tmpfs", "vboxsf", "CDFS"]
        mountPoints = []
        if hostInfo and "disk_info" in hostInfo:
            mountPointsDict = {}
            for mountpoint in hostInfo["disk_info"]:
                if not (mountpoint["mountpoint"] in undesirableMountPoints or
                            mountpoint["mountpoint"].startswith(("/boot", "/mnt")) or
                                mountpoint["type"] in undesirableFsTypes or
                                mountpoint["available"] == str(0)):
                    mountPointsDict[mountpoint["mountpoint"]] = self.to_number(mountpoint["available"])
            if mountPointsDict:
                mountPoints = sorted(mountPointsDict, key=mountPointsDict.get, reverse=True)
        mountPoints.append("/")
        return mountPoints

    @classmethod
    def to_number(cls, s):
        try:
            return int(re.sub("\D", "", s))
        except ValueError:
            return None

    def getAmsMemoryRecommendation(self, services, hosts):
        # MB per sink in hbase heapsize
        HEAP_PER_MASTER_COMPONENT = 50
        HEAP_PER_SLAVE_COMPONENT = 10

        schMemoryMap = {
            "HDFS": {
                "NAMENODE": HEAP_PER_MASTER_COMPONENT,
                "DATANODE": HEAP_PER_SLAVE_COMPONENT
            },
            "YARN": {
                "RESOURCEMANAGER": HEAP_PER_MASTER_COMPONENT,
            },
            "HBASE": {
                "HBASE_MASTER": HEAP_PER_MASTER_COMPONENT,
                "HBASE_REGIONSERVER": HEAP_PER_SLAVE_COMPONENT
            },
            "ACCUMULO": {
                "ACCUMULO_MASTER": HEAP_PER_MASTER_COMPONENT,
                "ACCUMULO_TSERVER": HEAP_PER_SLAVE_COMPONENT
            },
            "KAFKA": {
                "KAFKA_BROKER": HEAP_PER_MASTER_COMPONENT
            },
            "FLUME": {
                "FLUME_HANDLER": HEAP_PER_SLAVE_COMPONENT
            },
            "STORM": {
                "NIMBUS": HEAP_PER_MASTER_COMPONENT,
            },
            "AMBARI_METRICS": {
                "METRICS_COLLECTOR": HEAP_PER_MASTER_COMPONENT,
                "METRICS_MONITOR": HEAP_PER_SLAVE_COMPONENT
            }
        }
        total_sinks_count = 0
        # minimum heap size
        hbase_heapsize = 500
        for serviceName, componentsDict in schMemoryMap.items():
            for componentName, multiplier in componentsDict.items():
                schCount = len(
                    self.getHostsWithComponent(serviceName, componentName, services,
                                               hosts))
                hbase_heapsize += int((schCount * multiplier) ** 0.9)
                total_sinks_count += schCount
        collector_heapsize = int(hbase_heapsize / 4 if hbase_heapsize > 2048 else 512)

        return round_to_n(collector_heapsize), round_to_n(hbase_heapsize), total_sinks_count

    def recommendAmsConfigurations(self, configurations, clusterData, services,
                                   hosts):
        putAmsEnvProperty = self.putProperty(configurations, "ams-env",
                                             services)
        putAmsHbaseSiteProperty = self.putProperty(configurations,
                                                   "ams-hbase-site", services)
        putAmsSiteProperty = self.putProperty(configurations, "ams-site",
                                              services)
        putHbaseEnvProperty = self.putProperty(configurations, "ams-hbase-env",
                                               services)
        putGrafanaProperty = self.putProperty(configurations,
                                              "ams-grafana-env", services)
        putGrafanaPropertyAttribute = self.putPropertyAttribute(
            configurations, "ams-grafana-env")

        amsCollectorHosts = self.getComponentHostNames(
            services, "AMBARI_METRICS", "METRICS_COLLECTOR")

        timeline_metrics_service_webapp_address = '0.0.0.0'

        putAmsSiteProperty(
            "timeline.metrics.service.webapp.address",
            str(timeline_metrics_service_webapp_address) + ":6188")

        log_dir = "/var/log/ambari-metrics-collector"
        if "ams-env" in services["configurations"]:
            if "metrics_collector_log_dir" in services["configurations"][
                "ams-env"]["properties"]:
                log_dir = services["configurations"]["ams-env"]["properties"][
                    "metrics_collector_log_dir"]
            putHbaseEnvProperty("hbase_log_dir", log_dir)

        defaultFs = 'file:///'
        if "core-site" in services["configurations"] and \
                        "fs.defaultFS" in services["configurations"]["core-site"]["properties"]:
            defaultFs = services["configurations"]["core-site"]["properties"][
                "fs.defaultFS"]

        operatingMode = "embedded"
        if "ams-site" in services["configurations"]:
            if "timeline.metrics.service.operation.mode" in services[
                "configurations"]["ams-site"]["properties"]:
                operatingMode = services["configurations"]["ams-site"][
                    "properties"]["timeline.metrics.service.operation.mode"]

        if len(amsCollectorHosts) > 1:
            operatingMode = "distributed"
            putAmsSiteProperty("timeline.metrics.service.operation.mode",
                               operatingMode)

        if operatingMode == "distributed":
            putAmsSiteProperty("timeline.metrics.service.watcher.disabled",
                               'true')
            putAmsHbaseSiteProperty("hbase.cluster.distributed", 'true')
        else:
            putAmsSiteProperty("timeline.metrics.service.watcher.disabled",
                               'false')
            putAmsHbaseSiteProperty("hbase.cluster.distributed", 'false')

        rootDir = "file:///var/lib/ambari-metrics-collector/hbase"
        tmpDir = "/var/lib/ambari-metrics-collector/hbase-tmp"
        zk_port_default = []
        if "ams-hbase-site" in services["configurations"]:
            if "hbase.rootdir" in services["configurations"]["ams-hbase-site"][
                "properties"]:
                rootDir = services["configurations"]["ams-hbase-site"][
                    "properties"]["hbase.rootdir"]
            if "hbase.tmp.dir" in services["configurations"]["ams-hbase-site"][
                "properties"]:
                tmpDir = services["configurations"]["ams-hbase-site"][
                    "properties"]["hbase.tmp.dir"]
            if "hbase.zookeeper.property.clientPort" in services[
                "configurations"]["ams-hbase-site"]["properties"]:
                zk_port_default = services["configurations"]["ams-hbase-site"][
                    "properties"]["hbase.zookeeper.property.clientPort"]

                # Skip recommendation item if default value is present
        if operatingMode == "distributed" and not "{{zookeeper_clientPort}}" in zk_port_default:
            zkPort = self.getZKPort(services)
            putAmsHbaseSiteProperty("hbase.zookeeper.property.clientPort",
                                    zkPort)
        elif operatingMode == "embedded" and not "{{zookeeper_clientPort}}" in zk_port_default:
            putAmsHbaseSiteProperty("hbase.zookeeper.property.clientPort",
                                    "61181")

        mountpoints = ["/"]
        for collectorHostName in amsCollectorHosts:
            for host in hosts["items"]:
                if host["Hosts"]["host_name"] == collectorHostName:
                    mountpoints = self.getPreferredMountPoints(host["Hosts"])
                    break
        isLocalRootDir = rootDir.startswith("file://") or (
            defaultFs.startswith("file://") and rootDir.startswith("/"))
        if isLocalRootDir:
            rootDir = re.sub("^file:///|/", "", rootDir, count=1)
            rootDir = "file://" + os.path.join(mountpoints[0], rootDir)
        tmpDir = re.sub("^file:///|/", "", tmpDir, count=1)
        if len(mountpoints) > 1 and isLocalRootDir:
            tmpDir = os.path.join(mountpoints[1], tmpDir)
        else:
            tmpDir = os.path.join(mountpoints[0], tmpDir)
        putAmsHbaseSiteProperty("hbase.tmp.dir", tmpDir)

        if operatingMode == "distributed":
            putAmsHbaseSiteProperty("hbase.rootdir", "/user/ams/hbase")

        if operatingMode == "embedded":
            if isLocalRootDir:
                putAmsHbaseSiteProperty("hbase.rootdir", rootDir)
            else:
                putAmsHbaseSiteProperty(
                    "hbase.rootdir",
                    "file:///var/lib/ambari-metrics-collector/hbase")

        collector_heapsize, hbase_heapsize, total_sinks_count = self.getAmsMemoryRecommendation(
            services, hosts)

        putAmsEnvProperty("metrics_collector_heapsize", collector_heapsize)

        # putAmsSiteProperty("timeline.metrics.cache.size", max(
        #     100, int(log(total_sinks_count)) * 100))
        # putAmsSiteProperty("timeline.metrics.cache.commit.interval", min(
        #     10, max(12 - int(log(total_sinks_count)), 2)))

        # blockCache = 0.3, memstore = 0.35, phoenix-server = 0.15, phoenix-client = 0.25
        putAmsHbaseSiteProperty("hfile.block.cache.size", 0.3)
        putAmsHbaseSiteProperty("hbase.hregion.memstore.flush.size", 134217728)
        putAmsHbaseSiteProperty(
            "hbase.regionserver.global.memstore.upperLimit", 0.35)
        putAmsHbaseSiteProperty(
            "hbase.regionserver.global.memstore.lowerLimit", 0.3)

        if len(amsCollectorHosts) > 1:
            pass
        else:
            # blockCache = 0.3, memstore = 0.3, phoenix-server = 0.2, phoenix-client = 0.3
            if total_sinks_count >= 2000:
                putAmsHbaseSiteProperty("hbase.regionserver.handler.count", 60)
                putAmsHbaseSiteProperty("hbase.regionserver.hlog.blocksize",
                                        134217728)
                putAmsHbaseSiteProperty("hbase.regionserver.maxlogs", 64)
                putAmsHbaseSiteProperty("hbase.hregion.memstore.flush.size",
                                        268435456)
                putAmsHbaseSiteProperty(
                    "hbase.regionserver.global.memstore.upperLimit", 0.3)
                putAmsHbaseSiteProperty(
                    "hbase.regionserver.global.memstore.lowerLimit", 0.25)
                putAmsHbaseSiteProperty(
                    "phoenix.query.maxGlobalMemoryPercentage", 20)
                putAmsHbaseSiteProperty(
                    "phoenix.coprocessor.maxMetaDataCacheSize", 81920000)
                putAmsSiteProperty("phoenix.query.maxGlobalMemoryPercentage",
                                   30)
                putAmsSiteProperty(
                    "timeline.metrics.service.resultset.fetchSize", 10000)
            elif total_sinks_count >= 500:
                putAmsHbaseSiteProperty("hbase.regionserver.handler.count", 60)
                putAmsHbaseSiteProperty("hbase.regionserver.hlog.blocksize",
                                        134217728)
                putAmsHbaseSiteProperty("hbase.regionserver.maxlogs", 64)
                putAmsHbaseSiteProperty("hbase.hregion.memstore.flush.size",
                                        268435456)
                putAmsHbaseSiteProperty(
                    "phoenix.coprocessor.maxMetaDataCacheSize", 40960000)
                putAmsSiteProperty(
                    "timeline.metrics.service.resultset.fetchSize", 5000)
            else:
                putAmsHbaseSiteProperty(
                    "phoenix.coprocessor.maxMetaDataCacheSize", 20480000)
            pass

        metrics_api_handlers = min(50, max(20, int(total_sinks_count / 100)))
        putAmsSiteProperty("timeline.metrics.service.handler.thread.count",
                           metrics_api_handlers)

        # Distributed mode heap size
        if operatingMode == "distributed":
            hbase_heapsize = max(hbase_heapsize, 768)
            putHbaseEnvProperty("hbase_master_heapsize", "512")
            putHbaseEnvProperty("hbase_master_xmn_size",
                                "102")  # 20% of 512 heap size
            putHbaseEnvProperty("hbase_regionserver_heapsize", hbase_heapsize)
            putHbaseEnvProperty("regionserver_xmn_size", round_to_n(
                0.15 * hbase_heapsize, 64))
        else:
            # Embedded mode heap size : master + regionserver
            hbase_rs_heapsize = 768
            putHbaseEnvProperty("hbase_regionserver_heapsize",
                                hbase_rs_heapsize)
            putHbaseEnvProperty("hbase_master_heapsize", hbase_heapsize)
            putHbaseEnvProperty("hbase_master_xmn_size", round_to_n(0.15 * (
                hbase_heapsize + hbase_rs_heapsize), 64))

        # If no local DN in distributed mode
        if operatingMode == "distributed":
            dn_hosts = self.getComponentHostNames(services, "HDFS", "DATANODE")
            # call by Kerberos wizard sends only the service being affected
            # so it is possible for dn_hosts to be None but not amsCollectorHosts
            if dn_hosts and len(dn_hosts) > 0:
                if set(amsCollectorHosts).intersection(dn_hosts):
                    collector_cohosted_with_dn = "true"
                else:
                    collector_cohosted_with_dn = "false"
                putAmsHbaseSiteProperty("dfs.client.read.shortcircuit",
                                        collector_cohosted_with_dn)

        # split points
        scriptDir = os.path.dirname(os.path.abspath(__file__))
        metricsDir = os.path.join(
            scriptDir,
            '../../../../common-services/AMBARI_METRICS/0.1.0/package')
        serviceMetricsDir = os.path.join(metricsDir, 'files',
                                         'service-metrics')
        customServiceMetricsDir = os.path.join(
            scriptDir, '../../../../dashboards/service-metrics')
        sys.path.append(os.path.join(metricsDir, 'scripts'))
        servicesList = [service["StackServices"]["service_name"]
                        for service in services["services"]]

        from split_points import FindSplitPointsForAMSRegions

        ams_hbase_site = None
        ams_hbase_env = None

        # Overriden properties form the UI
        if "ams-hbase-site" in services["configurations"]:
            ams_hbase_site = services["configurations"]["ams-hbase-site"][
                "properties"]
        if "ams-hbase-env" in services["configurations"]:
            ams_hbase_env = services["configurations"]["ams-hbase-env"][
                "properties"]

        # Recommendations
        if not ams_hbase_site:
            ams_hbase_site = configurations["ams-hbase-site"]["properties"]
        if not ams_hbase_env:
            ams_hbase_env = configurations["ams-hbase-env"]["properties"]

        split_point_finder = FindSplitPointsForAMSRegions(
            ams_hbase_site, ams_hbase_env, serviceMetricsDir,
            operatingMode, servicesList)
        # customServiceMetricsDir, operatingMode, servicesList)

        result = split_point_finder.get_split_points()
        precision_splits = ' '
        aggregate_splits = ' '
        if result.precision:
            precision_splits = result.precision
        if result.aggregate:
            aggregate_splits = result.aggregate
        putAmsSiteProperty("timeline.metrics.host.aggregate.splitpoints",
                           ','.join(precision_splits))
        putAmsSiteProperty("timeline.metrics.cluster.aggregate.splitpoints",
                           ','.join(aggregate_splits))

        component_grafana_exists = False
        for service in services['services']:
            if 'components' in service:
                for component in service['components']:
                    if 'StackServiceComponents' in component:
                        # If Grafana is installed the hostnames would indicate its location
                        if 'METRICS_GRAFANA' in component['StackServiceComponents']['component_name'] and \
                                        len(component['StackServiceComponents']['hostnames']) != 0:
                            component_grafana_exists = True
                            break
        pass

        if not component_grafana_exists:
            putGrafanaPropertyAttribute("metrics_grafana_password", "visible",
                                        "false")

        pass

    def recommendStormConfigurations(self, configurations, clusterData,
                                     services, hosts):
        putStormSiteProperty = self.putProperty(configurations, "storm-site", services)
        putStormEnvProperty = self.putProperty(configurations, "storm-env", services)
        servicesList = [service["StackServices"]["service_name"]
                        for service in services["services"]]
        # Storm AMS integration
        if 'AMBARI_METRICS' in servicesList:
            putStormSiteProperty(
                'metrics.reporter.register',
                'org.apache.hadoop.metrics2.sink.storm.StormTimelineMetricsReporter')
        if "storm-site" in services["configurations"]:
            # atlas
            notifier_plugin_property = "storm.topology.submission.notifier.plugin.class"
            if notifier_plugin_property in services["configurations"]["storm-site"]["properties"] and \
                            services["configurations"]["storm-site"]["properties"][
                                notifier_plugin_property] is not None:

                notifier_plugin_value = services["configurations"]["storm-site"]["properties"][notifier_plugin_property]
            else:
                notifier_plugin_value = " "

            atlas_is_present = "ATLAS" in servicesList
            atlas_hook_class = "org.apache.atlas.storm.hook.StormAtlasHook"
            atlas_hook_is_set = atlas_hook_class in notifier_plugin_value
            enable_atlas_hook = False

            if atlas_is_present:
                putStormEnvProperty("storm.atlas.hook", "true")
            else:
                putStormEnvProperty("storm.atlas.hook", "false")

            if 'storm-env' in configurations and 'storm.atlas.hook' in configurations['storm-env']['properties']:
                enable_atlas_hook = configurations['storm-env']['properties']['storm.atlas.hook'] == "true"
            elif 'storm-env' in services['configurations'] and 'storm.atlas.hook' in \
                    services['configurations']['storm-env']['properties']:
                enable_atlas_hook = services['configurations']['storm-env']['properties']['storm.atlas.hook'] == "true"

            if enable_atlas_hook and not atlas_hook_is_set:
                notifier_plugin_value = atlas_hook_class if notifier_plugin_value == " " else ",".join(
                    [notifier_plugin_value, atlas_hook_class])

            if not enable_atlas_hook and atlas_hook_is_set:
                application_classes = [item for item in notifier_plugin_value.split(",") if
                                       item != atlas_hook_class and item != " "]
                notifier_plugin_value = ",".join(application_classes) if application_classes else " "

            if notifier_plugin_value.strip() != "":
                putStormSiteProperty(notifier_plugin_property, notifier_plugin_value)
            else:
                putStormStartupPropertyAttribute = self.putPropertyAttribute(configurations, "storm-site")
                putStormStartupPropertyAttribute(notifier_plugin_property, 'delete', 'true')
        storm_site = self.getServicesSiteProperties(services, "storm-site")
        storm_env = self.getServicesSiteProperties(services, "storm-env")
        putStormSiteAttributes = self.putPropertyAttribute(configurations, "storm-site")
        security_enabled = self.isSecurityEnabled(services)

        if storm_env and storm_site:
            if security_enabled:
                _storm_principal_name = storm_env[
                    'storm_principal_name'] if 'storm_principal_name' in storm_env else None
                storm_bare_jaas_principal = get_bare_principal(_storm_principal_name)
                if 'nimbus.impersonation.acl' in storm_site:
                    storm_nimbus_impersonation_acl = storm_site["nimbus.impersonation.acl"]
                    storm_nimbus_impersonation_acl.replace('{{storm_bare_jaas_principal}}', storm_bare_jaas_principal)
                    putStormSiteProperty('nimbus.impersonation.acl', storm_nimbus_impersonation_acl)
            else:
                if 'nimbus.impersonation.acl' in storm_site:
                    putStormSiteAttributes('nimbus.impersonation.acl', 'delete', 'true')
                if 'nimbus.impersonation.authorizer' in storm_site:
                    putStormSiteAttributes('nimbus.impersonation.authorizer', 'delete', 'true')

        rangerPluginEnabled = ''
        if 'ranger-storm-plugin-properties' in configurations and 'ranger-storm-plugin-enabled' in \
                configurations['ranger-storm-plugin-properties']['properties']:
            rangerPluginEnabled = configurations['ranger-storm-plugin-properties']['properties'][
                'ranger-storm-plugin-enabled']
        elif 'ranger-storm-plugin-properties' in services['configurations'] and 'ranger-storm-plugin-enabled' in \
                services['configurations']['ranger-storm-plugin-properties']['properties']:
            rangerPluginEnabled = services['configurations']['ranger-storm-plugin-properties']['properties'][
                'ranger-storm-plugin-enabled']

        storm_authorizer_class = 'org.apache.storm.security.auth.authorizer.SimpleACLAuthorizer'
        ranger_authorizer_class = 'org.apache.ranger.authorization.storm.authorizer.RangerStormAuthorizer'
        # Cluster is kerberized
        if security_enabled:
            putStormRangerPluginProperty = self.putProperty(configurations, "ranger-storm-plugin-properties", services)
            if rangerPluginEnabled and (rangerPluginEnabled.lower() == 'Yes'.lower()):
                putStormSiteProperty('nimbus.authorizer', ranger_authorizer_class)
                putStormRangerPluginProperty("ranger-storm-plugin-enabled", rangerPluginEnabled)
            else:
                putStormSiteProperty('nimbus.authorizer', storm_authorizer_class)
        else:
            putStormSiteAttributes('nimbus.authorizer', 'delete', 'true')

        servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
        # Storm AMS integration
        if 'AMBARI_METRICS' in servicesList:
            putStormSiteProperty('storm.cluster.metrics.consumer.register',
                                 '[{"class": "org.apache.hadoop.metrics2.sink.storm.StormTimelineMetricsReporter"}]')
            putStormSiteProperty('topology.metrics.consumer.register',
                                 '[{"class": "org.apache.hadoop.metrics2.sink.storm.StormTimelineMetricsSink", '
                                 '"parallelism.hint": 1, '
                                 '"whitelist": ["kafkaOffset\\\..+/", "__complete-latency", "__process-latency", '
                                 '"__receive\\\.population$", "__sendqueue\\\.population$", "__execute-count", "__emit-count", '
                                 '"__ack-count", "__fail-count", "memory/heap\\\.usedBytes$", "memory/nonHeap\\\.usedBytes$", '
                                 '"GC/.+\\\.count$", "GC/.+\\\.timeMs$"]}]')
        else:
            putStormSiteProperty('storm.cluster.metrics.consumer.register', 'null')
            putStormSiteProperty('topology.metrics.consumer.register', 'null')

    def recommendSpark2Configurations(self, configurations, clusterData,
                                      services, hosts):
        putSparkProperty = self.putProperty(configurations, "spark2-defaults",
                                            services)
        putSparkThriftSparkConf = self.putProperty(
            configurations, "spark2-thrift-sparkconf", services)

        spark_queue = self.recommendYarnQueue(services, "spark2-defaults",
                                              "spark.yarn.queue")
        if spark_queue is not None:
            putSparkProperty("spark.yarn.queue", spark_queue)

        spark_thrift_queue = self.recommendYarnQueue(
            services, "spark2-thrift-sparkconf", "spark.yarn.queue")
        if spark_thrift_queue is not None:
            putSparkThriftSparkConf("spark.yarn.queue", spark_thrift_queue)

        spark_llap_enabled = False
        if "spark2-env" in services["configurations"] and services["configurations"]["spark2-env"]["properties"]["enable_spark_llap"].lower() == "true":
            spark_llap_enabled = True
        if spark_llap_enabled and self.isSecurityEnabled(services):
            hive_zookeeper_quorum = services["configurations"]["hive-site"]["properties"]['hive.zookeeper.quorum']
            hive_server2_zookeeper_namespace = services["configurations"]["hive-site"]["properties"]['hive.server2.zookeeper.namespace']
            jdbc_url = "jdbc:hive2://" + hive_zookeeper_quorum + "/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=" + hive_server2_zookeeper_namespace
            llap_daemon_service_hosts = services["configurations"]["hive-interactive-site"]["properties"]['hive.llap.daemon.service.hosts']
            hiveserver_principal = services["configurations"]["hive-site"]["properties"]['hive.server2.authentication.kerberos.principal']
            putSparkProperty("spark.sql.hive.hiveserver2.jdbc.url", jdbc_url + ';principal=' + hiveserver_principal)
            putSparkProperty("spark.sql.hive.hiveserver2.jdbc.url.principal", hiveserver_principal)
            putSparkProperty("spark.hadoop.hive.llap.daemon.service.hosts", llap_daemon_service_hosts)
            putSparkProperty("spark.hadoop.hive.zookeeper.quorum", hive_zookeeper_quorum)

            putSparkThriftSparkConf("spark.sql.hive.hiveserver2.jdbc.url", jdbc_url + ';principal='+ hiveserver_principal + ';hive.server2.proxy.user=${user}')
            putSparkThriftSparkConf("spark.sql.hive.hiveserver2.jdbc.url.principal", hiveserver_principal)
            putSparkThriftSparkConf("spark.hadoop.hive.llap.daemon.service.hosts", llap_daemon_service_hosts)
            putSparkThriftSparkConf("spark.hadoop.hive.zookeeper.quorum", hive_zookeeper_quorum)
        else:
            putSparkPropertyAttribute = self.putPropertyAttribute(configurations,"spark2-defaults")
            putSparkThriftSparkPropertyAttribute = self.putPropertyAttribute(configurations,"spark2-thrift-sparkconf")
            putSparkPropertyAttribute('spark.sql.hive.hiveserver2.jdbc.url','delete', 'true')
            putSparkPropertyAttribute('spark.sql.hive.hiveserver2.jdbc.url.principal','delete', 'true')
            putSparkPropertyAttribute('spark.hadoop.hive.llap.daemon.service.hosts','delete', 'true')
            putSparkPropertyAttribute('spark.hadoop.hive.zookeeper.quorum','delete', 'true')

            putSparkThriftSparkPropertyAttribute('spark.sql.hive.hiveserver2.jdbc.url','delete', 'true')
            putSparkThriftSparkPropertyAttribute('spark.sql.hive.hiveserver2.jdbc.url.principal','delete', 'true')
            putSparkThriftSparkPropertyAttribute('spark.hadoop.hive.llap.daemon.service.hosts','delete', 'true')
            putSparkThriftSparkPropertyAttribute('spark.hadoop.hive.zookeeper.quorum','delete', 'true')

    def recommendSparkConfigurations(self, configurations, clusterData, services, hosts):
        """
        :type configurations dict
        :type clusterData dict
        :type services dict
        :type hosts dict
        """
        putSparkProperty = self.putProperty(configurations, "spark-defaults", services)
        putSparkThriftSparkConf = self.putProperty(configurations, "spark-thrift-sparkconf", services)

        spark_queue = self.recommendYarnQueue(services, "spark-defaults", "spark.yarn.queue")

        if spark_queue is not None:
            putSparkProperty("spark.yarn.queue", spark_queue)

        spark_thrift_queue = self.recommendYarnQueue(services, "spark-thrift-sparkconf", "spark.yarn.queue")
        if spark_thrift_queue is not None:
            putSparkThriftSparkConf("spark.yarn.queue", spark_thrift_queue)

        self.__recommendLivySuperUsers(configurations, services)
        self.__addZeppelinToLivySuperUsers(configurations, services)

        spark_llap_enabled = False
        if "spark2-env" in services["configurations"] and services["configurations"]["spark-env"]["properties"]["enable_spark_llap"].lower() == "true":
            spark_llap_enabled = True

        if spark_llap_enabled and self.isSecurityEnabled(services):
            hive_zookeeper_quorum = services["configurations"]["hive-site"]["properties"]['hive.zookeeper.quorum']
            hive_server2_zookeeper_namespace = services["configurations"]["hive-site"]["properties"]['hive.server2.zookeeper.namespace']
            jdbc_url = "jdbc:hive2://" + hive_zookeeper_quorum + "/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=" + hive_server2_zookeeper_namespace
            llap_daemon_service_hosts = services["configurations"]["hive-interactive-site"]["properties"]['hive.llap.daemon.service.hosts']
            hiveserver_principal = services["configurations"]["hive-site"]["properties"]['hive.server2.authentication.kerberos.principal']

            putSparkProperty("spark.sql.hive.hiveserver2.jdbc.url", jdbc_url + ';principal=' + hiveserver_principal)
            putSparkProperty("spark.sql.hive.hiveserver2.jdbc.url.principal", hiveserver_principal)
            putSparkProperty("spark.hadoop.hive.llap.daemon.service.hosts", llap_daemon_service_hosts)
            putSparkProperty("spark.hadoop.hive.zookeeper.quorum", hive_zookeeper_quorum)

            putSparkThriftSparkConf("spark.sql.hive.hiveserver2.jdbc.url", jdbc_url + ';principal='+ hiveserver_principal + ';hive.server2.proxy.user=${user}')
            putSparkThriftSparkConf("spark.sql.hive.hiveserver2.jdbc.url.principal", hiveserver_principal)
            putSparkThriftSparkConf("spark.hadoop.hive.llap.daemon.service.hosts", llap_daemon_service_hosts)
            putSparkThriftSparkConf("spark.hadoop.hive.zookeeper.quorum", hive_zookeeper_quorum)
        else:
            putSparkPropertyAttribute = self.putPropertyAttribute(configurations,"spark-defaults")
            putSparkThriftSparkPropertyAttribute = self.putPropertyAttribute(configurations,"spark-thrift-sparkconf")
            putSparkPropertyAttribute('spark.sql.hive.hiveserver2.jdbc.url','delete', 'true')
            putSparkPropertyAttribute('spark.sql.hive.hiveserver2.jdbc.url.principal','delete', 'true')
            putSparkPropertyAttribute('spark.hadoop.hive.llap.daemon.service.hosts','delete', 'true')
            putSparkPropertyAttribute('spark.hadoop.hive.zookeeper.quorum','delete', 'true')

            putSparkThriftSparkPropertyAttribute('spark.sql.hive.hiveserver2.jdbc.url','delete', 'true')
            putSparkThriftSparkPropertyAttribute('spark.sql.hive.hiveserver2.jdbc.url.principal','delete', 'true')
            putSparkThriftSparkPropertyAttribute('spark.hadoop.hive.llap.daemon.service.hosts','delete', 'true')
            putSparkThriftSparkPropertyAttribute('spark.hadoop.hive.zookeeper.quorum','delete', 'true')

    def recommendZeppelinConfigurations(self, configurations, clusterData, services, hosts):
        self.__addZeppelinToLivySuperUsers(configurations, services)

    def __recommendLivySuperUsers(self, configurations, services):
        """
        If Kerberos is enabled AND Zeppelin is installed and Spark Livy Server is installed, then set
        livy-conf/livy.superusers to contain the Zeppelin principal name from
        zeppelin-env/zeppelin.server.kerberos.principal

        :param configurations:
        :param services:
        """
        if self.isSecurityEnabled(services):
            zeppelin_env = self.getServicesSiteProperties(services, "zeppelin-env")

            if zeppelin_env and 'zeppelin.server.kerberos.principal' in zeppelin_env:
                zeppelin_principal = zeppelin_env['zeppelin.server.kerberos.principal']
                zeppelin_user = zeppelin_principal.split('@')[0] if zeppelin_principal else None

                if zeppelin_user:
                    livy_conf = self.getServicesSiteProperties(services, 'livy-conf')

                    if livy_conf:
                        superusers = livy_conf['livy.superusers'] if livy_conf and 'livy.superusers' in livy_conf else None

                        # add the Zeppelin user to the set of users
                        if superusers:
                            _superusers = superusers.split(',')
                            _superusers = [x.strip() for x in _superusers]
                            _superusers = filter(None, _superusers)  # Removes empty string elements from array
                        else:
                            _superusers = []

                        if zeppelin_user not in _superusers:
                            _superusers.append(zeppelin_user)

                            putLivyProperty = self.putProperty(configurations, 'livy-conf', services)
                            putLivyProperty('livy.superusers', ','.join(_superusers))

    def __addZeppelinToLivySuperUsers(self, configurations, services):
        """
        If Kerberos is enabled AND Zeppelin is installed and Spark Livy Server is installed, then set
        livy-conf/livy.superusers to contain the Zeppelin principal name from
        zeppelin-env/zeppelin.server.kerberos.principal

        :param configurations:
        :param services:
        """
        if self.isSecurityEnabled(services):
            zeppelin_env = self.getServicesSiteProperties(services, "zeppelin-env")

            if zeppelin_env and 'zeppelin.server.kerberos.principal' in zeppelin_env:
                zeppelin_principal = zeppelin_env['zeppelin.server.kerberos.principal']
                zeppelin_user = zeppelin_principal.split('@')[0] if zeppelin_principal else None

                if zeppelin_user:
                    livy_conf = self.getServicesSiteProperties(services, 'livy-conf')

                    if livy_conf:
                        superusers = livy_conf['livy.superusers'] if livy_conf and 'livy.superusers' in livy_conf else None

                        # add the Zeppelin user to the set of users
                        if superusers:
                            _superusers = superusers.split(',')
                            _superusers = [x.strip() for x in _superusers]
                            _superusers = filter(None, _superusers)  # Removes empty string elements from array
                        else:
                            _superusers = []

                        if zeppelin_user not in _superusers:
                            _superusers.append(zeppelin_user)

                            putLivyProperty = self.putProperty(configurations, 'livy-conf', services)
                            putLivyProperty('livy.superusers', ','.join(_superusers))

    def recommendMapReduce2Configurations(self, configurations, clusterData,
                                          services, hosts):
        self.recommendYARNConfigurations(configurations, clusterData, services,
                                         hosts)
        putMapredProperty = self.putProperty(configurations, "mapred-site",
                                             services)
        putMapredProperty('yarn.app.mapreduce.am.resource.mb',
                          int(clusterData['amMemory']))
        putMapredProperty('yarn.app.mapreduce.am.command-opts', "-Xmx" +
                          str(int(round(0.8 * clusterData['amMemory']))) + "m")
        putMapredProperty('mapreduce.map.memory.mb', clusterData['mapMemory'])
        putMapredProperty('mapreduce.reduce.memory.mb',
                          int(clusterData['reduceMemory']))
        putMapredProperty(
            'mapreduce.map.java.opts',
            "-Xmx" + str(int(round(0.8 * clusterData['mapMemory']))) + "m")
        putMapredProperty(
            'mapreduce.reduce.java.opts',
            "-Xmx" + str(int(round(0.8 * clusterData['reduceMemory']))) + "m")
        putMapredProperty('mapreduce.task.io.sort.mb', min(
            int(round(0.4 * clusterData['mapMemory'])), 1024))

        mapred_mounts = [
            ("mapred.local.dir", ["TASKTRACKER", "NODEMANAGER"],
             "/hadoop/mapred", "multi")
        ]

        mr_queue = self.recommendYarnQueue(services, "mapred-site",
                                           "mapreduce.job.queuename")
        if mr_queue is not None:
            putMapredProperty("mapreduce.job.queuename", mr_queue)

        putMapredProperty = self.putProperty(configurations, "mapred-site",
                                             services)
        nodemanagerMinRam = 1048576  # 1TB in mb
        if "referenceNodeManagerHost" in clusterData:
            nodemanagerMinRam = min(
                clusterData["referenceNodeManagerHost"]["total_mem"] / 1024,
                nodemanagerMinRam)
        putMapredProperty('yarn.app.mapreduce.am.command-opts', "-Xmx" + str(
            int(0.8 * int(configurations["mapred-site"]["properties"][
                              "yarn.app.mapreduce.am.resource.mb"]))) + "m")
        servicesList = [service["StackServices"]["service_name"]
                        for service in services["services"]]
        min_mapreduce_map_memory_mb = 0
        min_mapreduce_reduce_memory_mb = 0
        min_mapreduce_map_java_opts = 0

        mapredMapXmx = int(0.8 * int(configurations["mapred-site"][
                                         "properties"]["mapreduce.map.memory.mb"]))
        putMapredProperty(
            'mapreduce.map.java.opts',
            "-Xmx" + str(max(min_mapreduce_map_java_opts, mapredMapXmx)) + "m")
        putMapredProperty('mapreduce.reduce.java.opts', "-Xmx" + str(
            int(0.8 * int(configurations["mapred-site"]["properties"][
                              "mapreduce.reduce.memory.mb"]))) + "m")
        putMapredProperty('mapreduce.task.io.sort.mb',
                          str(min(int(0.7 * mapredMapXmx), 2047)))

    def recommendZookeeperConfigurations(self, configurations, clusterData,
                                         services, hosts):
        zk_mount_properties = [
            ("dataDir", "ZOOKEEPER_SERVER", "/hadoop/zookeeper", "single"),
        ]
        self.updateMountProperties("zoo.cfg", zk_mount_properties,
                                   configurations, services, hosts)

    def recommendYARNConfigurations(self, configurations, clusterData,
                                    services, hosts):
        putYarnSiteProperty = self.putProperty(configurations, "yarn-site",
                                               services)

        putYarnProperty = self.putProperty(configurations, "yarn-site",
                                           services)
        putYarnPropertyAttribute = self.putPropertyAttribute(configurations,
                                                             "yarn-site")
        putYarnEnvProperty = self.putProperty(configurations, "yarn-env",
                                              services)
        nodemanagerMinRam = 1048576  # 1TB in mb
        if "referenceNodeManagerHost" in clusterData:
            nodemanagerMinRam = min(
                clusterData["referenceNodeManagerHost"]["total_mem"] / 1024,
                nodemanagerMinRam)

        callContext = self.getCallContext(services)
        putYarnProperty('yarn.nodemanager.resource.memory-mb', int(
            round(
                min(clusterData['containers'] * clusterData['ramPerContainer'],
                    nodemanagerMinRam))))
        if 'recommendConfigurations' == callContext:
            putYarnProperty('yarn.nodemanager.resource.memory-mb', int(
                round(
                    min(clusterData['containers'] * clusterData[
                        'ramPerContainer'], nodemanagerMinRam))))
        else:
            # read from the supplied config
            if "yarn-site" in services[
                "configurations"] and "yarn.nodemanager.resource.memory-mb" in services[
                "configurations"]["yarn-site"]["properties"]:
                putYarnProperty(
                    'yarn.nodemanager.resource.memory-mb',
                    int(services["configurations"]["yarn-site"]["properties"][
                            "yarn.nodemanager.resource.memory-mb"]))
            else:
                putYarnProperty('yarn.nodemanager.resource.memory-mb', int(
                    round(
                        min(clusterData['containers'] * clusterData[
                            'ramPerContainer'], nodemanagerMinRam))))
            pass
        pass

        putYarnProperty('yarn.scheduler.maximum-allocation-mb',
                        int(configurations["yarn-site"]["properties"][
                                "yarn.nodemanager.resource.memory-mb"]))
        putYarnEnvProperty('min_user_id', self.get_system_min_uid())

        yarn_mount_properties = [
            ("yarn.nodemanager.local-dirs", "NODEMANAGER",
             "/hadoop/yarn/local", "multi"),
            ("yarn.nodemanager.log-dirs", "NODEMANAGER", "/hadoop/yarn/log",
             "multi"),
            ("yarn.timeline-service.leveldb-timeline-store.path",
             "APP_TIMELINE_SERVER", "/hadoop/yarn/timeline", "single"),
            ("yarn.timeline-service.leveldb-state-store.path",
             "APP_TIMELINE_SERVER", "/hadoop/yarn/timeline", "single")
        ]

        self.updateMountProperties("yarn-site", yarn_mount_properties,
                                   configurations, services, hosts)

        sc_queue_name = self.recommendYarnQueue(services, "yarn-env",
                                                "service_check.queue.name")
        if sc_queue_name is not None:
            putYarnEnvProperty("service_check.queue.name", sc_queue_name)

        containerExecutorGroup = 'hadoop'
        if 'cluster-env' in services[
            'configurations'] and 'user_group' in services[
            'configurations']['cluster-env']['properties']:
            containerExecutorGroup = services['configurations']['cluster-env'][
                'properties']['user_group']
        putYarnProperty("yarn.nodemanager.linux-container-executor.group",
                        containerExecutorGroup)

        putYarnProperty = self.putProperty(configurations, "yarn-site",
                                           services)
        putYarnProperty('yarn.nodemanager.resource.cpu-vcores',
                        clusterData['cpu'])
        putYarnProperty('yarn.scheduler.minimum-allocation-vcores', 1)
        putYarnProperty('yarn.scheduler.maximum-allocation-vcores',
                        configurations["yarn-site"]["properties"][
                            "yarn.nodemanager.resource.cpu-vcores"])
        # Property Attributes
        putYarnPropertyAttribute = self.putPropertyAttribute(configurations,
                                                             "yarn-site")
        nodeManagerHost = self.getHostWithComponent("YARN", "NODEMANAGER",
                                                    services, hosts)
        if (nodeManagerHost is not None):
            cpuPercentageLimit = 80.0
            if "yarn-site" in services[
                "configurations"] and "yarn.nodemanager.resource.percentage-physical-cpu-limit" in services[
                "configurations"]["yarn-site"]["properties"]:
                cpuPercentageLimit = float(
                    services["configurations"]["yarn-site"]["properties"]
                    ["yarn.nodemanager.resource.percentage-physical-cpu-limit"])
            cpuLimit = max(1, int(
                floor(nodeManagerHost["Hosts"]["cpu_count"] * (
                    cpuPercentageLimit / 100.0))))
            putYarnProperty('yarn.nodemanager.resource.cpu-vcores',
                            str(cpuLimit))
            putYarnProperty('yarn.scheduler.maximum-allocation-vcores',
                            configurations["yarn-site"]["properties"][
                                "yarn.nodemanager.resource.cpu-vcores"])
            putYarnPropertyAttribute(
                'yarn.nodemanager.resource.memory-mb', 'maximum', int(
                    nodeManagerHost["Hosts"]["total_mem"] /
                    1024))  # total_mem in kb
            putYarnPropertyAttribute('yarn.nodemanager.resource.cpu-vcores',
                                     'maximum',
                                     nodeManagerHost["Hosts"]["cpu_count"] * 2)
            putYarnPropertyAttribute(
                'yarn.scheduler.minimum-allocation-vcores', 'maximum',
                configurations["yarn-site"]["properties"][
                    "yarn.nodemanager.resource.cpu-vcores"])
            putYarnPropertyAttribute(
                'yarn.scheduler.maximum-allocation-vcores', 'maximum',
                configurations["yarn-site"]["properties"][
                    "yarn.nodemanager.resource.cpu-vcores"])
            putYarnPropertyAttribute(
                'yarn.scheduler.minimum-allocation-mb', 'maximum',
                configurations["yarn-site"]["properties"][
                    "yarn.nodemanager.resource.memory-mb"])
            putYarnPropertyAttribute(
                'yarn.scheduler.maximum-allocation-mb', 'maximum',
                configurations["yarn-site"]["properties"][
                    "yarn.nodemanager.resource.memory-mb"])

            kerberos_authentication_enabled = self.isSecurityEnabled(services)
            if kerberos_authentication_enabled:
                putYarnProperty(
                    'yarn.nodemanager.container-executor.class',
                    'org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor')

            if "yarn-env" in services[
                "configurations"] and "yarn_cgroups_enabled" in services[
                "configurations"]["yarn-env"]["properties"]:
                yarn_cgroups_enabled = services["configurations"]["yarn-env"][
                                           "properties"]["yarn_cgroups_enabled"].lower() == "true"
                if yarn_cgroups_enabled:
                    putYarnProperty(
                        'yarn.nodemanager.container-executor.class',
                        'org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor')
                    putYarnProperty(
                        'yarn.nodemanager.linux-container-executor.group',
                        'yarn')
                    putYarnProperty(
                        'yarn.nodemanager.linux-container-executor.resources-handler.class',
                        'org.apache.hadoop.yarn.server.nodemanager.util.CgroupsLCEResourcesHandler')
                    putYarnProperty(
                        'yarn.nodemanager.linux-container-executor.cgroups.hierarchy',
                        '/yarn')
                    putYarnProperty(
                        'yarn.nodemanager.linux-container-executor.cgroups.mount',
                        'true')
                    putYarnProperty(
                        'yarn.nodemanager.linux-container-executor.cgroups.mount-path',
                        '/cgroups')
                else:
                    if not kerberos_authentication_enabled:
                        putYarnProperty(
                            'yarn.nodemanager.container-executor.class',
                            'org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor')
                    putYarnPropertyAttribute(
                        'yarn.nodemanager.linux-container-executor.resources-handler.class',
                        'delete', 'true')
                    putYarnPropertyAttribute(
                        'yarn.nodemanager.linux-container-executor.cgroups.hierarchy',
                        'delete', 'true')
                    putYarnPropertyAttribute(
                        'yarn.nodemanager.linux-container-executor.cgroups.mount',
                        'delete', 'true')
                    putYarnPropertyAttribute(
                        'yarn.nodemanager.linux-container-executor.cgroups.mount-path',
                        'delete', 'true')

        if "yarn-site" in services["configurations"] and \
                        "yarn.resourcemanager.scheduler.monitor.enable" in services["configurations"]["yarn-site"][
                    "properties"]:
            scheduler_monitor_enabled = services["configurations"][
                "yarn-site"]["properties"][
                "yarn.resourcemanager.scheduler.monitor.enable"]
            if scheduler_monitor_enabled.lower() == 'true':
                putYarnSiteProperty(
                    'yarn.scheduler.capacity.ordering-policy.priority-utilization.underutilized-preemption.enabled',
                    "true")
            else:
                putYarnSiteProperty(
                    'yarn.scheduler.capacity.ordering-policy.priority-utilization.underutilized-preemption.enabled',
                    "false")

        if "yarn-site" in services["configurations"] and \
                        "yarn.timeline-service.webapp.https.address" in services["configurations"]["yarn-site"][
                    "properties"] and \
                        "yarn.http.policy" in services["configurations"]["yarn-site"]["properties"] and \
                        "yarn.log.server.web-service.url" in services["configurations"]["yarn-site"]["properties"]:
            if services["configurations"]["yarn-site"]["properties"]["yarn.http.policy"] == 'HTTP_ONLY':
                webapp_address = services["configurations"]["yarn-site"]["properties"][
                    "yarn.timeline-service.webapp.address"]
                webservice_url = "http://" + webapp_address + "/ws/v1/applicationhistory"
            else:
                webapp_address = services["configurations"]["yarn-site"]["properties"][
                    "yarn.timeline-service.webapp.https.address"]
                webservice_url = "https://" + webapp_address + "/ws/v1/applicationhistory"
            putYarnSiteProperty('yarn.log.server.web-service.url', webservice_url)

        if 'yarn-env' in services[
            'configurations'] and 'yarn_user' in services[
            'configurations']['yarn-env']['properties']:
            yarn_user = services['configurations']['yarn-env']['properties'][
                'yarn_user']
        else:
            yarn_user = 'yarn'

        putYarnSitePropertyAttributes = self.putPropertyAttribute(configurations, "yarn-site")
        if "ranger-env" in services["configurations"] and "ranger-yarn-plugin-properties" in services[
            "configurations"] and \
                        "ranger-yarn-plugin-enabled" in services["configurations"]["ranger-env"]["properties"]:
            putYarnRangerPluginProperty = self.putProperty(configurations, "ranger-yarn-plugin-properties", services)
            rangerEnvYarnPluginProperty = services["configurations"]["ranger-env"]["properties"][
                "ranger-yarn-plugin-enabled"]
            putYarnRangerPluginProperty("ranger-yarn-plugin-enabled", rangerEnvYarnPluginProperty)
        rangerPluginEnabled = ''
        if 'ranger-yarn-plugin-properties' in configurations and 'ranger-yarn-plugin-enabled' in \
                configurations['ranger-yarn-plugin-properties']['properties']:
            rangerPluginEnabled = configurations['ranger-yarn-plugin-properties']['properties'][
                'ranger-yarn-plugin-enabled']
        elif 'ranger-yarn-plugin-properties' in services['configurations'] and 'ranger-yarn-plugin-enabled' in \
                services['configurations']['ranger-yarn-plugin-properties']['properties']:
            rangerPluginEnabled = services['configurations']['ranger-yarn-plugin-properties']['properties'][
                'ranger-yarn-plugin-enabled']

        if rangerPluginEnabled and (rangerPluginEnabled.lower() == 'Yes'.lower()):
            putYarnSiteProperty('yarn.acl.enable', 'true')
            putYarnSiteProperty('yarn.authorization-provider',
                                'org.apache.ranger.authorization.yarn.authorizer.RangerYarnAuthorizer')
        else:
            putYarnSitePropertyAttributes('yarn.authorization-provider', 'delete', 'true')

        # calculate total_preemption_per_round
        total_preemption_per_round = str(round(max(float(1)/len(hosts['items']), 0.1),2))
        putYarnSiteProperty('yarn.resourcemanager.monitor.capacity.preemption.total_preemption_per_round', total_preemption_per_round)

        yarn_timeline_app_cache_size = None
        host_mem = None
        for host in hosts["items"]:
            host_mem = host["Hosts"]["total_mem"]
            break
        # Check if 'yarn.timeline-service.entity-group-fs-store.app-cache-size' in changed configs.
        changed_configs_has_ats_cache_size = self.isConfigPropertiesChanged(
            services, "yarn-site", ['yarn.timeline-service.entity-group-fs-store.app-cache-size'], False)
        # Check if it's : 1. 'apptimelineserver_heapsize' changed detected in changed-configurations)
        # OR 2. cluster initialization (services['changed-configurations'] should be empty in this case)
        if changed_configs_has_ats_cache_size:
            yarn_timeline_app_cache_size = self.read_yarn_apptimelineserver_cache_size(services)
        elif 0 == len(services['changed-configurations']):
            # Fetch host memory from 1st host, to be used for ATS config calculations below.
            if host_mem is not None:
                yarn_timeline_app_cache_size = self.calculate_yarn_apptimelineserver_cache_size(host_mem)
                putYarnSiteProperty('yarn.timeline-service.entity-group-fs-store.app-cache-size', yarn_timeline_app_cache_size)
                self.logger.info("Updated YARN config 'yarn.timeline-service.entity-group-fs-store.app-cache-size' as : {0}, "
                                 "using 'host_mem' = {1}".format(yarn_timeline_app_cache_size, host_mem))
            else:
                self.logger.info("Couldn't update YARN config 'yarn.timeline-service.entity-group-fs-store.app-cache-size' as "
                                 "'host_mem' read = {0}".format(host_mem))

        if yarn_timeline_app_cache_size is not None:
            # Calculation for 'ats_heapsize' is in MB.
            ats_heapsize = self.calculate_yarn_apptimelineserver_heapsize(host_mem, yarn_timeline_app_cache_size)
            putYarnEnvProperty('apptimelineserver_heapsize', ats_heapsize) # Value in MB
            self.logger.info("Updated YARN config 'apptimelineserver_heapsize' as : {0}, ".format(ats_heapsize))

        hsi_env_poperties = self.getServicesSiteProperties(services, "hive-interactive-env")
        cluster_env = self.getServicesSiteProperties(services, "cluster-env")

        # Queue 'llap' creation/removal logic (Used by Hive Interactive server and associated LLAP)
        if hsi_env_poperties and 'enable_hive_interactive' in hsi_env_poperties:
            enable_hive_interactive = hsi_env_poperties['enable_hive_interactive']
            LLAP_QUEUE_NAME = 'llap'

            # Hive Server interactive is already added or getting added
            if enable_hive_interactive == 'true':
                self.updateLlapConfigs(configurations, services, hosts, LLAP_QUEUE_NAME)
            else:  # When Hive Interactive Server is in 'off/removed' state.
                self.checkAndStopLlapQueue(services, configurations, LLAP_QUEUE_NAME)

        putYarnSiteProperty = self.putProperty(configurations, "yarn-site", services)
        timeline_plugin_classes_values = []
        timeline_plugin_classpath_values = []

        if self.isServiceDeployed(services, "TEZ"):
            timeline_plugin_classes_values.append('org.apache.tez.dag.history.logging.ats.TimelineCachePluginImpl')

        putYarnSiteProperty('yarn.timeline-service.entity-group-fs-store.group-id-plugin-classes', ",".join(timeline_plugin_classes_values))

    def getMetadataConnectionString(self, database_type):
        driverDict = {
            'mysql': 'jdbc:mysql://{0}:{2}/{1}?createDatabaseIfNotExist=true&useSSL=false',
            'derby': 'jdbc:derby://{0}:{2}/{1};create=true',
            'postgresql': 'jdbc:postgresql://{0}:{2}/{1}'
        }
        return driverDict.get(database_type.lower())

    def addToList(self, json_list, word):
        desr_list = json.loads(json_list)
        if word not in desr_list:
            desr_list.append(word)
        return json.dumps(desr_list)

    def removeFromList(self, json_list, word):
        desr_list = json.loads(json_list)
        if word in desr_list:
            desr_list.remove(word)
        return json.dumps(desr_list)

    def getMinMemory(self, component_hosts):
        min_ram_kb = 1073741824  # 1 TB
        for host in component_hosts:
            ram_kb = host['Hosts']['total_mem']
            min_ram_kb = min(min_ram_kb, ram_kb)
        return min_ram_kb

    def getMinCpu(self, component_hosts):
        min_cpu = 256
        for host in component_hosts:
            cpu_count = host['Hosts']['cpu_count']
            min_cpu = min(min_cpu, cpu_count)
        return min_cpu

    def getServiceConfigurationValidators(self):
        return {}

    def recommendHDFSConfigurations(self, configurations, clusterData,
                                    services, hosts):
        putHDFSProperty = self.putProperty(configurations, "hadoop-env",
                                           services)
        putHDFSSiteProperty = self.putProperty(configurations, "hdfs-site",
                                               services)
        putHDFSSitePropertyAttributes = self.putPropertyAttribute(
            configurations, "hdfs-site")
        putHDFSProperty('namenode_heapsize', max(
            int(clusterData['totalAvailableRam'] / 2), 1024))
        putHDFSProperty = self.putProperty(configurations, "hadoop-env",
                                           services)
        putHDFSProperty('namenode_opt_newsize', max(
            int(clusterData['totalAvailableRam'] / 8), 128))
        putHDFSProperty = self.putProperty(configurations, "hadoop-env",
                                           services)
        putHDFSProperty('namenode_opt_maxnewsize', max(
            int(clusterData['totalAvailableRam'] / 8), 256))

        # Check if NN HA is enabled and recommend removing dfs.namenode.rpc-address
        hdfsSiteProperties = getServicesSiteProperties(services, "hdfs-site")
        nameServices = None
        if hdfsSiteProperties and 'dfs.internal.nameservices' in hdfsSiteProperties:
            nameServices = hdfsSiteProperties['dfs.internal.nameservices']
        if nameServices is None and hdfsSiteProperties and 'dfs.nameservices' in hdfsSiteProperties:
            nameServices = hdfsSiteProperties['dfs.nameservices']
        if nameServices and "dfs.ha.namenodes.%s" % nameServices in hdfsSiteProperties:
            namenodes = hdfsSiteProperties["dfs.ha.namenodes.%s" %
                                           nameServices]
            if len(namenodes.split(',')) > 1:
                putHDFSSitePropertyAttributes("dfs.namenode.rpc-address",
                                              "delete", "true")

        hdfs_mount_properties = [
            ("dfs.datanode.data.dir", "DATANODE", "/hadoop/hdfs/data",
             "multi"), ("dfs.namenode.name.dir", "DATANODE",
                        "/hadoop/hdfs/namenode", "multi"),
            ("dfs.namenode.checkpoint.dir", "SECONDARY_NAMENODE",
             "/hadoop/hdfs/namesecondary", "single")
        ]

        self.updateMountProperties("hdfs-site", hdfs_mount_properties,
                                   configurations, services, hosts)

        if configurations and "hdfs-site" in configurations and \
                        "dfs.datanode.data.dir" in configurations["hdfs-site"]["properties"] and \
                        configurations["hdfs-site"]["properties"]["dfs.datanode.data.dir"] is not None:
            dataDirs = configurations["hdfs-site"]["properties"][
                "dfs.datanode.data.dir"].split(",")

        elif hdfsSiteProperties and "dfs.datanode.data.dir" in hdfsSiteProperties and \
                        hdfsSiteProperties["dfs.datanode.data.dir"] is not None:
            dataDirs = hdfsSiteProperties["dfs.datanode.data.dir"].split(",")

        # dfs.datanode.du.reserved should be set to 10-15% of volume size
        # For each host selects maximum size of the volume. Then gets minimum for all hosts.
        # This ensures that each host will have at least one data dir with available space.
        reservedSizeRecommendation = 0l  # kBytes
        for host in hosts["items"]:
            mountPoints = []
            mountPointDiskAvailableSpace = []  # kBytes
            for diskInfo in host["Hosts"]["disk_info"]:
                mountPoints.append(diskInfo["mountpoint"])
                mountPointDiskAvailableSpace.append(long(diskInfo["size"]))

            maxFreeVolumeSizeForHost = 0l  # kBytes
            for dataDir in dataDirs:
                mp = self.getMountPointForDir(dataDir, mountPoints)
                for i in range(len(mountPoints)):
                    if mp == mountPoints[i]:
                        if mountPointDiskAvailableSpace[
                            i] > maxFreeVolumeSizeForHost:
                            maxFreeVolumeSizeForHost = mountPointDiskAvailableSpace[
                                i]

            if not reservedSizeRecommendation or maxFreeVolumeSizeForHost and maxFreeVolumeSizeForHost < reservedSizeRecommendation:
                reservedSizeRecommendation = maxFreeVolumeSizeForHost

        if reservedSizeRecommendation:
            reservedSizeRecommendation = max(
                reservedSizeRecommendation * 1024 / 8,
                1073741824)  # At least 1Gb is reserved
            putHDFSSiteProperty('dfs.datanode.du.reserved',
                                reservedSizeRecommendation)  # Bytes

        # recommendations for "hadoop.proxyuser.*.hosts", "hadoop.proxyuser.*.groups" properties in core-site
        self.recommendHadoopProxyUsers(configurations, services, hosts)

        putHdfsSiteProperty = self.putProperty(configurations, "hdfs-site",
                                               services)
        putHdfsSitePropertyAttribute = self.putPropertyAttribute(
            configurations, "hdfs-site")
        putHdfsSiteProperty("dfs.datanode.max.transfer.threads", 16384
        if clusterData["hBaseInstalled"] else 4096)

        dataDirsCount = 1
        # Use users 'dfs.datanode.data.dir' first
        if "hdfs-site" in services[
            "configurations"] and "dfs.datanode.data.dir" in services[
            "configurations"]["hdfs-site"]["properties"]:
            dataDirsCount = len(
                str(services["configurations"]["hdfs-site"]["properties"][
                        "dfs.datanode.data.dir"]).split(","))
        elif "dfs.datanode.data.dir" in configurations["hdfs-site"][
            "properties"]:
            dataDirsCount = len(
                str(configurations["hdfs-site"]["properties"][
                        "dfs.datanode.data.dir"]).split(","))
        if dataDirsCount <= 2:
            failedVolumesTolerated = 0
        elif dataDirsCount <= 4:
            failedVolumesTolerated = 1
        else:
            failedVolumesTolerated = 2
        putHdfsSiteProperty("dfs.datanode.failed.volumes.tolerated",
                            failedVolumesTolerated)

        namenodeHosts = self.getHostsWithComponent("HDFS", "NAMENODE",
                                                   services, hosts)

        # 25 * # of cores on NameNode
        nameNodeCores = 4
        if namenodeHosts is not None and len(namenodeHosts):
            nameNodeCores = int(namenodeHosts[0]['Hosts']['cpu_count'])
        putHdfsSiteProperty("dfs.namenode.handler.count", 25 * nameNodeCores)
        if 25 * nameNodeCores > 200:
            putHdfsSitePropertyAttribute("dfs.namenode.handler.count",
                                         "maximum", 25 * nameNodeCores)

        servicesList = [service["StackServices"]["service_name"]
                        for service in services["services"]]
        if ('ranger-hdfs-plugin-properties' in services['configurations']
            ) and ('ranger-hdfs-plugin-enabled' in services['configurations'][
            'ranger-hdfs-plugin-properties']['properties']):
            rangerPluginEnabled = services['configurations'][
                'ranger-hdfs-plugin-properties']['properties'][
                'ranger-hdfs-plugin-enabled']
            if ("RANGER" in servicesList) and (
                        rangerPluginEnabled.lower() == 'Yes'.lower()):
                putHdfsSiteProperty("dfs.permissions.enabled", 'true')

        putHdfsSiteProperty("dfs.namenode.safemode.threshold-pct", "0.999"
        if len(namenodeHosts) > 1 else "1.000")

        putHdfsEnvProperty = self.putProperty(configurations, "hadoop-env",
                                              services)
        putHdfsEnvPropertyAttribute = self.putPropertyAttribute(configurations,
                                                                "hadoop-env")

        putHdfsEnvProperty('namenode_heapsize', max(
            int(clusterData['totalAvailableRam'] / 2), 1024))

        nn_heapsize_limit = None
        if (namenodeHosts is not None and len(namenodeHosts) > 0):
            if len(namenodeHosts) > 1:
                nn_max_heapsize = min(
                    int(namenodeHosts[0]["Hosts"]["total_mem"]),
                    int(namenodeHosts[1]["Hosts"]["total_mem"])) / 1024
                masters_at_host = max(
                    self.getHostComponentsByCategories(
                        namenodeHosts[0]["Hosts"]["host_name"], ["MASTER"],
                        services, hosts), self.getHostComponentsByCategories(
                        namenodeHosts[1]["Hosts"]["host_name"], ["MASTER"],
                        services, hosts))
            else:
                nn_max_heapsize = int(namenodeHosts[0]["Hosts"]["total_mem"] /
                                      1024)  # total_mem in kb
                masters_at_host = self.getHostComponentsByCategories(
                    namenodeHosts[0]["Hosts"]["host_name"], ["MASTER"],
                    services, hosts)

            putHdfsEnvPropertyAttribute('namenode_heapsize', 'maximum', max(
                nn_max_heapsize, 1024))

            nn_heapsize_limit = nn_max_heapsize
            nn_heapsize_limit -= clusterData["reservedRam"]
            if len(masters_at_host) > 1:
                nn_heapsize_limit = int(nn_heapsize_limit / 2)

            putHdfsEnvProperty('namenode_heapsize', max(nn_heapsize_limit,
                                                        1024))

        datanodeHosts = self.getHostsWithComponent("HDFS", "DATANODE",
                                                   services, hosts)
        if datanodeHosts is not None and len(datanodeHosts) > 0:
            min_datanode_ram_kb = 1073741824  # 1 TB
            for datanode in datanodeHosts:
                ram_kb = datanode['Hosts']['total_mem']
                min_datanode_ram_kb = min(min_datanode_ram_kb, ram_kb)

            datanodeFilesM = len(
                datanodeHosts) * dataDirsCount / 10  # in millions, # of files = # of disks * 100'000
            nn_memory_configs = [
                {'nn_heap': 1024,
                 'nn_opt': 128}, {'nn_heap': 3072,
                                  'nn_opt': 512}, {'nn_heap': 5376,
                                                   'nn_opt': 768},
                {'nn_heap': 9984,
                 'nn_opt': 1280}, {'nn_heap': 14848,
                                   'nn_opt': 2048}, {'nn_heap': 19456,
                                                     'nn_opt': 2560},
                {'nn_heap': 24320,
                 'nn_opt': 3072}, {'nn_heap': 33536,
                                   'nn_opt': 4352}, {'nn_heap': 47872,
                                                     'nn_opt': 6144},
                {'nn_heap': 59648,
                 'nn_opt': 7680}, {'nn_heap': 71424,
                                   'nn_opt': 8960}, {'nn_heap': 94976,
                                                     'nn_opt': 8960}
            ]
            index = {
                datanodeFilesM < 1: 0,
                1 <= datanodeFilesM < 5: 1,
                5 <= datanodeFilesM < 10: 2,
                10 <= datanodeFilesM < 20: 3,
                20 <= datanodeFilesM < 30: 4,
                30 <= datanodeFilesM < 40: 5,
                40 <= datanodeFilesM < 50: 6,
                50 <= datanodeFilesM < 70: 7,
                70 <= datanodeFilesM < 100: 8,
                100 <= datanodeFilesM < 125: 9,
                125 <= datanodeFilesM < 150: 10,
                150 <= datanodeFilesM: 11
            }[1]

            nn_memory_config = nn_memory_configs[index]

            # override with new values if applicable
            if nn_heapsize_limit is not None and nn_memory_config[
                'nn_heap'] <= nn_heapsize_limit:
                putHdfsEnvProperty('namenode_heapsize',
                                   nn_memory_config['nn_heap'])

            putHdfsEnvPropertyAttribute('dtnode_heapsize', 'maximum', int(
                min_datanode_ram_kb / 1024))

        nn_heapsize = int(configurations["hadoop-env"]["properties"][
                              "namenode_heapsize"])
        putHdfsEnvProperty('namenode_opt_newsize', max(
            int(nn_heapsize / 8), 128))
        putHdfsEnvProperty('namenode_opt_maxnewsize', max(
            int(nn_heapsize / 8), 128))

        putHdfsSitePropertyAttribute = self.putPropertyAttribute(
            configurations, "hdfs-site")
        putHdfsSitePropertyAttribute('dfs.datanode.failed.volumes.tolerated',
                                     'maximum', dataDirsCount)

        keyserverHostsString = None
        keyserverPortString = None
        if "hadoop-env" in services[
            "configurations"] and "keyserver_host" in services[
            "configurations"]["hadoop-env"][
            "properties"] and "keyserver_port" in services[
            "configurations"]["hadoop-env"]["properties"]:
            keyserverHostsString = services["configurations"]["hadoop-env"][
                "properties"]["keyserver_host"]
            keyserverPortString = services["configurations"]["hadoop-env"][
                "properties"]["keyserver_port"]

        if ('ranger-hdfs-plugin-properties' in services['configurations']) and (
                    'ranger-hdfs-plugin-enabled' in services['configurations']['ranger-hdfs-plugin-properties'][
                    'properties']):
            rangerPluginEnabled = ''
            if 'ranger-hdfs-plugin-properties' in configurations and 'ranger-hdfs-plugin-enabled' in \
                    configurations['ranger-hdfs-plugin-properties']['properties']:
                rangerPluginEnabled = configurations['ranger-hdfs-plugin-properties']['properties'][
                    'ranger-hdfs-plugin-enabled']
            elif 'ranger-hdfs-plugin-properties' in services['configurations'] and 'ranger-hdfs-plugin-enabled' in \
                    services['configurations']['ranger-hdfs-plugin-properties']['properties']:
                rangerPluginEnabled = services['configurations']['ranger-hdfs-plugin-properties']['properties'][
                    'ranger-hdfs-plugin-enabled']

            if rangerPluginEnabled and (rangerPluginEnabled.lower() == 'Yes'.lower()):
                putHdfsSiteProperty("dfs.namenode.inode.attributes.provider.class",
                                    'org.apache.ranger.authorization.hadoop.RangerHdfsAuthorizer')
            else:
                putHdfsSitePropertyAttribute('dfs.namenode.inode.attributes.provider.class', 'delete', 'true')
        else:
            putHdfsSitePropertyAttribute('dfs.namenode.inode.attributes.provider.class', 'delete', 'true')

        if 'ranger-hdfs-plugin-properties' in configurations and 'ranger-hdfs-plugin-enabled' in \
                configurations['ranger-hdfs-plugin-properties']['properties']:
            ranger_hdfs_plugin_enabled = (configurations['ranger-hdfs-plugin-properties']['properties'][
                                              'ranger-hdfs-plugin-enabled'].lower() == 'Yes'.lower())
        elif 'ranger-hdfs-plugin-properties' in services['configurations'] and 'ranger-hdfs-plugin-enabled' in \
                services['configurations']['ranger-hdfs-plugin-properties']['properties']:
            ranger_hdfs_plugin_enabled = (services['configurations']['ranger-hdfs-plugin-properties']['properties'][
                                              'ranger-hdfs-plugin-enabled'].lower() == 'Yes'.lower())
        else:
            ranger_hdfs_plugin_enabled = False

        if "ranger-env" in services["configurations"] and "ranger-hdfs-plugin-properties" in services[
            "configurations"] and "ranger-hdfs-plugin-enabled" in services["configurations"]["ranger-env"][
            "properties"]:
            putHdfsRangerPluginProperty = self.putProperty(configurations, "ranger-hdfs-plugin-properties", services)
            rangerEnvHdfsPluginProperty = services["configurations"]["ranger-env"]["properties"][
                "ranger-hdfs-plugin-enabled"]
            putHdfsRangerPluginProperty("ranger-hdfs-plugin-enabled", rangerEnvHdfsPluginProperty)
        putHdfsSitePropertyAttribute = self.putPropertyAttribute(configurations, "hdfs-site")
        putCoreSitePropertyAttribute = self.putPropertyAttribute(configurations, "core-site")
        if not "RANGER_KMS" in servicesList:
            putCoreSitePropertyAttribute('hadoop.security.key.provider.path', 'delete', 'true')
            putHdfsSitePropertyAttribute('dfs.encryption.key.provider.uri', 'delete', 'true')
        if 'hadoop-env' in services['configurations'] and 'hdfs_user' in services['configurations']['hadoop-env'][
            'properties']:
            hdfs_user = services['configurations']['hadoop-env']['properties']['hdfs_user']
        else:
            hdfs_user = 'hadoop'
        if ranger_hdfs_plugin_enabled and 'ranger-hdfs-plugin-properties' in services[
            'configurations']:
            Logger.info("Setting HDFS Repo user for Ranger.")
            putHdfsSiteProperty("dfs.namenode.inode.attributes.provider.class",
                                'org.apache.ranger.authorization.hadoop.RangerHdfsAuthorizer')
            putRangerHDFSPluginProperty = self.putProperty(configurations, "ranger-hdfs-plugin-properties", services)
        else:
            Logger.info("Not setting HDFS Repo user for Ranger.")

    def recommendHIVEConfigurations(self, configurations, clusterData,
                                    services, hosts):
        putHiveServerProperty = self.putProperty(configurations,
                                                 "hiveserver2-site", services)
        putHiveEnvProperty = self.putProperty(configurations, "hive-env",
                                              services)
        putHiveSiteProperty = self.putProperty(configurations, "hive-site",
                                               services)
        putWebhcatSiteProperty = self.putProperty(configurations,
                                                  "webhcat-site", services)
        putHiveSitePropertyAttribute = self.putPropertyAttribute(
            configurations, "hive-site")
        putHiveEnvPropertyAttributes = self.putPropertyAttribute(
            configurations, "hive-env")
        putHiveServerPropertyAttributes = self.putPropertyAttribute(
            configurations, "hiveserver2-site")
        servicesList = [service["StackServices"]["service_name"]
                        for service in services["services"]]

        security_enabled = self.isSecurityEnabled(services)

        #  Storage
        putHiveEnvProperty("hive_exec_orc_storage_strategy", "SPEED")
        putHiveSiteProperty("hive.exec.orc.encoding.strategy", configurations[
            "hive-env"]["properties"]["hive_exec_orc_storage_strategy"])
        putHiveSiteProperty("hive.exec.orc.compression.strategy",
                            configurations["hive-env"]["properties"][
                                "hive_exec_orc_storage_strategy"])

        putHiveSiteProperty("hive.exec.orc.default.stripe.size", "67108864")
        putHiveSiteProperty("hive.exec.orc.default.compress", "ZLIB")
        putHiveSiteProperty("hive.optimize.index.filter", "true")
        putHiveSiteProperty("hive.optimize.sort.dynamic.partition", "false")

        # Vectorization
        putHiveSiteProperty("hive.vectorized.execution.enabled", "true")
        putHiveSiteProperty("hive.vectorized.execution.reduce.enabled",
                            "false")

        # Transactions
        putHiveEnvProperty("hive_txn_acid", "off")
        if str(configurations["hive-env"]["properties"][
                   "hive_txn_acid"]).lower() == "on":
            putHiveSiteProperty(
                "hive.txn.manager",
                "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager")
            putHiveSiteProperty("hive.support.concurrency", "true")
            putHiveSiteProperty("hive.compactor.initiator.on", "true")
            putHiveSiteProperty("hive.compactor.worker.threads", "1")
            putHiveSiteProperty("hive.exec.dynamic.partition.mode",
                                "nonstrict")
        else:
            putHiveSiteProperty(
                "hive.txn.manager",
                "org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager")
            putHiveSiteProperty("hive.support.concurrency", "false")
            putHiveSiteProperty("hive.compactor.initiator.on", "false")
            putHiveSiteProperty("hive.compactor.worker.threads", "0")
            putHiveSiteProperty("hive.exec.dynamic.partition.mode", "strict")

        hiveMetastoreHost = self.getHostWithComponent("HIVE", "HIVE_METASTORE",
                                                      services, hosts)
        if hiveMetastoreHost is not None and len(hiveMetastoreHost) > 0:
            putHiveSiteProperty(
                "hive.metastore.uris", "thrift://" +
                                       hiveMetastoreHost["Hosts"]["host_name"] + ":9083")

        # ATS
        putHiveEnvProperty("hive_timeline_logging_enabled", "true")

        hooks_properties = ["hive.exec.pre.hooks", "hive.exec.post.hooks",
                            "hive.exec.failure.hooks"]
        include_ats_hook = str(configurations["hive-env"]["properties"][
                                   "hive_timeline_logging_enabled"]).lower() == "true"

        ats_hook_class = "org.apache.hadoop.hive.ql.hooks.ATSHook"
        for hooks_property in hooks_properties:
            if hooks_property in configurations["hive-site"]["properties"]:
                hooks_value = configurations["hive-site"]["properties"][
                    hooks_property]
            else:
                hooks_value = " "
            if include_ats_hook and ats_hook_class not in hooks_value:
                if hooks_value == " ":
                    hooks_value = ats_hook_class
                else:
                    hooks_value = hooks_value + "," + ats_hook_class
            if not include_ats_hook and ats_hook_class in hooks_value:
                hooks_classes = []
                for hook_class in hooks_value.split(","):
                    if hook_class != ats_hook_class and hook_class != " ":
                        hooks_classes.append(hook_class)
                if hooks_classes:
                    hooks_value = ",".join(hooks_classes)
                else:
                    hooks_value = " "

            putHiveSiteProperty(hooks_property, hooks_value)
            # Tez Engine
        if "TEZ" in servicesList:
            putHiveSiteProperty("hive.execution.engine", "tez")
        else:
            putHiveSiteProperty("hive.execution.engine", "mr")

        container_size = "512"

        if not "yarn-site" in configurations:
            self.recommendYARNConfigurations(configurations, clusterData,
                                             services, hosts)
            # duplicate tez task resource calc logic, direct dependency doesn't look good here (in case of Hive without Tez)
            container_size = clusterData['mapMemory'] if clusterData[
                                                             'mapMemory'] > 2048 else int(clusterData['reduceMemory'])
            putHiveSiteProperty("hive.tez.container.size", min(
                int(configurations["yarn-site"]["properties"][
                        "yarn.scheduler.maximum-allocation-mb"]), container_size))

            putHiveSitePropertyAttribute(
                "hive.tez.container.size", "maximum",
                int(configurations["yarn-site"]["properties"][
                        "yarn.scheduler.maximum-allocation-mb"]))

        if "yarn-site" in services["configurations"]:
            if "yarn.scheduler.minimum-allocation-mb" in services[
                "configurations"]["yarn-site"]["properties"]:
                putHiveSitePropertyAttribute(
                    "hive.tez.container.size", "minimum",
                    int(services["configurations"]["yarn-site"]["properties"][
                            "yarn.scheduler.minimum-allocation-mb"]))
            if "yarn.scheduler.maximum-allocation-mb" in services[
                "configurations"]["yarn-site"]["properties"]:
                putHiveSitePropertyAttribute(
                    "hive.tez.container.size", "maximum",
                    int(services["configurations"]["yarn-site"]["properties"][
                            "yarn.scheduler.maximum-allocation-mb"]))

        putHiveSiteProperty("hive.prewarm.enabled", "false")
        putHiveSiteProperty("hive.prewarm.numcontainers", "3")
        putHiveSiteProperty("hive.tez.auto.reducer.parallelism", "true")
        putHiveSiteProperty("hive.tez.dynamic.partition.pruning", "true")

        container_size = configurations["hive-site"]["properties"][
            "hive.tez.container.size"]
        container_size_bytes = int(int(container_size) * 0.8 * 1024 *
                                   1024)  # Xmx == 80% of container
        # Memory
        putHiveSiteProperty("hive.auto.convert.join.noconditionaltask.size",
                            int(round(container_size_bytes / 3)))
        putHiveSitePropertyAttribute(
            "hive.auto.convert.join.noconditionaltask.size", "maximum",
            container_size_bytes)
        putHiveSiteProperty("hive.exec.reducers.bytes.per.reducer", "67108864")

        # CBO
        if "hive-site" in services[
            "configurations"] and "hive.cbo.enable" in services[
            "configurations"]["hive-site"]["properties"]:
            hive_cbo_enable = services["configurations"]["hive-site"][
                "properties"]["hive.cbo.enable"]
            putHiveSiteProperty("hive.stats.fetch.partition.stats",
                                hive_cbo_enable)
            putHiveSiteProperty("hive.stats.fetch.column.stats",
                                hive_cbo_enable)

        putHiveSiteProperty("hive.compute.query.using.stats", "true")

        # Interactive Query
        putHiveSiteProperty("hive.server2.tez.initialize.default.sessions",
                            "false")
        putHiveSiteProperty("hive.server2.tez.sessions.per.default.queue", "1")
        putHiveSiteProperty("hive.server2.enable.doAs", "true")

        yarn_queues = "default"
        capacitySchedulerProperties = {}
        if "capacity-scheduler" in services['configurations']:
            if "capacity-scheduler" in services['configurations'][
                "capacity-scheduler"]["properties"]:
                properties = str(services['configurations'][
                                     "capacity-scheduler"]["properties"][
                                     "capacity-scheduler"]).split('\n')
                for property in properties:
                    key, sep, value = property.partition("=")
                    capacitySchedulerProperties[key] = value
            if "yarn.scheduler.capacity.root.queues" in capacitySchedulerProperties:
                yarn_queues = str(capacitySchedulerProperties[
                                      "yarn.scheduler.capacity.root.queues"])
            elif "yarn.scheduler.capacity.root.queues" in services[
                'configurations']["capacity-scheduler"]["properties"]:
                yarn_queues = services['configurations']["capacity-scheduler"][
                    "properties"]["yarn.scheduler.capacity.root.queues"]
        # Interactive Queues property attributes
        putHiveServerPropertyAttribute = self.putPropertyAttribute(
            configurations, "hiveserver2-site")
        toProcessQueues = yarn_queues.split(",")
        leafQueueNames = set()  # Remove duplicates
        while len(toProcessQueues) > 0:
            queue = toProcessQueues.pop()
            queueKey = "yarn.scheduler.capacity.root." + queue + ".queues"
            if queueKey in capacitySchedulerProperties:
                # This is a parent queue - need to add children
                subQueues = capacitySchedulerProperties[queueKey].split(",")
                for subQueue in subQueues:
                    toProcessQueues.append(queue + "." + subQueue)
            else:
                # This is a leaf queue
                queueName = queue.split(".")[
                    -1]  # Fully qualified queue name does not work, we should use only leaf name
                leafQueueNames.add(queueName)
        leafQueues = [{"label": str(queueName) + " queue",
                       "value": queueName} for queueName in leafQueueNames]
        leafQueues = sorted(leafQueues, key=lambda q: q['value'])
        putHiveSitePropertyAttribute("hive.server2.tez.default.queues",
                                     "entries", leafQueues)
        putHiveSiteProperty("hive.server2.tez.default.queues", ",".join(
            [leafQueue['value'] for leafQueue in leafQueues]))

        webhcat_queue = self.recommendYarnQueue(services, "webhcat-site", "templeton.hadoop.queue.name")
        if webhcat_queue is not None:
            putWebhcatSiteProperty("templeton.hadoop.queue.name", webhcat_queue)

        # Security
        if ("configurations" not in services) or ("hive-env" not in services["configurations"]) or (
                    "properties" not in services["configurations"]["hive-env"]) or (
                    "hive_security_authorization" not in services["configurations"]["hive-env"]["properties"]) or str(
            services["configurations"]["hive-env"]["properties"]["hive_security_authorization"]).lower() == "none":
            putHiveEnvProperty("hive_security_authorization", "None")
        else:
            putHiveEnvProperty("hive_security_authorization",
                               services["configurations"]["hive-env"]["properties"]["hive_security_authorization"])

        # Recommend Ranger Hive authorization as per Ranger Hive plugin property
        if "ranger-env" in services["configurations"] and "hive-env" in services[
            "configurations"] and "ranger-hive-plugin-enabled" in services["configurations"]["ranger-env"][
            "properties"]:
            rangerEnvHivePluginProperty = services["configurations"]["ranger-env"]["properties"][
                "ranger-hive-plugin-enabled"]
            rangerEnvHiveAuthProperty = services["configurations"]["hive-env"]["properties"][
                "hive_security_authorization"]
            if (rangerEnvHivePluginProperty.lower() == "yes"):
                putHiveEnvProperty("hive_security_authorization", "Ranger")
            elif (rangerEnvHiveAuthProperty.lower() == "ranger"):
                putHiveEnvProperty("hive_security_authorization", "None")

        # hive_security_authorization == 'none'
        # this property is unrelated to Kerberos
        if str(configurations["hive-env"]["properties"]["hive_security_authorization"]).lower() == "none":
            putHiveSiteProperty("hive.security.authorization.manager",
                                "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdConfOnlyAuthorizerFactory")
            if ("hive.security.authorization.manager" in configurations["hiveserver2-site"]["properties"]) or \
                    ("hiveserver2-site" not in services["configurations"]) or \
                    ("hiveserver2-site" in services["configurations"] and "hive.security.authorization.manager" in
                        services["configurations"]["hiveserver2-site"]["properties"]):
                putHiveServerPropertyAttribute("hive.security.authorization.manager", "delete", "true")
            if ("hive.security.authenticator.manager" in configurations["hiveserver2-site"]["properties"]) or \
                    ("hiveserver2-site" not in services["configurations"]) or \
                    ("hiveserver2-site" in services["configurations"] and "hive.security.authenticator.manager" in
                        services["configurations"]["hiveserver2-site"]["properties"]):
                putHiveServerPropertyAttribute("hive.security.authenticator.manager", "delete", "true")
            if ("hive.conf.restricted.list" in configurations["hiveserver2-site"]["properties"]) or \
                    ("hiveserver2-site" not in services["configurations"]) or \
                    ("hiveserver2-site" in services["configurations"] and "hive.conf.restricted.list" in
                        services["configurations"]["hiveserver2-site"]["properties"]):
                putHiveServerPropertyAttribute("hive.conf.restricted.list", "delete", "true")
            if "KERBEROS" not in servicesList:  # Kerberos security depends on this property
                putHiveSiteProperty("hive.security.authorization.enabled", "false")
        else:
            putHiveSiteProperty("hive.security.authorization.enabled", "true")

        try:
            auth_manager_value = str(
                configurations["hive-env"]["properties"]["hive.security.metastore.authorization.manager"])
        except KeyError:
            auth_manager_value = 'org.apache.hadoop.hive.ql.security.authorization.StorageBasedAuthorizationProvider'
            pass
        auth_manager_values = auth_manager_value.split(",")
        sqlstdauth_class = "org.apache.hadoop.hive.ql.security.authorization.MetaStoreAuthzAPIAuthorizerEmbedOnly"

        putHiveSiteProperty("hive.server2.enable.doAs", "true")

        putHiveSiteProperty("hive.security.metastore.authorization.manager", ",".join(auth_manager_values))
        # hive_security_authorization == 'ranger'
        if str(configurations["hive-env"]["properties"]["hive_security_authorization"]).lower() == "ranger":
            putHiveSiteProperty("hive.server2.enable.doAs", "false")
            putHiveServerProperty("hive.security.authorization.enabled", "true")
            putHiveServerProperty("hive.security.authorization.manager",
                                  "org.apache.ranger.authorization.hive.authorizer.RangerHiveAuthorizerFactory")
            putHiveServerProperty("hive.security.authenticator.manager",
                                  "org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator")
            putHiveServerProperty("hive.conf.restricted.list",
                                  "hive.security.authenticator.manager,hive.security.authorization.manager,hive.security.metastore.authorization.manager,"
                                  "hive.security.metastore.authenticator.manager,hive.users.in.admin.role,hive.server2.xsrf.filter.enabled,hive.security.authorization.enabled")

        # hive_security_authorization == 'None'
        if str(configurations["hive-env"]["properties"]["hive_security_authorization"]).lower() == "None":
            putHiveSiteProperty("hive.server2.enable.doAs", "true")
            putHiveServerProperty("hive.security.authorization.enabled", "false")
            putHiveServerPropertyAttributes("hive.security.authorization.manager", 'delete', 'true')
            putHiveServerPropertyAttributes("hive.security.authenticator.manager", 'delete', 'true')
            putHiveServerPropertyAttributes("hive.conf.restricted.list", 'delete', 'true')

        putHiveSiteProperty("hive.server2.use.SSL", "false")

        # Hive authentication
        hive_server2_auth = None
        if "hive-site" in services["configurations"] and "hive.server2.authentication" in \
                services["configurations"]["hive-site"]["properties"]:
            hive_server2_auth = str(
                services["configurations"]["hive-site"]["properties"]["hive.server2.authentication"]).lower()
        elif "hive.server2.authentication" in configurations["hive-site"]["properties"]:
            hive_server2_auth = str(configurations["hive-site"]["properties"]["hive.server2.authentication"]).lower()

        if hive_server2_auth == "ldap":
            putHiveSiteProperty("hive.server2.authentication.ldap.url", "")
        else:
            if ("hive.server2.authentication.ldap.url" in configurations["hive-site"]["properties"]) or \
                    ("hive-site" not in services["configurations"]) or \
                    ("hive-site" in services["configurations"] and "hive.server2.authentication.ldap.url" in
                        services["configurations"]["hive-site"]["properties"]):
                putHiveSitePropertyAttribute("hive.server2.authentication.ldap.url", "delete", "true")

        if hive_server2_auth == "kerberos":
            if "hive-site" in services["configurations"] and "hive.server2.authentication.kerberos.keytab" not in \
                    services["configurations"]["hive-site"]["properties"]:
                putHiveSiteProperty("hive.server2.authentication.kerberos.keytab", "")
            if "hive-site" in services["configurations"] and "hive.server2.authentication.kerberos.principal" not in \
                    services["configurations"]["hive-site"]["properties"]:
                putHiveSiteProperty("hive.server2.authentication.kerberos.principal", "")
        elif "KERBEROS" not in servicesList:  # Since 'hive_server2_auth' cannot be relied on within the default, empty recommendations request
            if ("hive.server2.authentication.kerberos.keytab" in configurations["hive-site"]["properties"]) or \
                    ("hive-site" not in services["configurations"]) or \
                    ("hive-site" in services["configurations"] and "hive.server2.authentication.kerberos.keytab" in
                        services["configurations"]["hive-site"]["properties"]):
                putHiveSitePropertyAttribute("hive.server2.authentication.kerberos.keytab", "delete", "true")
            if ("hive.server2.authentication.kerberos.principal" in configurations["hive-site"]["properties"]) or \
                    ("hive-site" not in services["configurations"]) or \
                    ("hive-site" in services["configurations"] and "hive.server2.authentication.kerberos.principal" in
                        services["configurations"]["hive-site"]["properties"]):
                putHiveSitePropertyAttribute("hive.server2.authentication.kerberos.principal", "delete", "true")

        if hive_server2_auth == "pam":
            putHiveSiteProperty("hive.server2.authentication.pam.services", "")
        else:
            if ("hive.server2.authentication.pam.services" in configurations["hive-site"]["properties"]) or \
                    ("hive-site" not in services["configurations"]) or \
                    ("hive-site" in services["configurations"] and "hive.server2.authentication.pam.services" in
                        services["configurations"]["hive-site"]["properties"]):
                putHiveSitePropertyAttribute("hive.server2.authentication.pam.services", "delete", "true")

        if hive_server2_auth == "custom":
            putHiveSiteProperty("hive.server2.custom.authentication.class", "")
        else:
            if ("hive.server2.authentication" in configurations["hive-site"]["properties"]) or \
                    ("hive-site" not in services["configurations"]) or \
                    ("hive-site" in services["configurations"] and "hive.server2.custom.authentication.class" in
                        services["configurations"]["hive-site"]["properties"]):
                putHiveSitePropertyAttribute("hive.server2.custom.authentication.class", "delete", "true")
        if 'hive-env' in services['configurations'] and 'hive_user' in services['configurations']['hive-env'][
            'properties']:
            hive_user = services['configurations']['hive-env']['properties']['hive_user']
        else:
            hive_user = 'hive'

        if 'hive-env' in configurations and 'hive_security_authorization' in configurations['hive-env']['properties']:
            ranger_hive_plugin_enabled = (
                configurations['hive-env']['properties']['hive_security_authorization'].lower() == 'ranger')
        elif 'hive-env' in services['configurations'] and 'hive_security_authorization' in \
                services['configurations']['hive-env']['properties']:
            ranger_hive_plugin_enabled = (
                services['configurations']['hive-env']['properties']['hive_security_authorization'].lower() == 'ranger')
        else:
            ranger_hive_plugin_enabled = False

        if ranger_hive_plugin_enabled and 'ranger-hive-plugin-properties' in services[
            'configurations']:
            Logger.info("Setting Hive Repo user for Ranger.")
            putRangerHivePluginProperty = self.putProperty(configurations, "ranger-hive-plugin-properties", services)
            rangerEnvHivePluginProperty = services["configurations"]["ranger-env"]["properties"][
                "ranger-hive-plugin-enabled"]
            putRangerHivePluginProperty("ranger-hive-plugin-enabled", rangerEnvHivePluginProperty)
        else:
            Logger.info("Not setting Hive Repo user for Ranger.")

        # Atlas
        hooks_property = "hive.exec.post.hooks"
        atlas_hook_class = "org.apache.atlas.hive.hook.HiveHook"
        if hooks_property in configurations["hive-site"]["properties"]:
            hooks_value = configurations["hive-site"]["properties"][hooks_property]
        else:
            hooks_value = ""
        hive_hooks = [x.strip() for x in hooks_value.split(",")]
        hive_hooks = [x for x in hive_hooks if x != ""]
        is_atlas_present_in_cluster = "ATLAS" in servicesList

        enable_atlas_hook = False
        if is_atlas_present_in_cluster:
            putHiveEnvProperty("hive.atlas.hook", "true")
        else:
            putHiveEnvProperty("hive.atlas.hook", "false")

        if 'hive-env' in configurations and 'hive.atlas.hook' in configurations['hive-env']['properties']:
            enable_atlas_hook = configurations['hive-env']['properties']['hive.atlas.hook'] == "true"
        elif 'hive-env' in services['configurations'] and 'hive.atlas.hook' in services['configurations']['hive-env'][
            'properties']:
            enable_atlas_hook = services['configurations']['hive-env']['properties']['hive.atlas.hook'] == "true"

        if enable_atlas_hook:
            # Append atlas hook if not already present.
            is_atlas_hook_in_config = atlas_hook_class in hive_hooks
            if not is_atlas_hook_in_config:
                hive_hooks.append(atlas_hook_class)
        else:
            # Remove the atlas hook since Atlas service is not present.
            hive_hooks = [x for x in hive_hooks if x != atlas_hook_class]

        # Convert hive_hooks back to a csv, unless there are 0 elements, which should be " "
        hooks_value = " " if len(hive_hooks) == 0 else ",".join(hive_hooks)
        putHiveSiteProperty(hooks_property, hooks_value)

        atlas_server_host_info = self.getHostWithComponent("ATLAS", "ATLAS_SERVER", services, hosts)
        if is_atlas_present_in_cluster and atlas_server_host_info:
            atlas_rest_host = atlas_server_host_info['Hosts']['host_name']
            scheme = "http"
            metadata_port = "21000"
            atlas_server_default_https_port = "21443"
            tls_enabled = "false"
            if 'application-properties' in services['configurations']:
                if 'atlas.enableTLS' in services['configurations']['application-properties']['properties']:
                    tls_enabled = services['configurations']['application-properties']['properties']['atlas.enableTLS']
                if 'atlas.server.http.port' in services['configurations']['application-properties']['properties']:
                    metadata_port = services['configurations']['application-properties']['properties'][
                        'atlas.server.http.port']
                if tls_enabled.lower() == "true":
                    scheme = "https"
                    if 'atlas.server.https.port' in services['configurations']['application-properties']['properties']:
                        metadata_port = services['configurations']['application-properties']['properties'][
                            'atlas.server.https.port']
                    else:
                        metadata_port = atlas_server_default_https_port
            putHiveSiteProperty('atlas.rest.address', '{0}://{1}:{2}'.format(scheme, atlas_rest_host, metadata_port))
        else:
            putHiveSitePropertyAttribute('atlas.cluster.name', 'delete', 'true')
            putHiveSitePropertyAttribute('atlas.rest.address', 'delete', 'true')

        # TEZ JVM options
        jvmGCParams = "-XX:+UseParallelGC"
        if "ambari-server-properties" in services and "java.home" in services["ambari-server-properties"]:
            # JDK8 needs different parameters
            match = re.match(".*\/jdk(1\.\d+)[\-\_\.][^/]*$", services["ambari-server-properties"]["java.home"])
            if match and len(match.groups()) > 0:
                # Is version >= 1.8
                versionSplits = re.split("\.", match.group(1))
                if versionSplits and len(versionSplits) > 1 and int(versionSplits[0]) > 0 and int(versionSplits[1]) > 7:
                    jvmGCParams = "-XX:+UseG1GC -XX:+ResizeTLAB"
        putHiveSiteProperty('hive.tez.java.opts', "-server -Djava.net.preferIPv4Stack=true -XX:NewRatio=8 -XX:+UseNUMA " + jvmGCParams + " -XX:+PrintGCDetails -verbose:gc -XX:+PrintGCTimeStamps")

        putHiveInteractiveEnvProperty = self.putProperty(configurations, "hive-interactive-env", services)
        putHiveInteractiveSiteProperty = self.putProperty(configurations, 'hive-interactive-site', services)
        putHiveInteractiveEnvPropertyAttribute = self.putPropertyAttribute(configurations, "hive-interactive-env")

        # For 'Hive Server Interactive', if the component exists.
        hsi_hosts = self.getHostsForComponent(services, "HIVE", "HIVE_SERVER_INTERACTIVE")
        hsi_properties = self.getServicesSiteProperties(services, 'hive-interactive-site')

        if len(hsi_hosts) > 0:
            putHiveInteractiveEnvProperty('enable_hive_interactive', 'true')

            # Update 'hive.llap.daemon.queue.name' property attributes if capacity scheduler is changed.
            if hsi_properties and 'hive.llap.daemon.queue.name' in hsi_properties:
                self.setLlapDaemonQueuePropAttributes(services, configurations)

                hsi_conf_properties = self.getSiteProperties(configurations, 'hive-interactive-site')

                hive_tez_default_queue = hsi_properties["hive.llap.daemon.queue.name"]
                if hsi_conf_properties and "hive.llap.daemon.queue.name" in hsi_conf_properties:
                    hive_tez_default_queue = hsi_conf_properties['hive.llap.daemon.queue.name']

                if hive_tez_default_queue:
                    putHiveInteractiveSiteProperty("hive.server2.tez.default.queues", hive_tez_default_queue)
                    self.logger.debug("Updated 'hive.server2.tez.default.queues' config : '{0}'".format(hive_tez_default_queue))
        else:
            self.logger.info("DBG: Setting 'num_llap_nodes' config's  READ ONLY attribute as 'True'.")
            putHiveInteractiveEnvProperty('enable_hive_interactive', 'false')
            putHiveInteractiveEnvPropertyAttribute("num_llap_nodes", "read_only", "true")

        if hsi_properties and "hive.llap.zk.sm.connectionString" in hsi_properties:
            zookeeper_host_port = self.getZKHostPortString(services)
            if zookeeper_host_port:
                putHiveInteractiveSiteProperty("hive.llap.zk.sm.connectionString", zookeeper_host_port)

        if 'hive-atlas-application.properties' in services['configurations']:
            putHiveAtlasHookProperty = self.putProperty(configurations, "hive-atlas-application.properties", services)
            putHiveAtlasHookPropertyAttribute = self.putPropertyAttribute(configurations,"hive-atlas-application.properties")
            if security_enabled and enable_atlas_hook:
                putHiveAtlasHookProperty('atlas.jaas.ticketBased-KafkaClient.loginModuleControlFlag', 'required')
                putHiveAtlasHookProperty('atlas.jaas.ticketBased-KafkaClient.loginModuleName', 'com.sun.security.auth.module.Krb5LoginModule')
                putHiveAtlasHookProperty('atlas.jaas.ticketBased-KafkaClient.option.useTicketCache', 'true')
            else:
                putHiveAtlasHookPropertyAttribute('atlas.jaas.ticketBased-KafkaClient.loginModuleControlFlag', 'delete', 'true')
                putHiveAtlasHookPropertyAttribute('atlas.jaas.ticketBased-KafkaClient.loginModuleName', 'delete', 'true')
                putHiveAtlasHookPropertyAttribute('atlas.jaas.ticketBased-KafkaClient.option.useTicketCache', 'delete', 'true')

    def recommendHBASEConfigurations(self, configurations, clusterData, services, hosts):
        putHbaseEnvPropertyAttributes = self.putPropertyAttribute(
            configurations, "hbase-env")

        hmaster_host = self.getHostWithComponent("HBASE", "HBASE_MASTER",
                                                 services, hosts)
        if hmaster_host is not None:
            host_ram = hmaster_host["Hosts"]["total_mem"]
            putHbaseEnvPropertyAttributes('hbase_master_heapsize', 'maximum',
                                          max(1024, int(host_ram / 1024)))

        rs_hosts = self.getHostsWithComponent("HBASE", "HBASE_REGIONSERVER",
                                              services, hosts)
        if rs_hosts is not None and len(rs_hosts) > 0:
            min_ram = rs_hosts[0]["Hosts"]["total_mem"]
            for host in rs_hosts:
                host_ram = host["Hosts"]["total_mem"]
                min_ram = min(min_ram, host_ram)

            putHbaseEnvPropertyAttributes('hbase_regionserver_heapsize',
                                          'maximum', max(1024, int(
                    min_ram * 0.8 / 1024)))

        putHbaseSiteProperty = self.putProperty(configurations, "hbase-site",
                                                services)
        putHbaseSitePropertyAttributes = self.putPropertyAttribute(
            configurations, "hbase-site")
        putHbaseSiteProperty("hbase.regionserver.global.memstore.size", '0.4')

        if 'hbase-env' in services['configurations'] and 'phoenix_sql_enabled' in \
                services['configurations']['hbase-env']['properties'] and \
                        'true' == services['configurations']['hbase-env']['properties']['phoenix_sql_enabled'].lower():
            putHbaseSiteProperty(
                "hbase.regionserver.wal.codec",
                'org.apache.hadoop.hbase.regionserver.wal.IndexedWALEditCodec')
            putHbaseSiteProperty("phoenix.functions.allowUserDefinedFunctions",
                                 'true')
        else:
            putHbaseSiteProperty(
                "hbase.regionserver.wal.codec",
                'org.apache.hadoop.hbase.regionserver.wal.WALCellCodec')
            if ('hbase.rpc.controllerfactory.class' in configurations["hbase-site"]["properties"]) or \
                    ('hbase-site' in services['configurations'] and 'hbase.rpc.controllerfactory.class' in
                        services['configurations']["hbase-site"]["properties"]):
                putHbaseSitePropertyAttributes(
                    'hbase.rpc.controllerfactory.class', 'delete', 'true')
            if ('phoenix.functions.allowUserDefinedFunctions' in configurations["hbase-site"]["properties"]) or \
                    ('hbase-site' in services['configurations'] and 'phoenix.functions.allowUserDefinedFunctions' in
                        services['configurations']["hbase-site"]["properties"]):
                putHbaseSitePropertyAttributes(
                    'phoenix.functions.allowUserDefinedFunctions', 'delete',
                    'true')

        if "ranger-env" in services["configurations"] and "ranger-hbase-plugin-properties" in services[
            "configurations"] and \
                        "ranger-hbase-plugin-enabled" in services["configurations"]["ranger-env"]["properties"]:
            putHbaseRangerPluginProperty = self.putProperty(configurations, "ranger-hbase-plugin-properties", services)
            rangerEnvHbasePluginProperty = services["configurations"]["ranger-env"]["properties"][
                "ranger-hbase-plugin-enabled"]
            putHbaseRangerPluginProperty("ranger-hbase-plugin-enabled", rangerEnvHbasePluginProperty)
            if "cluster-env" in services["configurations"] and "smokeuser" in services["configurations"]["cluster-env"][
                "properties"]:
                smoke_user = services["configurations"]["cluster-env"]["properties"]["smokeuser"]
                putHbaseRangerPluginProperty("policy_user", smoke_user)
        rangerPluginEnabled = ''
        if 'ranger-hbase-plugin-properties' in configurations and 'ranger-hbase-plugin-enabled' in \
                configurations['ranger-hbase-plugin-properties']['properties']:
            rangerPluginEnabled = configurations['ranger-hbase-plugin-properties']['properties'][
                'ranger-hbase-plugin-enabled']
        elif 'ranger-hbase-plugin-properties' in services['configurations'] and 'ranger-hbase-plugin-enabled' in \
                services['configurations']['ranger-hbase-plugin-properties']['properties']:
            rangerPluginEnabled = services['configurations']['ranger-hbase-plugin-properties']['properties'][
                'ranger-hbase-plugin-enabled']

        if rangerPluginEnabled and rangerPluginEnabled.lower() == 'Yes'.lower():
            putHbaseSiteProperty('hbase.security.authorization', 'true')

        # Recommend configs for bucket cache
        threshold = 23  # 2 Gb is reserved for other offheap memory
        mb = 1024
        if (int(clusterData["hbaseRam"]) > threshold):
            # To enable cache - calculate values
            regionserver_total_ram = int(clusterData["hbaseRam"]) * mb
            regionserver_heap_size = 20480
            regionserver_max_direct_memory_size = regionserver_total_ram - regionserver_heap_size
            hfile_block_cache_size = '0.4'
            block_cache_heap = 8192  # int(regionserver_heap_size * hfile_block_cache_size)
            hbase_regionserver_global_memstore_size = '0.4'
            reserved_offheap_memory = 2048
            bucketcache_offheap_memory = regionserver_max_direct_memory_size - reserved_offheap_memory
            hbase_bucketcache_size = bucketcache_offheap_memory
            hbase_bucketcache_percentage_in_combinedcache = float(
                bucketcache_offheap_memory) / hbase_bucketcache_size
            hbase_bucketcache_percentage_in_combinedcache_str = "{0:.4f}".format(
                math.ceil(hbase_bucketcache_percentage_in_combinedcache *
                          10000) / 10000.0)

            # Set values in hbase-site
            putHbaseSiteProperty('hfile.block.cache.size',
                                 hfile_block_cache_size)
            putHbaseSiteProperty('hbase.regionserver.global.memstore.size',
                                 hbase_regionserver_global_memstore_size)
            putHbaseSiteProperty('hbase.bucketcache.ioengine', 'offheap')
            putHbaseSiteProperty('hbase.bucketcache.size',
                                 hbase_bucketcache_size)
            putHbaseSiteProperty(
                'hbase.bucketcache.percentage.in.combinedcache',
                hbase_bucketcache_percentage_in_combinedcache_str)

            # Enable in hbase-env
            putHbaseEnvProperty = self.putProperty(configurations, "hbase-env",
                                                   services)
            putHbaseEnvProperty('hbase_max_direct_memory_size',
                                regionserver_max_direct_memory_size)
            putHbaseEnvProperty('hbase_regionserver_heapsize',
                                regionserver_heap_size)
        else:
            # Disable
            if ('hbase.bucketcache.ioengine' in configurations["hbase-site"]["properties"]) or \
                    ('hbase-site' in services['configurations'] and 'hbase.bucketcache.ioengine' in
                        services['configurations']["hbase-site"]["properties"]):
                putHbaseSitePropertyAttributes('hbase.bucketcache.ioengine',
                                               'delete', 'true')
            if ('hbase.bucketcache.size' in configurations["hbase-site"]["properties"]) or \
                    ('hbase-site' in services['configurations'] and 'hbase.bucketcache.size' in
                        services['configurations']["hbase-site"]["properties"]):
                putHbaseSitePropertyAttributes('hbase.bucketcache.size',
                                               'delete', 'true')
            if ('hbase.bucketcache.percentage.in.combinedcache' in configurations["hbase-site"]["properties"]) or \
                    ('hbase-site' in services['configurations'] and 'hbase.bucketcache.percentage.in.combinedcache' in
                        services['configurations']["hbase-site"]["properties"]):
                putHbaseSitePropertyAttributes(
                    'hbase.bucketcache.percentage.in.combinedcache', 'delete',
                    'true')



        # Authorization
        hbaseCoProcessorConfigs = {
            'hbase.coprocessor.region.classes': [],
            'hbase.coprocessor.regionserver.classes': [],
            'hbase.coprocessor.master.classes': []
        }
        for key in hbaseCoProcessorConfigs:
            hbase_coprocessor_classes = None
            if key in configurations["hbase-site"]["properties"]:
                hbase_coprocessor_classes = configurations["hbase-site"]["properties"][key].strip()
            elif 'hbase-site' in services['configurations'] and key in services['configurations']["hbase-site"]["properties"]:
                hbase_coprocessor_classes = services['configurations']["hbase-site"]["properties"][key].strip()
            if hbase_coprocessor_classes:
                hbaseCoProcessorConfigs[key] = hbase_coprocessor_classes.split(',')

        # If configurations has it - it has priority as it is calculated. Then, the service's configurations will be used.
        hbase_security_authorization = None
        if 'hbase-site' in configurations and 'hbase.security.authorization' in configurations['hbase-site']['properties']:
            hbase_security_authorization = configurations['hbase-site']['properties']['hbase.security.authorization']
        elif 'hbase-site' in services['configurations'] and 'hbase.security.authorization' in services['configurations']['hbase-site']['properties']:
            hbase_security_authorization = services['configurations']['hbase-site']['properties']['hbase.security.authorization']
        if hbase_security_authorization:
            if 'true' == hbase_security_authorization.lower():
                hbaseCoProcessorConfigs['hbase.coprocessor.master.classes'].append('org.apache.hadoop.hbase.security.access.AccessController')
                hbaseCoProcessorConfigs['hbase.coprocessor.regionserver.classes'].append('org.apache.hadoop.hbase.security.access.AccessController')
                # regional classes when hbase authorization is enabled
                authRegionClasses = ['org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint', 'org.apache.hadoop.hbase.security.access.AccessController']
                for item in range(len(authRegionClasses)):
                    hbaseCoProcessorConfigs['hbase.coprocessor.region.classes'].append(authRegionClasses[item])
            else:
                if 'org.apache.hadoop.hbase.security.access.AccessController' in hbaseCoProcessorConfigs['hbase.coprocessor.region.classes']:
                    hbaseCoProcessorConfigs['hbase.coprocessor.region.classes'].remove('org.apache.hadoop.hbase.security.access.AccessController')
                if 'org.apache.hadoop.hbase.security.access.AccessController' in hbaseCoProcessorConfigs['hbase.coprocessor.master.classes']:
                    hbaseCoProcessorConfigs['hbase.coprocessor.master.classes'].remove('org.apache.hadoop.hbase.security.access.AccessController')

                hbaseCoProcessorConfigs['hbase.coprocessor.region.classes'].append("org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint")
                if ('hbase.coprocessor.regionserver.classes' in configurations["hbase-site"]["properties"]) or \
                        ('hbase-site' in services['configurations'] and 'hbase.coprocessor.regionserver.classes' in services['configurations']["hbase-site"]["properties"]):
                    putHbaseSitePropertyAttributes('hbase.coprocessor.regionserver.classes', 'delete', 'true')
        else:
            hbaseCoProcessorConfigs['hbase.coprocessor.region.classes'].append("org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint")
            if ('hbase.coprocessor.regionserver.classes' in configurations["hbase-site"]["properties"]) or \
                    ('hbase-site' in services['configurations'] and 'hbase.coprocessor.regionserver.classes' in services['configurations']["hbase-site"]["properties"]):
                putHbaseSitePropertyAttributes('hbase.coprocessor.regionserver.classes', 'delete', 'true')

        # Authentication
        if 'hbase-site' in services['configurations'] and 'hbase.security.authentication' in services['configurations']['hbase-site']['properties']:
            if 'kerberos' == services['configurations']['hbase-site']['properties']['hbase.security.authentication'].lower():
                if 'org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint' not in hbaseCoProcessorConfigs['hbase.coprocessor.region.classes']:
                    hbaseCoProcessorConfigs['hbase.coprocessor.region.classes'].append('org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint')
                if 'org.apache.hadoop.hbase.security.token.TokenProvider' not in hbaseCoProcessorConfigs['hbase.coprocessor.region.classes']:
                    hbaseCoProcessorConfigs['hbase.coprocessor.region.classes'].append('org.apache.hadoop.hbase.security.token.TokenProvider')
            else:
                if 'org.apache.hadoop.hbase.security.token.TokenProvider' in hbaseCoProcessorConfigs['hbase.coprocessor.region.classes']:
                    hbaseCoProcessorConfigs['hbase.coprocessor.region.classes'].remove('org.apache.hadoop.hbase.security.token.TokenProvider')
        #Remove duplicates
        for key in hbaseCoProcessorConfigs:
            uniqueCoprocessorRegionClassList = []
            [uniqueCoprocessorRegionClassList.append(i)
             for i in hbaseCoProcessorConfigs[key] if
             not i in uniqueCoprocessorRegionClassList
             and (i.strip() not in ['{{hbase_coprocessor_region_classes}}', '{{hbase_coprocessor_master_classes}}', '{{hbase_coprocessor_regionserver_classes}}'])]
            putHbaseSiteProperty(key, ','.join(set(uniqueCoprocessorRegionClassList)))

        rangerClass = 'org.apache.ranger.authorization.hbase.RangerAuthorizationCoprocessor'
        nonRangerClass = 'org.apache.hadoop.hbase.security.access.AccessController'
        hbaseClassConfigs =  hbaseCoProcessorConfigs.keys()

        for item in range(len(hbaseClassConfigs)):
            if 'hbase-site' in services['configurations']:
                if hbaseClassConfigs[item] in services['configurations']['hbase-site']['properties']:
                    if 'hbase-site' in configurations and hbaseClassConfigs[item] in configurations['hbase-site']['properties']:
                        coprocessorConfig = configurations['hbase-site']['properties'][hbaseClassConfigs[item]]
                    else:
                        coprocessorConfig = services['configurations']['hbase-site']['properties'][hbaseClassConfigs[item]]
                    coprocessorClasses = coprocessorConfig.split(",")
                    coprocessorClasses = filter(None, coprocessorClasses) # Removes empty string elements from array
                    if rangerPluginEnabled and rangerPluginEnabled.lower() == 'Yes'.lower():
                        if nonRangerClass in coprocessorClasses:
                            coprocessorClasses.remove(nonRangerClass)
                        if not rangerClass in coprocessorClasses:
                            coprocessorClasses.append(rangerClass)
                        putHbaseSiteProperty(hbaseClassConfigs[item], ','.join(coprocessorClasses))
                    elif rangerPluginEnabled and rangerPluginEnabled.lower() == 'No'.lower():
                        if rangerClass in coprocessorClasses:
                            coprocessorClasses.remove(rangerClass)
                            if not nonRangerClass in coprocessorClasses:
                                coprocessorClasses.append(nonRangerClass)
                            putHbaseSiteProperty(hbaseClassConfigs[item], ','.join(coprocessorClasses))
                elif rangerPluginEnabled and rangerPluginEnabled.lower() == 'Yes'.lower():
                    putHbaseSiteProperty(hbaseClassConfigs[item], rangerClass)



        if 'hbase-env' in services['configurations'] and 'phoenix_sql_enabled' in \
                services['configurations']['hbase-env']['properties'] and \
                        'true' == services['configurations']['hbase-env']['properties']['phoenix_sql_enabled'].lower():
            if 'hbase.rpc.controllerfactory.class' in services['configurations']['hbase-site']['properties'] and \
                            services['configurations']['hbase-site']['properties'][
                                'hbase.rpc.controllerfactory.class'] == \
                            'org.apache.hadoop.hbase.ipc.controller.ServerRpcControllerFactory':
                putHbaseSitePropertyAttributes(
                    'hbase.rpc.controllerfactory.class', 'delete', 'true')

            putHbaseSiteProperty(
                "hbase.region.server.rpc.scheduler.factory.class",
                "org.apache.hadoop.hbase.ipc.PhoenixRpcSchedulerFactory")
        else:
            putHbaseSitePropertyAttributes(
                'hbase.region.server.rpc.scheduler.factory.class', 'delete',
                'true')

        if 'hbase-env' in services['configurations'] and 'hbase_user' in services['configurations']['hbase-env'][
            'properties']:
            hbase_user = services['configurations']['hbase-env']['properties']['hbase_user']
        else:
            hbase_user = 'hbase'

    def recommendKAFKAConfigurations(self, configurations, clusterData,
                                     services, hosts):
        kafka_mounts = [("log.dirs", "KAFKA_BROKER", "/data01/kafka,/data02/kafka,/data03/kafka,/data04/kafka,/data05/kafka,/data06/kafka,/data07/kafka,/data08/kafka,/data09/kafka,/data10/kafka,/data11/kafka,/data12/kafka", "multi")]

        self.updateMountProperties("kafka-broker", kafka_mounts,
                                   configurations, services, hosts)

        servicesList = [service["StackServices"]["service_name"]
                        for service in services["services"]]
        kafka_broker = getServicesSiteProperties(services, "kafka-broker")

        security_enabled = self.isSecurityEnabled(services)

        putKafkaBrokerProperty = self.putProperty(configurations,
                                                  "kafka-broker", services)
        putKafkaLog4jProperty = self.putProperty(configurations, "kafka-log4j",
                                                 services)
        putKafkaBrokerAttributes = self.putPropertyAttribute(configurations,
                                                             "kafka-broker")

        if security_enabled:
            kafka_env = getServicesSiteProperties(services, "kafka-env")
            kafka_user = kafka_env.get(
                'kafka_user') if kafka_env is not None else None

            if kafka_user is not None:
                kafka_super_users = kafka_broker.get(
                    'super.users') if kafka_broker is not None else None

                # kafka_super_super_users is expected to be formatted as:  User:user1;User:user2
                if kafka_super_users is not None and kafka_super_users != '':
                    # Parse kafka_super_users to get a set of unique user names and rebuild the property value
                    user_names = set()
                    user_names.add(kafka_user)
                    for match in re.findall('User:([^;]*)', kafka_super_users):
                        user_names.add(match)
                    kafka_super_users = 'User:' + ";User:".join(user_names)
                else:
                    kafka_super_users = 'User:' + kafka_user

                putKafkaBrokerProperty("super.users", kafka_super_users)

            putKafkaBrokerProperty(
                "principal.to.local.class",
                "kafka.security.auth.KerberosPrincipalToLocal")
            putKafkaBrokerProperty("security.inter.broker.protocol",
                                   "SASL_PLAINTEXT")
            putKafkaBrokerProperty("zookeeper.set.acl", "true")

        else:  # not security_enabled
            # remove unneeded properties
            putKafkaBrokerAttributes('super.users', 'delete', 'true')
            putKafkaBrokerAttributes('principal.to.local.class', 'delete',
                                     'true')
            putKafkaBrokerAttributes('security.inter.broker.protocol',
                                     'delete', 'true')
        if security_enabled:
            putKafkaBrokerProperty("authorizer.class.name", 'kafka.security.auth.SimpleAclAuthorizer')
        else:
            putKafkaBrokerAttributes('authorizer.class.name', 'delete', 'true')

        if "AMBARI_METRICS" in servicesList:
            putKafkaBrokerProperty('kafka.metrics.reporters',
                                   'org.apache.hadoop.metrics2.sink.kafka.KafkaTimelineMetricsReporter')

        if "ranger-env" in services["configurations"] \
                and "ranger-kafka-plugin-properties" in services["configurations"] \
                and "ranger-kafka-plugin-enabled" in services["configurations"]["ranger-env"]["properties"]:
            putKafkaRangerPluginProperty = self.putProperty(configurations, "ranger-kafka-plugin-properties", services)
            ranger_kafka_plugin_enabled = services["configurations"]["ranger-env"]["properties"][
                "ranger-kafka-plugin-enabled"]
            putKafkaRangerPluginProperty("ranger-kafka-plugin-enabled", ranger_kafka_plugin_enabled)

        # Determine if the Ranger/Kafka Plugin is enabled
        ranger_plugin_enabled = "RANGER" in servicesList
        # Only if the RANGER service is installed....
        if ranger_plugin_enabled:
            # If ranger-kafka-plugin-properties/ranger-kafka-plugin-enabled,
            # determine if the Ranger/Kafka plug-in enabled enabled or not
            if 'ranger-kafka-plugin-properties' in configurations and \
                            'ranger-kafka-plugin-enabled' in configurations['ranger-kafka-plugin-properties'][
                        'properties']:
                ranger_plugin_enabled = configurations['ranger-kafka-plugin-properties']['properties'][
                                            'ranger-kafka-plugin-enabled'].lower() == 'yes'
            # If ranger-kafka-plugin-properties/ranger-kafka-plugin-enabled was not changed,
            # determine if the Ranger/Kafka plug-in enabled enabled or not
            elif 'ranger-kafka-plugin-properties' in services['configurations'] and \
                            'ranger-kafka-plugin-enabled' in \
                            services['configurations']['ranger-kafka-plugin-properties']['properties']:
                ranger_plugin_enabled = services['configurations']['ranger-kafka-plugin-properties']['properties'][
                                            'ranger-kafka-plugin-enabled'].lower() == 'yes'

        # Determine the value for kafka-broker/authorizer.class.name
        if ranger_plugin_enabled:
            # If the Ranger plugin for Kafka is enabled, set authorizer.class.name to
            # "org.apache.ranger.authorization.kafka.authorizer.RangerKafkaAuthorizer" whether Kerberos is
            # enabled or not.
            putKafkaBrokerProperty("authorizer.class.name",
                                   'org.apache.ranger.authorization.kafka.authorizer.RangerKafkaAuthorizer')
        elif security_enabled:
            putKafkaBrokerProperty("authorizer.class.name", 'kafka.security.auth.SimpleAclAuthorizer')
        else:
            putKafkaBrokerAttributes('authorizer.class.name', 'delete', 'true')

        if ranger_plugin_enabled:
            kafkaLog4jRangerLines = [{
                "name": "log4j.appender.rangerAppender",
                "value": "org.apache.log4j.DailyRollingFileAppender"
            },
                {
                    "name": "log4j.appender.rangerAppender.DatePattern",
                    "value": "'.'yyyy-MM-dd-HH"
                },
                {
                    "name": "log4j.appender.rangerAppender.File",
                    "value": "${kafka.logs.dir}/ranger_kafka.log"
                },
                {
                    "name": "log4j.appender.rangerAppender.layout",
                    "value": "org.apache.log4j.PatternLayout"
                },
                {
                    "name": "log4j.appender.rangerAppender.layout.ConversionPattern",
                    "value": "%d{ISO8601} %p [%t] %C{6} (%F:%L) - %m%n"
                },
                {
                    "name": "log4j.logger.org.apache.ranger",
                    "value": "INFO, rangerAppender"
                }]
            # change kafka-log4j when ranger plugin is installed
            if 'kafka-log4j' in services['configurations'] and 'content' in services['configurations']['kafka-log4j'][
                'properties']:
                kafkaLog4jContent = services['configurations']['kafka-log4j']['properties']['content']
                for item in range(len(kafkaLog4jRangerLines)):
                    if kafkaLog4jRangerLines[item]["name"] not in kafkaLog4jContent:
                        kafkaLog4jContent += '\n' + kafkaLog4jRangerLines[item]["name"] + '=' + \
                                             kafkaLog4jRangerLines[item]["value"]
                putKafkaLog4jProperty("content", kafkaLog4jContent)

            zookeeper_host_port = self.getZKHostPortString(services)
            if zookeeper_host_port:
                putRangerKafkaPluginProperty = self.putProperty(configurations, 'ranger-kafka-plugin-properties',
                                                                services)
                putRangerKafkaPluginProperty('zookeeper.connect', zookeeper_host_port)

        if 'kafka-env' in services['configurations'] and 'kafka_user' in services['configurations']['kafka-env'][
            'properties']:
            kafka_user = services['configurations']['kafka-env']['properties']['kafka_user']
        else:
            kafka_user = "kafka"


    def getMountPathVariations(self, initial_value, component_name, services, hosts):
        available_mounts = []

        if not initial_value:
            return available_mounts

        mounts = self.__getSameHostMounts(hosts)
        sep = "/"

        if not mounts:
            return available_mounts

        for mount in mounts:
            new_mount = initial_value if mount == "/" else os.path.join(mount + sep, initial_value.lstrip(sep))
            if new_mount not in available_mounts:
                available_mounts.append(new_mount)

        # no list transformations after filling the list, because this will cause item order change
        return available_mounts

    def getMountPoints(self, hosts):
        mount_points = []

        for item in hosts["items"]:
            if "disk_info" in item["Hosts"]:
                mount_points.append(item["Hosts"]["disk_info"])

        return mount_points

    def isSecurityEnabled(self, services):
        return "cluster-env" in services["configurations"] \
               and "security_enabled" in services["configurations"]["cluster-env"]["properties"] \
               and services["configurations"]["cluster-env"]["properties"]["security_enabled"].lower() == "true"

    def parseCardinality(self, cardinality, hostsCount):
        if not cardinality:
            return (None, None)

        if "+" in cardinality:
            return (int(cardinality[:-1]), int(hostsCount))
        elif "-" in cardinality:
            nums = cardinality.split("-")
            return (int(nums[0]), int(nums[1]))
        elif "ALL" == cardinality:
            return (int(hostsCount), int(hostsCount))
        elif cardinality.isdigit():
            return (int(cardinality), int(cardinality))

        return (None, None)

    def getServiceNames(self, services):
        return [service["StackServices"]["service_name"] for service in services["services"]]

    def filterHostMounts(self, hosts, services):
        if not services or "items" not in hosts:
            return hosts

        banned_filesystems = ["devtmpfs", "tmpfs", "vboxsf", "cdfs"]
        banned_mount_points = ["/etc/resolv.conf", "/etc/hostname", "/boot", "/mnt", "/tmp", "/run/secrets"]

        cluster_env = getServicesSiteProperties(services, "cluster-env")
        ignore_list = []

        if cluster_env and "agent_mounts_ignore_list" in cluster_env and cluster_env[
            "agent_mounts_ignore_list"].strip():
            ignore_list = [x.strip() for x in cluster_env["agent_mounts_ignore_list"].strip().split(",")]

        ignore_list.extend(banned_mount_points)

        for host in hosts["items"]:
            if "Hosts" not in host and "disk_info" not in host["Hosts"]:
                continue

            host = host["Hosts"]
            disk_info = []

            for disk in host["disk_info"]:
                if disk["mountpoint"] not in ignore_list \
                        and disk["type"].lower() not in banned_filesystems:
                    disk_info.append(disk)

            host["disk_info"] = disk_info

        return hosts

    def putProperty(self, config, configType, services=None):
        userConfigs = {}
        changedConfigs = []
        # if services parameter, prefer values, set by user
        if services:
            if 'configurations' in services.keys():
                userConfigs = services['configurations']
            if 'changed-configurations' in services.keys():
                changedConfigs = services["changed-configurations"]

        if configType not in config:
            config[configType] = {}
        if "properties" not in config[configType]:
            config[configType]["properties"] = {}

        def appendProperty(key, value):
            # If property exists in changedConfigs, do not override, use user defined property
            if not self.isPropertyRequested(configType, key, changedConfigs) \
                    and configType in userConfigs and key in userConfigs[configType]['properties']:
                config[configType]["properties"][key] = userConfigs[configType]['properties'][key]
            else:
                config[configType]["properties"][key] = str(value)

        return appendProperty

    def __isPropertyInChangedConfigs(self, configType, propertyName, changedConfigs):
        for changedConfig in changedConfigs:
            if changedConfig['type'] == configType and changedConfig['name'] == propertyName:
                return True
        return False

    def isPropertyRequested(self, configType, propertyName, changedConfigs):
        if self.allRequestedProperties:
            return configType in self.allRequestedProperties and propertyName in self.allRequestedProperties[configType]
        else:
            return not self.__isPropertyInChangedConfigs(configType, propertyName, changedConfigs)

    def __getSameHostMounts(self, hosts):
        if not hosts:
            return None

        hostMounts = self.getMountPoints(hosts)
        mounts = []
        for m in hostMounts:
            host_mounts = set([item["mountpoint"] for item in m])
            mounts = host_mounts if not mounts else mounts & host_mounts

        return sorted(mounts)

    def getMountPathVariation(self, initial_value, component_name, services, hosts):
        try:
            return [self.getMountPathVariations(initial_value, component_name, services, hosts)[0]]
        except IndexError:
            return []

    def putPropertyAttribute(self, config, configType):
        if configType not in config:
            config[configType] = {}

        def appendPropertyAttribute(key, attribute, attributeValue):
            if "property_attributes" not in config[configType]:
                config[configType]["property_attributes"] = {}
            if key not in config[configType]["property_attributes"]:
                config[configType]["property_attributes"][key] = {}
            config[configType]["property_attributes"][key][attribute] = attributeValue if isinstance(attributeValue,
                                                                                                     list) else str(
                attributeValue)

        return appendPropertyAttribute

    def updateMountProperties(self, siteConfig, propertyDefinitions, configurations, services, hosts):
        props = getServicesSiteProperties(services, siteConfig)
        put_f = self.putProperty(configurations, siteConfig, services)

        for prop_item in propertyDefinitions:
            name, component, default_value, rc_type = prop_item
            recommendation = None

            if props is None or name not in props:
                if rc_type == "multi":
                    recommendation = self.getMountPathVariations(default_value, component, services, hosts)
                else:
                    recommendation = self.getMountPathVariation(default_value, component, services, hosts)
            elif props and name in props and props[name] == default_value:
                if rc_type == "multi":
                    recommendation = self.getMountPathVariations(default_value, component, services, hosts)
                else:
                    recommendation = self.getMountPathVariation(default_value, component, services, hosts)

            if recommendation:
                put_f(name, ",".join(recommendation))

    def getHostNamesWithComponent(self, serviceName, componentName, services):
        """
        Returns the list of hostnames on which service component is installed
        """
        if services is not None and serviceName in [service["StackServices"]["service_name"] for service in
                                                    services["services"]]:
            service = [serviceEntry for serviceEntry in services["services"] if
                       serviceEntry["StackServices"]["service_name"] == serviceName][0]
            components = [componentEntry for componentEntry in service["components"] if
                          componentEntry["StackServiceComponents"]["component_name"] == componentName]
            if (len(components) > 0 and len(components[0]["StackServiceComponents"]["hostnames"]) > 0):
                componentHostnames = components[0]["StackServiceComponents"]["hostnames"]
                return componentHostnames
        return []

    def getHostsWithComponent(self, serviceName, componentName, services, hosts):
        if services is not None and hosts is not None and serviceName in [service["StackServices"]["service_name"] for
                                                                          service in services["services"]]:
            service = [serviceEntry for serviceEntry in services["services"] if
                       serviceEntry["StackServices"]["service_name"] == serviceName][0]
            components = [componentEntry for componentEntry in service["components"] if
                          componentEntry["StackServiceComponents"]["component_name"] == componentName]
            if (len(components) > 0 and len(components[0]["StackServiceComponents"]["hostnames"]) > 0):
                componentHostnames = components[0]["StackServiceComponents"]["hostnames"]
                componentHosts = [host for host in hosts["items"] if host["Hosts"]["host_name"] in componentHostnames]
                return componentHosts
        return []

    def getHostWithComponent(self, serviceName, componentName, services, hosts):
        componentHosts = self.getHostsWithComponent(serviceName, componentName, services, hosts)
        if (len(componentHosts) > 0):
            return componentHosts[0]
        return None

    def getHostComponentsByCategories(self, hostname, categories, services, hosts):
        components = []
        if services is not None and hosts is not None:
            for service in services["services"]:
                components.extend([componentEntry for componentEntry in service["components"]
                                   if componentEntry["StackServiceComponents"]["component_category"] in categories
                                   and hostname in componentEntry["StackServiceComponents"]["hostnames"]])
        return components

    def getZKHostPortString(self, services, include_port=True):
        """
        Returns the comma delimited string of zookeeper server host with the configure port installed in a cluster
        Example: zk.host1.org:2181,zk.host2.org:2181,zk.host3.org:2181
        include_port boolean param -> If port is also needed.
        """
        servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
        include_zookeeper = "ZOOKEEPER" in servicesList
        zookeeper_host_port = ''

        if include_zookeeper:
            zookeeper_hosts = self.getHostNamesWithComponent("ZOOKEEPER", "ZOOKEEPER_SERVER", services)
            zookeeper_host_port_arr = []

            if include_port:
                zookeeper_port = self.getZKPort(services)
                for i in range(len(zookeeper_hosts)):
                    zookeeper_host_port_arr.append(zookeeper_hosts[i] + ':' + zookeeper_port)
            else:
                for i in range(len(zookeeper_hosts)):
                    zookeeper_host_port_arr.append(zookeeper_hosts[i])

            zookeeper_host_port = ",".join(zookeeper_host_port_arr)
        return zookeeper_host_port

    def getZKPort(self, services):
        zookeeper_port = '2181'  # default port
        if 'zoo.cfg' in services['configurations'] and (
                    'clientPort' in services['configurations']['zoo.cfg']['properties']):
            zookeeper_port = services['configurations']['zoo.cfg']['properties']['clientPort']
        return zookeeper_port

    def getConfigurationClusterSummary(self, servicesList, hosts, components, services):

        hBaseInstalled = False
        if 'HBASE' in servicesList:
            hBaseInstalled = True

        cluster = {
            "cpu": 0,
            "disk": 0,
            "ram": 0,
            "hBaseInstalled": hBaseInstalled,
            "components": components
        }

        if len(hosts["items"]) > 0:
            nodeManagerHosts = self.getHostsWithComponent("YARN", "NODEMANAGER", services, hosts)
            # NodeManager host with least memory is generally used in calculations as it will work in larger hosts.
            if nodeManagerHosts is not None and len(nodeManagerHosts) > 0:
                nodeManagerHost = nodeManagerHosts[0];
                for nmHost in nodeManagerHosts:
                    if nmHost["Hosts"]["total_mem"] < nodeManagerHost["Hosts"]["total_mem"]:
                        nodeManagerHost = nmHost
                host = nodeManagerHost["Hosts"]
                cluster["referenceNodeManagerHost"] = host
            else:
                host = hosts["items"][0]["Hosts"]
            cluster["referenceHost"] = host
            cluster["cpu"] = host["cpu_count"]
            cluster["disk"] = len(host["disk_info"])
            cluster["ram"] = int(host["total_mem"] / (1024 * 1024))

        ramRecommendations = [
            {"os": 1, "hbase": 1},
            {"os": 2, "hbase": 1},
            {"os": 2, "hbase": 2},
            {"os": 4, "hbase": 4},
            {"os": 6, "hbase": 8},
            {"os": 8, "hbase": 8},
            {"os": 8, "hbase": 8},
            {"os": 12, "hbase": 16},
            {"os": 24, "hbase": 24},
            {"os": 32, "hbase": 32},
            {"os": 64, "hbase": 32}
        ]
        index = {
            cluster["ram"] <= 4: 0,
            4 < cluster["ram"] <= 8: 1,
            8 < cluster["ram"] <= 16: 2,
            16 < cluster["ram"] <= 24: 3,
            24 < cluster["ram"] <= 48: 4,
            48 < cluster["ram"] <= 64: 5,
            64 < cluster["ram"] <= 72: 6,
            72 < cluster["ram"] <= 96: 7,
            96 < cluster["ram"] <= 128: 8,
            128 < cluster["ram"] <= 256: 9,
            256 < cluster["ram"]: 10
        }[1]

        cluster["reservedRam"] = ramRecommendations[index]["os"]
        cluster["hbaseRam"] = ramRecommendations[index]["hbase"]

        cluster["minContainerSize"] = {
            cluster["ram"] <= 4: 256,
            4 < cluster["ram"] <= 8: 512,
            8 < cluster["ram"] <= 24: 1024,
            24 < cluster["ram"]: 2048
        }[1]

        totalAvailableRam = cluster["ram"] - cluster["reservedRam"]
        if cluster["hBaseInstalled"]:
            totalAvailableRam -= cluster["hbaseRam"]
        cluster["totalAvailableRam"] = max(512, totalAvailableRam * 1024)
        '''containers = max(3, min (2*cores,min (1.8*DISKS,(Total available RAM) / MIN_CONTAINER_SIZE))))'''
        cluster["containers"] = round(max(3,
                                          min(2 * cluster["cpu"],
                                              min(ceil(1.8 * cluster["disk"]),
                                                  cluster["totalAvailableRam"] / cluster["minContainerSize"]))))

        '''ramPerContainers = max(2GB, RAM - reservedRam - hBaseRam) / containers'''
        cluster["ramPerContainer"] = abs(cluster["totalAvailableRam"] / cluster["containers"])
        '''If greater than 1GB, value will be in multiples of 512.'''
        if cluster["ramPerContainer"] > 1024:
            cluster["ramPerContainer"] = int(cluster["ramPerContainer"] / 512) * 512

        cluster["mapMemory"] = int(cluster["ramPerContainer"])
        cluster["reduceMemory"] = cluster["ramPerContainer"]
        cluster["amMemory"] = max(cluster["mapMemory"], cluster["reduceMemory"])

        return cluster

    def getMountPointForDir(self, dir, mountPoints):
        bestMountFound = None
        if dir:
            dir = re.sub("^file://", "", dir, count=1).strip().lower()

        for mountPoint in mountPoints:
            if os.path.join(dir, "").startswith(os.path.join(mountPoint, "")):
                if bestMountFound is None:
                    bestMountFound = mountPoint
            elif os.path.join(bestMountFound, "").count(os.path.sep) < os.path.join(mountPoint, "").count(os.path.sep):
                bestMountFound = mountPoint

        return bestMountFound

    def getCallContext(self, services):
        if services:
            if DefaultStackAdvisor.ADVISOR_CONTEXT in services:
                Logger.info("call type context : " + str(services[DefaultStackAdvisor.ADVISOR_CONTEXT]))
                return services[DefaultStackAdvisor.ADVISOR_CONTEXT][DefaultStackAdvisor.CALL_TYPE]
        return ""

    def getUserOperationContext(self, services, contextName):
        if services:
            if 'user-context' in services.keys():
                userContext = services["user-context"]
                if contextName in userContext:
                    return userContext[contextName]
        return None

    def getMastersWithMultipleInstances(self):
        return ['ZOOKEEPER_SERVER', 'HBASE_MASTER']

    def getNotValuableComponents(self):
        return ['JOURNALNODE', 'ZKFC', 'GANGLIA_MONITOR']

    def getNotPreferableOnServerComponents(self):
        return ['GANGLIA_SERVER', 'METRICS_COLLECTOR']

    def getCardinalitiesDict(self, hosts):
        return {
            'ZOOKEEPER_SERVER': {"min": 3},
            'HBASE_MASTER': {"min": 1},
        }

    def getComponentLayoutSchemes(self):
        return {
            'NAMENODE': {"else": 0},
            'SECONDARY_NAMENODE': {"else": 1},
            'HBASE_MASTER': {6: 0, 31: 2, "else": 3},

            'HISTORYSERVER': {31: 1, "else": 2},
            'RESOURCEMANAGER': {31: 1, "else": 2},

            'OOZIE_SERVER': {6: 1, 31: 2, "else": 3},

            'HIVE_SERVER': {6: 1, 31: 2, "else": 4},
            'HIVE_METASTORE': {6: 1, 31: 2, "else": 4},
            'WEBHCAT_SERVER': {6: 1, 31: 2, "else": 4},
            'METRICS_COLLECTOR': {3: 2, 6: 2, 31: 3, "else": 5},
        }

    def mergeValidators(self, parentValidators, childValidators):
        for service, configsDict in childValidators.iteritems():
            if service not in parentValidators:
                parentValidators[service] = {}
            parentValidators[service].update(configsDict)

    def checkSiteProperties(self, siteProperties, *propertyNames):
        """
        Check if properties defined in site properties.
        :param siteProperties: config properties dict
        :param *propertyNames: property names to validate
        :returns: True if all properties defined, in other cases returns False
        """
        if siteProperties is None:
            return False
        for name in propertyNames:
            if not (name in siteProperties):
                return False
        return True

    def get_service_component_meta(self, service, component, services):
        """
        Function retrieve service component meta information as dict from services.json
        If no service or component found, would be returned empty dict

        Return value example:
            "advertise_version" : true,
            "bulk_commands_display_name" : "",
            "bulk_commands_master_component_name" : "",
            "cardinality" : "1+",
            "component_category" : "CLIENT",
            "component_name" : "HBASE_CLIENT",
            "custom_commands" : [ ],
            "decommission_allowed" : false,
            "display_name" : "HBase Client",
            "has_bulk_commands_definition" : false,
            "is_client" : true,
            "is_master" : false,
            "reassign_allowed" : false,
            "recovery_enabled" : false,
            "service_name" : "HBASE",
            "stack_name" : "HDP",
            "stack_version" : "2.5",
            "hostnames" : [ "host1", "host2" ]

        :type service str
        :type component str
        :type services dict
        :rtype dict
        """
        __stack_services = "StackServices"
        __stack_service_components = "StackServiceComponents"

        if not services:
            return {}

        service_meta = [item for item in services["services"] if item[__stack_services]["service_name"] == service]
        if len(service_meta) == 0:
            return {}

        service_meta = service_meta[0]
        component_meta = [item for item in service_meta["components"] if
                          item[__stack_service_components]["component_name"] == component]

        if len(component_meta) == 0:
            return {}

        return component_meta[0][__stack_service_components]

    def is_secured_cluster(self, services):
        """
        Detects if cluster is secured or not
        :type services dict
        :rtype bool
        """
        return services and "cluster-env" in services["configurations"] and \
               "security_enabled" in services["configurations"]["cluster-env"]["properties"] and \
               services["configurations"]["cluster-env"]["properties"]["security_enabled"].lower() == "true"

    def get_services_list(self, services):
        """
        Returns available services as list

        :type services dict
        :rtype list
        """
        if not services:
            return []

        return [service["StackServices"]["service_name"] for service in services["services"]]

    def get_components_list(self, service, services):
        """
        Return list of components for specific service
        :type service str
        :type services dict
        :rtype list
        """
        __stack_services = "StackServices"
        __stack_service_components = "StackServiceComponents"

        if not services:
            return []

        service_meta = [item for item in services["services"] if item[__stack_services]["service_name"] == service]
        if len(service_meta) == 0:
            return []

        service_meta = service_meta[0]
        return [item[__stack_service_components]["component_name"] for item in service_meta["components"]]

    def get_system_min_uid(self):
        login_defs = '/etc/login.defs'
        uid_min_tag = 'UID_MIN'
        comment_tag = '#'
        uid_min = uid_default = '1000'
        uid = None

        if os.path.exists(login_defs):
            with open(login_defs, 'r') as f:
                data = f.read().split('\n')
                # look for uid_min_tag in file
                uid = filter(lambda x: uid_min_tag in x, data)
                # filter all lines, where uid_min_tag was found in comments
                uid = filter(lambda x: x.find(comment_tag) > x.find(uid_min_tag) or x.find(comment_tag) == -1, uid)

            if uid is not None and len(uid) > 0:
                uid = uid[0]
                comment = uid.find(comment_tag)
                tag = uid.find(uid_min_tag)
                if comment == -1:
                    uid_tag = tag + len(uid_min_tag)
                    uid_min = uid[uid_tag:].strip()
                elif comment > tag:
                    uid_tag = tag + len(uid_min_tag)
                    uid_min = uid[uid_tag:comment].strip()

        # check result for value
        try:
            int(uid_min)
        except ValueError:
            return uid_default

        return uid_min

    def recommendYarnQueue(self, services, catalog_name=None, queue_property=None):
        old_queue_name = None

        if services and 'configurations' in services:
            configurations = services["configurations"]
            if catalog_name in configurations and queue_property in configurations[catalog_name]["properties"]:
                old_queue_name = configurations[catalog_name]["properties"][queue_property]

            capacity_scheduler_properties, _ = self.getCapacitySchedulerProperties(services)
            leaf_queues = sorted(self.getAllYarnLeafQueues(capacity_scheduler_properties))

            if leaf_queues and (old_queue_name is None or old_queue_name not in leaf_queues):
                return leaf_queues.pop()
            elif old_queue_name and old_queue_name in leaf_queues:
                return None
        return "default"

    def getCapacitySchedulerProperties(self, services):
        capacity_scheduler_properties = dict()
        received_as_key_value_pair = True
        if "capacity-scheduler" in services['configurations']:
            if "capacity-scheduler" in services['configurations']["capacity-scheduler"]["properties"]:
                cap_sched_props_as_str = services['configurations']["capacity-scheduler"]["properties"][
                    "capacity-scheduler"]
                if cap_sched_props_as_str:
                    cap_sched_props_as_str = str(cap_sched_props_as_str).split('\n')
                    if len(cap_sched_props_as_str) > 0 and cap_sched_props_as_str[0] != 'null':
                        # Received confgs as one "\n" separated string
                        for property in cap_sched_props_as_str:
                            key, sep, value = property.partition("=")
                            capacity_scheduler_properties[key] = value
                        self.logger.info(
                            "'capacity-scheduler' configs is passed-in as a single '\\n' separated string. "
                            "count(services['configurations']['capacity-scheduler']['properties']['capacity-scheduler']) = "
                            "{0}".format(len(capacity_scheduler_properties)))
                        received_as_key_value_pair = False
                    else:
                        self.logger.info(
                            "Passed-in services['configurations']['capacity-scheduler']['properties']['capacity-scheduler'] is 'null'.")
                else:
                    self.logger.info("'capacity-scheduler' configs not passed-in as single '\\n' string in "
                                     "services['configurations']['capacity-scheduler']['properties']['capacity-scheduler'].")
            if not capacity_scheduler_properties:
                # Received configs as a dictionary (Generally on 1st invocation).
                capacity_scheduler_properties = services['configurations']["capacity-scheduler"]["properties"]
                self.logger.info("'capacity-scheduler' configs is passed-in as a dictionary. "
                                 "count(services['configurations']['capacity-scheduler']['properties']) = {0}".format(
                    len(capacity_scheduler_properties)))
        else:
            self.logger.error("Couldn't retrieve 'capacity-scheduler' from services.")

        self.logger.info("Retrieved 'capacity-scheduler' received as dictionary : '{0}'. configs : {1}" \
                         .format(received_as_key_value_pair, capacity_scheduler_properties.items()))
        return capacity_scheduler_properties, received_as_key_value_pair

    def getAllYarnLeafQueues(self, capacitySchedulerProperties):
        """
        Gets all YARN leaf queues.
        """
        config_list = capacitySchedulerProperties.keys()
        yarn_queues = None
        leafQueueNames = set()
        if 'yarn.scheduler.capacity.root.queues' in config_list:
            yarn_queues = capacitySchedulerProperties.get('yarn.scheduler.capacity.root.queues')

        if yarn_queues:
            toProcessQueues = yarn_queues.split(",")
            while len(toProcessQueues) > 0:
                queue = toProcessQueues.pop()
                queueKey = "yarn.scheduler.capacity.root." + queue + ".queues"
                if queueKey in capacitySchedulerProperties:
                    # If parent queue, add children
                    subQueues = capacitySchedulerProperties[queueKey].split(",")
                    for subQueue in subQueues:
                        toProcessQueues.append(queue + "." + subQueue)
                else:
                    leafQueuePathSplits = queue.split(".")
                    if leafQueuePathSplits > 0:
                        leafQueueName = leafQueuePathSplits[-1]
                        leafQueueNames.add(leafQueueName)
        return leafQueueNames
        # endregion

    def getOracleDBConnectionHostPort(self, db_type, db_host, rangerDbName):
        connection_string = self.getDBConnectionHostPort(db_type, db_host)
        colon_count = db_host.count(':')
        if colon_count == 1 and '/' in db_host:
            connection_string = "//" + connection_string
        elif colon_count == 0 or colon_count == 1:
            connection_string = "//" + connection_string + "/" + rangerDbName if rangerDbName else "//" + connection_string

        return connection_string

    def getDBConnectionHostPort(self, db_type, db_host):
        DB_TYPE_DEFAULT_PORT_MAP = {"MYSQL": "3306", "ORACLE": "1521", "POSTGRES": "5432", "MSSQL": "1433",
                                    "SQLA": "2638"}
        connection_string = ""
        if db_type is None or db_type == "":
            return connection_string
        else:
            colon_count = db_host.count(':')
            if colon_count == 0:
                if DB_TYPE_DEFAULT_PORT_MAP.has_key(db_type):
                    connection_string = db_host + ":" + DB_TYPE_DEFAULT_PORT_MAP[db_type]
                else:
                    connection_string = db_host
            elif colon_count == 1:
                connection_string = db_host
            elif colon_count == 2:
                connection_string = db_host
        return connection_string

    def recommendRangerConfigurations(self, configurations, clusterData, services, hosts):
        servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
        putRangerAdminProperty = self.putProperty(configurations, "ranger-admin-site", services)
        putRangerEnvProperty = self.putProperty(configurations, "ranger-env", services)
        putRangerUgsyncSite = self.putProperty(configurations, "ranger-ugsync-site", services)

        if 'admin-properties' in services['configurations'] and (
                    'DB_FLAVOR' in services['configurations']['admin-properties']['properties']) \
                and ('db_host' in services['configurations']['admin-properties']['properties']) and (
                    'db_name' in services['configurations']['admin-properties']['properties']):

            rangerDbFlavor = services['configurations']["admin-properties"]["properties"]["DB_FLAVOR"]
            rangerDbHost = services['configurations']["admin-properties"]["properties"]["db_host"]
            rangerDbName = services['configurations']["admin-properties"]["properties"]["db_name"]
            ranger_db_url_dict = {
                'MYSQL': {'ranger.jpa.jdbc.driver': 'com.mysql.jdbc.Driver',
                          'ranger.jpa.jdbc.url': 'jdbc:mysql://' + self.getDBConnectionHostPort(rangerDbFlavor,
                                                                                                rangerDbHost) + '/' + rangerDbName},
                'ORACLE': {'ranger.jpa.jdbc.driver': 'oracle.jdbc.driver.OracleDriver',
                           'ranger.jpa.jdbc.url': 'jdbc:oracle:thin:@' + self.getOracleDBConnectionHostPort(
                               rangerDbFlavor, rangerDbHost, rangerDbName)},
                'POSTGRES': {'ranger.jpa.jdbc.driver': 'org.postgresql.Driver',
                             'ranger.jpa.jdbc.url': 'jdbc:postgresql://' + self.getDBConnectionHostPort(rangerDbFlavor,
                                                                                                        rangerDbHost) + '/' + rangerDbName},
                'MSSQL': {'ranger.jpa.jdbc.driver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
                          'ranger.jpa.jdbc.url': 'jdbc:sqlserver://' + self.getDBConnectionHostPort(rangerDbFlavor,
                                                                                                    rangerDbHost) + ';databaseName=' + rangerDbName},
                'SQLA': {'ranger.jpa.jdbc.driver': 'sap.jdbc4.sqlanywhere.IDriver',
                         'ranger.jpa.jdbc.url': 'jdbc:sqlanywhere:host=' + self.getDBConnectionHostPort(rangerDbFlavor,
                                                                                                        rangerDbHost) + ';database=' + rangerDbName}
            }
            rangerDbProperties = ranger_db_url_dict.get(rangerDbFlavor, ranger_db_url_dict['MYSQL'])
            for key in rangerDbProperties:
                putRangerAdminProperty(key, rangerDbProperties.get(key))

            if 'admin-properties' in services['configurations'] and (
                        'DB_FLAVOR' in services['configurations']['admin-properties']['properties']) \
                    and ('db_host' in services['configurations']['admin-properties']['properties']):

                rangerDbFlavor = services['configurations']["admin-properties"]["properties"]["DB_FLAVOR"]
                rangerDbHost = services['configurations']["admin-properties"]["properties"]["db_host"]
                ranger_db_privelege_url_dict = {
                    'MYSQL': {
                        'ranger_privelege_user_jdbc_url': 'jdbc:mysql://' + self.getDBConnectionHostPort(rangerDbFlavor,
                                                                                                         rangerDbHost)},
                    'ORACLE': {
                        'ranger_privelege_user_jdbc_url': 'jdbc:oracle:thin:@' + self.getOracleDBConnectionHostPort(
                            rangerDbFlavor, rangerDbHost, None)},
                    'POSTGRES': {'ranger_privelege_user_jdbc_url': 'jdbc:postgresql://' + self.getDBConnectionHostPort(
                        rangerDbFlavor, rangerDbHost) + '/postgres'},
                    'MSSQL': {'ranger_privelege_user_jdbc_url': 'jdbc:sqlserver://' + self.getDBConnectionHostPort(
                        rangerDbFlavor, rangerDbHost) + ';'},
                    'SQLA': {'ranger_privelege_user_jdbc_url': 'jdbc:sqlanywhere:host=' + self.getDBConnectionHostPort(
                        rangerDbFlavor, rangerDbHost) + ';'}
                }
                rangerPrivelegeDbProperties = ranger_db_privelege_url_dict.get(rangerDbFlavor,
                                                                               ranger_db_privelege_url_dict['MYSQL'])
                for key in rangerPrivelegeDbProperties:
                    putRangerEnvProperty(key, rangerPrivelegeDbProperties.get(key))

        # Recommend ldap settings based on ambari.properties configuration
        if 'ambari-server-properties' in services and \
                        'ambari.ldap.isConfigured' in services['ambari-server-properties'] and \
                        services['ambari-server-properties']['ambari.ldap.isConfigured'].lower() == "true":
            serverProperties = services['ambari-server-properties']
            if 'authentication.ldap.baseDn' in serverProperties:
                putRangerUgsyncSite('ranger.usersync.ldap.searchBase', serverProperties['authentication.ldap.baseDn'])
            if 'authentication.ldap.groupMembershipAttr' in serverProperties:
                putRangerUgsyncSite('ranger.usersync.group.memberattributename',
                                    serverProperties['authentication.ldap.groupMembershipAttr'])
            if 'authentication.ldap.groupNamingAttr' in serverProperties:
                putRangerUgsyncSite('ranger.usersync.group.nameattribute',
                                    serverProperties['authentication.ldap.groupNamingAttr'])
            if 'authentication.ldap.groupObjectClass' in serverProperties:
                putRangerUgsyncSite('ranger.usersync.group.objectclass',
                                    serverProperties['authentication.ldap.groupObjectClass'])
            if 'authentication.ldap.managerDn' in serverProperties:
                putRangerUgsyncSite('ranger.usersync.ldap.binddn', serverProperties['authentication.ldap.managerDn'])
            if 'authentication.ldap.primaryUrl' in serverProperties:
                ldap_protocol = 'ldap://'
                if 'authentication.ldap.useSSL' in serverProperties and serverProperties[
                    'authentication.ldap.useSSL'] == 'true':
                    ldap_protocol = 'ldaps://'
                ldapUrl = ldap_protocol + serverProperties['authentication.ldap.primaryUrl'] if serverProperties[
                    'authentication.ldap.primaryUrl'] else serverProperties['authentication.ldap.primaryUrl']
                putRangerUgsyncSite('ranger.usersync.ldap.url', ldapUrl)
            if 'authentication.ldap.userObjectClass' in serverProperties:
                putRangerUgsyncSite('ranger.usersync.ldap.user.objectclass',
                                    serverProperties['authentication.ldap.userObjectClass'])
            if 'authentication.ldap.usernameAttribute' in serverProperties:
                putRangerUgsyncSite('ranger.usersync.ldap.user.nameattribute',
                                    serverProperties['authentication.ldap.usernameAttribute'])

        # Recommend Ranger Authentication method
        authMap = {
            'org.apache.ranger.unixusersync.process.UnixUserGroupBuilder': 'UNIX',
            'org.apache.ranger.ldapusersync.process.LdapUserGroupBuilder': 'LDAP'
        }

        if 'ranger-ugsync-site' in services['configurations'] and 'ranger.usersync.source.impl.class' in \
                services['configurations']["ranger-ugsync-site"]["properties"]:
            rangerUserSyncClass = services['configurations']["ranger-ugsync-site"]["properties"][
                "ranger.usersync.source.impl.class"]
            if rangerUserSyncClass in authMap:
                rangerSqlConnectorProperty = authMap.get(rangerUserSyncClass)
                putRangerAdminProperty('ranger.authentication.method', rangerSqlConnectorProperty)

        if 'ranger-env' in services['configurations'] and 'is_solrCloud_enabled' in \
                services['configurations']["ranger-env"]["properties"]:
            isSolrCloudEnabled = services['configurations']["ranger-env"]["properties"][
                                     "is_solrCloud_enabled"] == "true"
        else:
            isSolrCloudEnabled = False

        if isSolrCloudEnabled:
            zookeeper_host_port = self.getZKHostPortString(services)
            ranger_audit_zk_port = ''
            if zookeeper_host_port:
                ranger_audit_zk_port = '{0}/{1}'.format(zookeeper_host_port, 'infra-solr')
                putRangerAdminProperty('ranger.audit.solr.zookeepers', ranger_audit_zk_port)
        else:
            putRangerAdminProperty('ranger.audit.solr.zookeepers', 'NONE')

        # Recommend ranger.audit.solr.zookeepers and xasecure.audit.destination.hdfs.dir
        include_hdfs = "HDFS" in servicesList
        if include_hdfs:
            if 'core-site' in services['configurations'] and (
                        'fs.defaultFS' in services['configurations']['core-site']['properties']):
                default_fs = services['configurations']['core-site']['properties']['fs.defaultFS']
                putRangerEnvProperty('xasecure.audit.destination.hdfs.dir',
                                     '{0}/{1}/{2}'.format(default_fs, 'ranger', 'audit'))

        # Recommend Ranger supported service's audit properties
        ranger_services = [
            {'service_name': 'HDFS', 'audit_file': 'ranger-hdfs-audit'},
            {'service_name': 'YARN', 'audit_file': 'ranger-yarn-audit'},
            {'service_name': 'HBASE', 'audit_file': 'ranger-hbase-audit'},
            {'service_name': 'HIVE', 'audit_file': 'ranger-hive-audit'},
            {'service_name': 'KNOX', 'audit_file': 'ranger-knox-audit'},
            {'service_name': 'KAFKA', 'audit_file': 'ranger-kafka-audit'},
            {'service_name': 'STORM', 'audit_file': 'ranger-storm-audit'}
        ]

        for item in range(len(ranger_services)):
            if ranger_services[item]['service_name'] in servicesList:
                component_audit_file = ranger_services[item]['audit_file']
                if component_audit_file in services["configurations"]:
                    ranger_audit_dict = [
                        {'filename': 'ranger-env', 'configname': 'xasecure.audit.destination.db',
                         'target_configname': 'xasecure.audit.destination.db'},
                        {'filename': 'ranger-env', 'configname': 'xasecure.audit.destination.hdfs',
                         'target_configname': 'xasecure.audit.destination.hdfs'},
                        {'filename': 'ranger-env', 'configname': 'xasecure.audit.destination.hdfs.dir',
                         'target_configname': 'xasecure.audit.destination.hdfs.dir'},
                        {'filename': 'ranger-env', 'configname': 'xasecure.audit.destination.solr',
                         'target_configname': 'xasecure.audit.destination.solr'},
                        {'filename': 'ranger-admin-site', 'configname': 'ranger.audit.solr.urls',
                         'target_configname': 'xasecure.audit.destination.solr.urls'},
                        {'filename': 'ranger-admin-site', 'configname': 'ranger.audit.solr.zookeepers',
                         'target_configname': 'xasecure.audit.destination.solr.zookeepers'}
                    ]
                    putRangerAuditProperty = self.putProperty(configurations, component_audit_file, services)

                    for item in ranger_audit_dict:
                        if item['filename'] in services["configurations"] and item['configname'] in \
                                services["configurations"][item['filename']]["properties"]:
                            if item['filename'] in configurations and item['configname'] in \
                                    configurations[item['filename']]["properties"]:
                                rangerAuditProperty = configurations[item['filename']]["properties"][item['configname']]
                            else:
                                rangerAuditProperty = services["configurations"][item['filename']]["properties"][
                                    item['configname']]
                            putRangerAuditProperty(item['target_configname'], rangerAuditProperty)

        audit_solr_flag = 'false'
        audit_db_flag = 'false'
        ranger_audit_source_type = 'solr'
        if 'ranger-env' in services['configurations'] and 'xasecure.audit.destination.solr' in \
                services['configurations']["ranger-env"]["properties"]:
            audit_solr_flag = services['configurations']["ranger-env"]["properties"]['xasecure.audit.destination.solr']
        if 'ranger-env' in services['configurations'] and 'xasecure.audit.destination.db' in \
                services['configurations']["ranger-env"]["properties"]:
            audit_db_flag = services['configurations']["ranger-env"]["properties"]['xasecure.audit.destination.db']

        if audit_db_flag == 'true' and audit_solr_flag == 'false':
            ranger_audit_source_type = 'db'
        putRangerAdminProperty('ranger.audit.source.type', ranger_audit_source_type)

        knox_host = 'localhost'
        knox_port = '8443'
        if 'KNOX' in servicesList:
            knox_hosts = self.getComponentHostNames(services, "KNOX", "KNOX_GATEWAY")
            if len(knox_hosts) > 0:
                knox_hosts.sort()
                knox_host = knox_hosts[0]
            if 'gateway-site' in services['configurations'] and 'gateway.port' in \
                    services['configurations']["gateway-site"]["properties"]:
                knox_port = services['configurations']["gateway-site"]["properties"]['gateway.port']
            putRangerAdminProperty('ranger.sso.providerurl',
                                   'https://{0}:{1}/gateway/knoxsso/api/v1/websso'.format(knox_host, knox_port))

        required_services = [
            {'service_name': 'HDFS', 'config_type': 'ranger-hdfs-security'},
            {'service_name': 'YARN', 'config_type': 'ranger-yarn-security'},
            {'service_name': 'HBASE', 'config_type': 'ranger-hbase-security'},
            {'service_name': 'HIVE', 'config_type': 'ranger-hive-security'},
            # {'service_name': 'KNOX', 'config_type': 'ranger-knox-security'},
            {'service_name': 'KAFKA', 'config_type': 'ranger-kafka-security'},
            # {'service_name': 'RANGER_KMS','config_type': 'ranger-kms-security'},
            {'service_name': 'STORM', 'config_type': 'ranger-storm-security'}
        ]

        # Build policymgr_external_url
        protocol = 'http'
        ranger_admin_host = 'localhost'
        port = '6080'

        # Check if http is disabled. For HDP-2.3 this can be checked in ranger-admin-site/ranger.service.http.enabled
        # For Ranger-0.4.0 this can be checked in ranger-site/http.enabled
        if ('ranger-site' in services['configurations'] and 'http.enabled' in services['configurations']['ranger-site'][
            'properties'] \
                    and services['configurations']['ranger-site']['properties']['http.enabled'].lower() == 'false') or \
                ('ranger-admin-site' in services['configurations'] and 'ranger.service.http.enabled' in
                    services['configurations']['ranger-admin-site']['properties'] \
                         and services['configurations']['ranger-admin-site']['properties'][
                        'ranger.service.http.enabled'].lower() == 'false'):
            # HTTPS protocol is used
            protocol = 'https'
            # Starting Ranger-0.5.0.2.3 port stored in ranger-admin-site ranger.service.https.port
            if 'ranger-admin-site' in services['configurations'] and \
                            'ranger.service.https.port' in services['configurations']['ranger-admin-site'][
                        'properties']:
                port = services['configurations']['ranger-admin-site']['properties']['ranger.service.https.port']
            # In Ranger-0.4.0 port stored in ranger-site https.service.port
            elif 'ranger-site' in services['configurations'] and \
                            'https.service.port' in services['configurations']['ranger-site']['properties']:
                port = services['configurations']['ranger-site']['properties']['https.service.port']
        else:
            # HTTP protocol is used
            # Starting Ranger-0.5.0.2.3 port stored in ranger-admin-site ranger.service.http.port
            if 'ranger-admin-site' in services['configurations'] and \
                            'ranger.service.http.port' in services['configurations']['ranger-admin-site']['properties']:
                port = services['configurations']['ranger-admin-site']['properties']['ranger.service.http.port']
            # In Ranger-0.4.0 port stored in ranger-site http.service.port
            elif 'ranger-site' in services['configurations'] and \
                            'http.service.port' in services['configurations']['ranger-site']['properties']:
                port = services['configurations']['ranger-site']['properties']['http.service.port']

        ranger_admin_hosts = self.getComponentHostNames(services, "RANGER", "RANGER_ADMIN")
        if ranger_admin_hosts:
            if len(ranger_admin_hosts) > 1 \
                    and services['configurations'] \
                    and 'admin-properties' in services['configurations'] and 'policymgr_external_url' in \
                    services['configurations']['admin-properties']['properties'] \
                    and services['configurations']['admin-properties']['properties']['policymgr_external_url'] \
                    and services['configurations']['admin-properties']['properties']['policymgr_external_url'].strip():

                # in case of HA deployment keep the policymgr_external_url specified in the config
                policymgr_external_url = services['configurations']['admin-properties']['properties'][
                    'policymgr_external_url']
            else:

                ranger_admin_host = ranger_admin_hosts[0]
                policymgr_external_url = "%s://%s:%s" % (protocol, ranger_admin_host, port)

            putRangerAdminProperty('policymgr_external_url', policymgr_external_url)
        # recommendation for ranger url for ranger-supported plugins
        self.recommendRangerUrlConfigurations(configurations, services, required_services)

        cluster_env = getServicesSiteProperties(services, "cluster-env")
        security_enabled = cluster_env is not None and "security_enabled" in cluster_env and \
                           cluster_env["security_enabled"].lower() == "true"
        if "ranger-env" in configurations and not security_enabled:
            putRangerEnvProperty("ranger-storm-plugin-enabled", "No")

        has_ranger_tagsync = False
        putTagsyncAppProperty = self.putProperty(configurations, "tagsync-application-properties", services)
        putTagsyncSiteProperty = self.putProperty(configurations, "ranger-tagsync-site", services)

        application_properties = self.getServicesSiteProperties(services, "application-properties")

        ranger_tagsync_host = self.getHostsForComponent(services, "RANGER", "RANGER_TAGSYNC")
        has_ranger_tagsync = len(ranger_tagsync_host) > 0

        if 'ATLAS' in servicesList and has_ranger_tagsync:
            atlas_hosts = self.getHostNamesWithComponent("ATLAS", "ATLAS_SERVER", services)
            atlas_host = 'localhost' if len(atlas_hosts) == 0 else atlas_hosts[0]
            protocol = 'http'
            atlas_port = '21000'

            if application_properties and 'atlas.enableTLS' in application_properties and application_properties[
                'atlas.enableTLS'].lower() == 'true':
                protocol = 'https'
                if 'atlas.server.https.port' in application_properties:
                    atlas_port = application_properties['atlas.server.https.port']
            else:
                protocol = 'http'
                if application_properties and 'atlas.server.http.port' in application_properties:
                    atlas_port = application_properties['atlas.server.http.port']

            atlas_rest_endpoint = '{0}://{1}:{2}'.format(protocol, atlas_host, atlas_port)

            putTagsyncSiteProperty('ranger.tagsync.source.atlas', 'true')
            putTagsyncSiteProperty('ranger.tagsync.source.atlasrest.endpoint', atlas_rest_endpoint)

        zookeeper_host_port = self.getZKHostPortString(services)
        if zookeeper_host_port and has_ranger_tagsync:
            putTagsyncAppProperty('atlas.kafka.zookeeper.connect', zookeeper_host_port)

        if 'KAFKA' in servicesList and has_ranger_tagsync:
            kafka_hosts = self.getHostNamesWithComponent("KAFKA", "KAFKA_BROKER", services)
            kafka_port = '6667'
            if 'kafka-broker' in services['configurations'] and (
                        'port' in services['configurations']['kafka-broker']['properties']):
                kafka_port = services['configurations']['kafka-broker']['properties']['port']
            kafka_host_port = []
            for i in range(len(kafka_hosts)):
                kafka_host_port.append(kafka_hosts[i] + ':' + kafka_port)

            final_kafka_host = ",".join(kafka_host_port)
            putTagsyncAppProperty('atlas.kafka.bootstrap.servers', final_kafka_host)

        is_solr_cloud_enabled = False
        if 'ranger-env' in services['configurations'] and 'is_solrCloud_enabled' in \
                services['configurations']['ranger-env']['properties']:
            is_solr_cloud_enabled = services['configurations']['ranger-env']['properties'][
                                        'is_solrCloud_enabled'] == 'true'

        is_external_solr_cloud_enabled = False
        if 'ranger-env' in services['configurations'] and 'is_external_solrCloud_enabled' in \
                services['configurations']['ranger-env']['properties']:
            is_external_solr_cloud_enabled = services['configurations']['ranger-env']['properties'][
                                                 'is_external_solrCloud_enabled'] == 'true'

        ranger_audit_zk_port = ''

        if 'AMBARI_INFRA' in servicesList and zookeeper_host_port and is_solr_cloud_enabled and not is_external_solr_cloud_enabled:
            zookeeper_host_port = zookeeper_host_port.split(',')
            zookeeper_host_port.sort()
            zookeeper_host_port = ",".join(zookeeper_host_port)
            infra_solr_znode = '/infra-solr'

            if 'infra-solr-env' in services['configurations'] and \
                    ('infra_solr_znode' in services['configurations']['infra-solr-env']['properties']):
                infra_solr_znode = services['configurations']['infra-solr-env']['properties']['infra_solr_znode']
                ranger_audit_zk_port = '{0}{1}'.format(zookeeper_host_port, infra_solr_znode)
            putRangerAdminProperty('ranger.audit.solr.zookeepers', ranger_audit_zk_port)
        elif zookeeper_host_port and is_solr_cloud_enabled and is_external_solr_cloud_enabled:
            ranger_audit_zk_port = '{0}/{1}'.format(zookeeper_host_port, 'infra-solr')
            putRangerAdminProperty('ranger.audit.solr.zookeepers', ranger_audit_zk_port)
        else:
            putRangerAdminProperty('ranger.audit.solr.zookeepers', 'NONE')

        ranger_services = [
            {'service_name': 'HDFS', 'audit_file': 'ranger-hdfs-audit'},
            {'service_name': 'YARN', 'audit_file': 'ranger-yarn-audit'},
            {'service_name': 'HBASE', 'audit_file': 'ranger-hbase-audit'},
            {'service_name': 'HIVE', 'audit_file': 'ranger-hive-audit'},
            {'service_name': 'KNOX', 'audit_file': 'ranger-knox-audit'},
            {'service_name': 'KAFKA', 'audit_file': 'ranger-kafka-audit'},
            {'service_name': 'STORM', 'audit_file': 'ranger-storm-audit'},
            {'service_name': 'RANGER_KMS', 'audit_file': 'ranger-kms-audit'},
            {'service_name': 'ATLAS', 'audit_file': 'ranger-atlas-audit'}
        ]

        for item in range(len(ranger_services)):
            if ranger_services[item]['service_name'] in servicesList:
                component_audit_file = ranger_services[item]['audit_file']
                if component_audit_file in services["configurations"]:
                    ranger_audit_dict = [
                        {'filename': 'ranger-admin-site', 'configname': 'ranger.audit.solr.urls',
                         'target_configname': 'xasecure.audit.destination.solr.urls'},
                        {'filename': 'ranger-admin-site', 'configname': 'ranger.audit.solr.zookeepers',
                         'target_configname': 'xasecure.audit.destination.solr.zookeepers'}
                    ]
                    putRangerAuditProperty = self.putProperty(configurations, component_audit_file, services)

                    for item in ranger_audit_dict:
                        if item['filename'] in services["configurations"] and item['configname'] in \
                                services["configurations"][item['filename']]["properties"]:
                            if item['filename'] in configurations and item['configname'] in \
                                    configurations[item['filename']]["properties"]:
                                rangerAuditProperty = configurations[item['filename']]["properties"][item['configname']]
                            else:
                                rangerAuditProperty = services["configurations"][item['filename']]["properties"][
                                    item['configname']]
                            putRangerAuditProperty(item['target_configname'], rangerAuditProperty)

        if "HDFS" in servicesList:
            hdfs_user = None
            if "hadoop-env" in services["configurations"] and "hdfs_user" in services["configurations"]["hadoop-env"][
                "properties"]:
                hdfs_user = services["configurations"]["hadoop-env"]["properties"]["hdfs_user"]
                putRangerAdminProperty('ranger.kms.service.user.hdfs', hdfs_user)

        if "HIVE" in servicesList:
            hive_user = None
            if "hive-env" in services["configurations"] and "hive_user" in services["configurations"]["hive-env"][
                "properties"]:
                hive_user = services["configurations"]["hive-env"]["properties"]["hive_user"]
                putRangerAdminProperty('ranger.kms.service.user.hive', hive_user)

        ranger_plugins_serviceuser = [
            {'service_name': 'HDFS', 'file_name': 'hadoop-env', 'config_name': 'hdfs_user',
             'target_configname': 'ranger.plugins.hdfs.serviceuser'},
            {'service_name': 'HIVE', 'file_name': 'hive-env', 'config_name': 'hive_user',
             'target_configname': 'ranger.plugins.hive.serviceuser'},
            {'service_name': 'YARN', 'file_name': 'yarn-env', 'config_name': 'yarn_user',
             'target_configname': 'ranger.plugins.yarn.serviceuser'},
            {'service_name': 'HBASE', 'file_name': 'hbase-env', 'config_name': 'hbase_user',
             'target_configname': 'ranger.plugins.hbase.serviceuser'},
            {'service_name': 'KNOX', 'file_name': 'knox-env', 'config_name': 'knox_user',
             'target_configname': 'ranger.plugins.knox.serviceuser'},
            {'service_name': 'STORM', 'file_name': 'storm-env', 'config_name': 'storm_user',
             'target_configname': 'ranger.plugins.storm.serviceuser'},
            {'service_name': 'KAFKA', 'file_name': 'kafka-env', 'config_name': 'kafka_user',
             'target_configname': 'ranger.plugins.kafka.serviceuser'},
            {'service_name': 'RANGER_KMS', 'file_name': 'kms-env', 'config_name': 'kms_user',
             'target_configname': 'ranger.plugins.kms.serviceuser'},
            {'service_name': 'ATLAS', 'file_name': 'atlas-env', 'config_name': 'metadata_user',
             'target_configname': 'ranger.plugins.atlas.serviceuser'}
        ]

        for item in range(len(ranger_plugins_serviceuser)):
            if ranger_plugins_serviceuser[item]['service_name'] in servicesList:
                file_name = ranger_plugins_serviceuser[item]['file_name']
                config_name = ranger_plugins_serviceuser[item]['config_name']
                target_configname = ranger_plugins_serviceuser[item]['target_configname']
                if file_name in services["configurations"] and config_name in services["configurations"][file_name][
                    "properties"]:
                    service_user = services["configurations"][file_name]["properties"][config_name]
                    putRangerAdminProperty(target_configname, service_user)

        if "ATLAS" in servicesList:
            if "ranger-env" in services["configurations"]:
                putAtlasRangerAuditProperty = self.putProperty(configurations, 'ranger-atlas-audit', services)
                xasecure_audit_destination_hdfs = ''
                xasecure_audit_destination_hdfs_dir = ''
                xasecure_audit_destination_solr = ''
                if 'xasecure.audit.destination.hdfs' in configurations['ranger-env']['properties']:
                    xasecure_audit_destination_hdfs = configurations['ranger-env']['properties'][
                        'xasecure.audit.destination.hdfs']
                else:
                    xasecure_audit_destination_hdfs = services['configurations']['ranger-env']['properties'][
                        'xasecure.audit.destination.hdfs']

                if 'core-site' in services['configurations'] and (
                            'fs.defaultFS' in services['configurations']['core-site']['properties']):
                    xasecure_audit_destination_hdfs_dir = '{0}/{1}/{2}'.format(
                        services['configurations']['core-site']['properties']['fs.defaultFS'], 'ranger', 'audit')

                if 'xasecure.audit.destination.solr' in configurations['ranger-env']['properties']:
                    xasecure_audit_destination_solr = configurations['ranger-env']['properties'][
                        'xasecure.audit.destination.solr']
                else:
                    xasecure_audit_destination_solr = services['configurations']['ranger-env']['properties'][
                        'xasecure.audit.destination.solr']

                putAtlasRangerAuditProperty('xasecure.audit.destination.hdfs', xasecure_audit_destination_hdfs)
                putAtlasRangerAuditProperty('xasecure.audit.destination.hdfs.dir', xasecure_audit_destination_hdfs_dir)
                putAtlasRangerAuditProperty('xasecure.audit.destination.solr', xasecure_audit_destination_solr)
        required_services = [
            {'service_name': 'ATLAS', 'config_type': 'ranger-atlas-security'}
        ]

        delta_sync_enabled = False
        if 'ranger-ugsync-site' in services['configurations'] and 'ranger.usersync.ldap.deltasync' in \
                services['configurations']['ranger-ugsync-site']['properties']:
            delta_sync_enabled = services['configurations']['ranger-ugsync-site']['properties'][
                                     'ranger.usersync.ldap.deltasync'] == "true"

        if delta_sync_enabled:
            putRangerUgsyncSite("ranger.usersync.group.searchenabled", "true")
        else:
            putRangerUgsyncSite("ranger.usersync.group.searchenabled", "false")

    def recommendRangerUrlConfigurations(self, configurations, services, requiredServices):
        servicesList = [service["StackServices"]["service_name"] for service in services["services"]]

        policymgr_external_url = ""
        if 'admin-properties' in services['configurations'] and 'policymgr_external_url' in \
                services['configurations']['admin-properties']['properties']:
            if 'admin-properties' in configurations and 'policymgr_external_url' in configurations['admin-properties'][
                'properties']:
                policymgr_external_url = configurations['admin-properties']['properties']['policymgr_external_url']
            else:
                policymgr_external_url = services['configurations']['admin-properties']['properties'][
                    'policymgr_external_url']

        for index in range(len(requiredServices)):
            if requiredServices[index]['service_name'] in servicesList:
                component_config_type = requiredServices[index]['config_type']
                component_name = requiredServices[index]['service_name']
                component_config_property = 'ranger.plugin.{0}.policy.rest.url'.format(component_name.lower())
                if requiredServices[index]['service_name'] == 'RANGER_KMS':
                    component_config_property = 'ranger.plugin.kms.policy.rest.url'
                putRangerSecurityProperty = self.putProperty(configurations, component_config_type, services)
                if component_config_type in services["configurations"] and component_config_property in \
                        services["configurations"][component_config_type]["properties"]:
                    putRangerSecurityProperty(component_config_property, policymgr_external_url)

    def recommendHadoopProxyUsers(self, configurations, services, hosts):
        servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
        users = {}

        if 'forced-configurations' not in services:
            services["forced-configurations"] = []

        if "HDFS" in servicesList:
            hdfs_user = None
            if "hadoop-env" in services["configurations"] and "hdfs_user" in services["configurations"]["hadoop-env"][
                "properties"]:
                hdfs_user = services["configurations"]["hadoop-env"]["properties"]["hdfs_user"]
                if not hdfs_user in users and hdfs_user is not None:
                    users[hdfs_user] = {"propertyHosts": "*", "propertyGroups": "*", "config": "hadoop-env",
                                        "propertyName": "hdfs_user"}

        if "OOZIE" in servicesList:
            oozie_user = None
            if "oozie-env" in services["configurations"] and "oozie_user" in services["configurations"]["oozie-env"][
                "properties"]:
                oozie_user = services["configurations"]["oozie-env"]["properties"]["oozie_user"]
                oozieServerHostsNameSet = set()
                oozieServerHosts = self.getHostsWithComponent("OOZIE", "OOZIE_SERVER", services, hosts)
                if oozieServerHosts is not None:
                    for oozieServerHost in oozieServerHosts:
                        oozieServerHostsNameSet.add(oozieServerHost["Hosts"]["host_name"])
                    oozieServerHostsNames = ",".join(sorted(oozieServerHostsNameSet))
                    if not oozie_user in users and oozie_user is not None:
                        users[oozie_user] = {"propertyHosts": oozieServerHostsNames, "propertyGroups": "*",
                                             "config": "oozie-env", "propertyName": "oozie_user"}

        hive_user = None
        if "HIVE" in servicesList:
            webhcat_user = None
            if "hive-env" in services["configurations"] and "hive_user" in services["configurations"]["hive-env"][
                "properties"] \
                    and "webhcat_user" in services["configurations"]["hive-env"]["properties"]:
                hive_user = services["configurations"]["hive-env"]["properties"]["hive_user"]
                webhcat_user = services["configurations"]["hive-env"]["properties"]["webhcat_user"]
                hiveServerHosts = self.getHostsWithComponent("HIVE", "HIVE_SERVER", services, hosts)
                hiveServerInteractiveHosts = self.getHostsWithComponent("HIVE", "HIVE_SERVER_INTERACTIVE", services,
                                                                        hosts)
                webHcatServerHosts = self.getHostsWithComponent("HIVE", "WEBHCAT_SERVER", services, hosts)

                if hiveServerHosts is not None:
                    hiveServerHostsNameSet = set()
                    for hiveServerHost in hiveServerHosts:
                        hiveServerHostsNameSet.add(hiveServerHost["Hosts"]["host_name"])
                    # Append Hive Server Interactive host as well, as it is Hive2/HiveServer2 component.
                    if hiveServerInteractiveHosts:
                        for hiveServerInteractiveHost in hiveServerInteractiveHosts:
                            hiveServerInteractiveHostName = hiveServerInteractiveHost["Hosts"]["host_name"]
                            if hiveServerInteractiveHostName not in hiveServerHostsNameSet:
                                hiveServerHostsNameSet.add(hiveServerInteractiveHostName)
                                Logger.info(
                                    "Appended (if not exiting), Hive Server Interactive Host : '{0}', to Hive Server Host List : '{1}'".format(
                                        hiveServerInteractiveHostName, hiveServerHostsNameSet))

                    hiveServerHostsNames = ",".join(
                        sorted(hiveServerHostsNameSet))  # includes Hive Server interactive host also.
                    Logger.info("Hive Server and Hive Server Interactive (if enabled) Host List : {0}".format(
                        hiveServerHostsNameSet))
                    if not hive_user in users and hive_user is not None:
                        users[hive_user] = {"propertyHosts": hiveServerHostsNames, "propertyGroups": "*",
                                            "config": "hive-env", "propertyName": "hive_user"}

                if webHcatServerHosts is not None:
                    webHcatServerHostsNameSet = set()
                    for webHcatServerHost in webHcatServerHosts:
                        webHcatServerHostsNameSet.add(webHcatServerHost["Hosts"]["host_name"])
                    webHcatServerHostsNames = ",".join(sorted(webHcatServerHostsNameSet))
                    if not webhcat_user in users and webhcat_user is not None:
                        # not install webcat use *
                        users[webhcat_user] = {"propertyHosts": '*', "propertyGroups": "*",
                                               "config": "hive-env", "propertyName": "webhcat_user"}

        if "YARN" in servicesList:
            yarn_user = None
            if "yarn-env" in services["configurations"] and "yarn_user" in services["configurations"]["yarn-env"][
                "properties"]:
                yarn_user = services["configurations"]["yarn-env"]["properties"]["yarn_user"]
                rmHosts = self.getHostsWithComponent("YARN", "RESOURCEMANAGER", services, hosts)

                if len(rmHosts) > 1:
                    rmHostsNameSet = set()
                    for rmHost in rmHosts:
                        rmHostsNameSet.add(rmHost["Hosts"]["host_name"])
                    rmHostsNames = ",".join(sorted(rmHostsNameSet))
                    if not yarn_user in users and yarn_user is not None:
                        users[yarn_user] = {"propertyHosts": rmHostsNames, "config": "yarn-env",
                                            "propertyName": "yarn_user"}

        if "FALCON" in servicesList:
            falconUser = None
            if "falcon-env" in services["configurations"] and "falcon_user" in services["configurations"]["falcon-env"][
                "properties"]:
                falconUser = services["configurations"]["falcon-env"]["properties"]["falcon_user"]
                if not falconUser in users and falconUser is not None:
                    users[falconUser] = {"propertyHosts": "*", "propertyGroups": "*", "config": "falcon-env",
                                         "propertyName": "falcon_user"}

        if "SPARK" in servicesList:
            livyUser = None
            if "livy-env" in services["configurations"] and "livy_user" in services["configurations"]["livy-env"][
                "properties"]:
                livyUser = services["configurations"]["livy-env"]["properties"]["livy_user"]
                if not livyUser in users and livyUser is not None:
                    users[livyUser] = {"propertyHosts": "*", "propertyGroups": "*", "config": "livy-env",
                                       "propertyName": "livy_user"}

        putCoreSiteProperty = self.putProperty(configurations, "core-site", services)
        putCoreSitePropertyAttribute = self.putPropertyAttribute(configurations, "core-site")

        for user_name, user_properties in users.iteritems():
            if hive_user and hive_user == user_name:
                if "propertyHosts" in user_properties:
                    services["forced-configurations"].append(
                        {"type": "core-site", "name": "hadoop.proxyuser.{0}.hosts".format(hive_user)})
            # Add properties "hadoop.proxyuser.*.hosts", "hadoop.proxyuser.*.groups" to core-site for all users
            self.put_proxyuser_value(user_name, user_properties["propertyHosts"], services=services,
                                     configurations=configurations, put_function=putCoreSiteProperty)
            Logger.info(
                "Updated hadoop.proxyuser.{0}.hosts as : {1}".format(hive_user, user_properties["propertyHosts"]))
            if "propertyGroups" in user_properties:
                self.put_proxyuser_value(user_name, user_properties["propertyGroups"], is_groups=True,
                                         services=services, configurations=configurations,
                                         put_function=putCoreSiteProperty)

            # Remove old properties if user was renamed
            userOldValue = self.getOldValue(services, user_properties["config"], user_properties["propertyName"])
            if userOldValue is not None and userOldValue != user_name:
                putCoreSitePropertyAttribute("hadoop.proxyuser.{0}.hosts".format(userOldValue), 'delete', 'true')
                services["forced-configurations"].append(
                    {"type": "core-site", "name": "hadoop.proxyuser.{0}.hosts".format(userOldValue)})
                services["forced-configurations"].append(
                    {"type": "core-site", "name": "hadoop.proxyuser.{0}.hosts".format(user_name)})

                if "propertyGroups" in user_properties:
                    putCoreSitePropertyAttribute("hadoop.proxyuser.{0}.groups".format(userOldValue), 'delete', 'true')
                    services["forced-configurations"].append(
                        {"type": "core-site", "name": "hadoop.proxyuser.{0}.groups".format(userOldValue)})
                    services["forced-configurations"].append(
                        {"type": "core-site", "name": "hadoop.proxyuser.{0}.groups".format(user_name)})
        self.recommendAmbariProxyUsersForHDFS(services, configurations, servicesList, putCoreSiteProperty,
                                              putCoreSitePropertyAttribute)

    def recommendAmbariProxyUsersForHDFS(self, services, configurations, servicesList, putCoreSiteProperty,
                                         putCoreSitePropertyAttribute):
        if "HDFS" in servicesList:
            ambari_user = self.getAmbariUser(services)
            ambariHostName = socket.getfqdn()

            self.put_proxyuser_value(ambari_user, ambariHostName, services=services, configurations=configurations,
                                     put_function=putCoreSiteProperty)
            self.put_proxyuser_value(ambari_user, "*", is_groups=True, services=services, configurations=configurations,
                                     put_function=putCoreSiteProperty)

            old_ambari_user = self.getOldAmbariUser(services)
            if old_ambari_user is not None:
                putCoreSitePropertyAttribute("hadoop.proxyuser.{0}.hosts".format(old_ambari_user), 'delete', 'true')
                putCoreSitePropertyAttribute("hadoop.proxyuser.{0}.groups".format(old_ambari_user), 'delete', 'true')

    def put_proxyuser_value(self, user_name, value, is_groups=False, services=None, configurations=None,
                            put_function=None):
        is_wildcard_value, current_value = self.get_data_for_proxyuser(user_name, services, configurations, is_groups)
        result_value = "*"
        result_values_set = self.merge_proxyusers_values(current_value, value)
        if len(result_values_set) > 0:
            result_value = ",".join(sorted([val for val in result_values_set if val]))

        if is_groups:
            property_name = "hadoop.proxyuser.{0}.groups".format(user_name)
        else:
            property_name = "hadoop.proxyuser.{0}.hosts".format(user_name)

        put_function(property_name, result_value)

    def merge_proxyusers_values(self, first, second):
        result = set()

        def append(data):
            if isinstance(data, str) or isinstance(data, unicode):
                if data != "*":
                    result.update(data.split(","))
            else:
                result.update(data)

        append(first)
        append(second)
        return result

    def get_data_for_proxyuser(self, user_name, services, configurations, groups=False):
        """
        Returns values of proxyuser properties for given user. Properties can be
        hadoop.proxyuser.username.groups or hadoop.proxyuser.username.hosts

        :param user_name:
        :param services:
        :param configurations:
        :param groups: if true, will return values for group property, not hosts
        :return: tuple (wildcard_value, set[values]), where wildcard_value indicates if property value was *
        """
        if "core-site" in services["configurations"]:
            coreSite = services["configurations"]["core-site"]['properties']
        else:
            coreSite = {}
        if groups:
            property_name = "hadoop.proxyuser.{0}.groups".format(user_name)
        else:
            property_name = "hadoop.proxyuser.{0}.hosts".format(user_name)
        if property_name in coreSite:
            property_value = coreSite[property_name]
            if property_value == "*":
                return True, set()
            else:
                property_value, replacement_map = self.preserve_special_values(property_value)
                result_values = set([v.strip() for v in property_value.split(",")])
                if "core-site" in configurations:
                    if property_name in configurations["core-site"]['properties']:
                        additional_value, additional_replacement_map = self.preserve_special_values(
                            configurations["core-site"]['properties'][property_name])
                        replacement_map.update(additional_replacement_map)
                        result_values = result_values.union([v.strip() for v in additional_value.split(",")])
                self.restore_special_values(result_values, replacement_map)
                return False, result_values
        return False, set()

    def getOldAmbariUser(self, services):
        ambari_user = None
        if "cluster-env" in services["configurations"]:
            if "security_enabled" in services["configurations"]["cluster-env"]["properties"] \
                    and services["configurations"]["cluster-env"]["properties"]["security_enabled"].lower() == "true":
                ambari_user = services['ambari-server-properties']['ambari-server.user']
            elif "ambari_principal_name" in services["configurations"]["cluster-env"]["properties"]:
                ambari_user = services["configurations"]["cluster-env"]["properties"]["ambari_principal_name"]
                ambari_user = ambari_user.split('@')[0]
        return ambari_user

    PROXYUSER_SPECIAL_RE = [r"\$\{(?:([\w\-\.]+)/)?([\w\-\.]+)(?:\s*\|\s*(.+?))?\}"]

    @classmethod
    def preserve_special_values(cls, value):
        """
        Replace matches of PROXYUSER_SPECIAL_RE with random strings.

        :param value: input string
        :return: result string and dictionary that contains mapping random string to original value
        """

        def gen_random_str():
            return ''.join(random.choice(string.digits + string.ascii_letters) for _ in range(20))

        result = value
        replacements_dict = {}
        for regexp in cls.PROXYUSER_SPECIAL_RE:
            for match in re.finditer(regexp, value):
                matched_string = match.string[match.start():match.end()]
                rand_str = gen_random_str()
                result = result.replace(matched_string, rand_str)
                replacements_dict[rand_str] = matched_string
        return result, replacements_dict

    @staticmethod
    def restore_special_values(data, replacement_dict):
        """
        Replace random strings in data set to their original values using replacement_dict.

        :param data:
        :param replacement_dict:
        :return:
        """
        for replacement, original in replacement_dict.iteritems():
            data.remove(replacement)
            data.add(original)

    def getOldValue(self, services, configType, propertyName):
        if services:
            if 'changed-configurations' in services.keys():
                changedConfigs = services["changed-configurations"]
                for changedConfig in changedConfigs:
                    if changedConfig["type"] == configType and changedConfig[
                        "name"] == propertyName and "old_value" in changedConfig:
                        return changedConfig["old_value"]
        return None

    def recommendFalconConfigurations(self, configurations, clusterData, services, hosts):
        falcon_mounts = [
            ("*.falcon.graph.storage.directory", "FALCON_SERVER", "/hadoop/falcon/data/lineage/graphdb", "single")
        ]
        self.updateMountProperties("falcon-startup.properties", falcon_mounts, configurations, services, hosts)
        putFalconEnvProperty = self.putProperty(configurations, "falcon-env", services)
        enable_atlas_hook = False
        servicesList = [service["StackServices"]["service_name"] for service in services["services"]]

        if "ATLAS" in servicesList:
            putFalconEnvProperty("falcon.atlas.hook", "true")
        else:
            putFalconEnvProperty("falcon.atlas.hook", "false")

    def getAmbariUser(self, services):
        ambari_user = services['ambari-server-properties']['ambari-server.user']
        if "cluster-env" in services["configurations"] \
                and "ambari_principal_name" in services["configurations"]["cluster-env"]["properties"] \
                and "security_enabled" in services["configurations"]["cluster-env"]["properties"] \
                and services["configurations"]["cluster-env"]["properties"]["security_enabled"].lower() == "true":
            ambari_user = services["configurations"]["cluster-env"]["properties"]["ambari_principal_name"]
            ambari_user = ambari_user.split('@')[0]
        return ambari_user

    def recommendTitanConfigurations(self, configurations, clusterData, services, hosts):
        putTitanPropertyAttribute = self.putPropertyAttribute(configurations, "titan-env")
        servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
        knox_enabled = "KNOX" in servicesList
        if knox_enabled:
            putTitanPropertyAttribute("SimpleAuthenticator", "visible", "false")

    def recommendSolrConfigurations(self, configurations, clusterData, services, hosts):
        putSolrEnvProperty = self.putProperty(configurations, "solr-env", services)
        # Update ranger-solr-plugin-properties/ranger-solr-plugin-enabled to match ranger-env/ranger-solr-plugin-enabled
        if "ranger-env" in services["configurations"] \
                and "ranger-solr-plugin-properties" in services["configurations"] \
                and "ranger-solr-plugin-enabled" in services["configurations"]["ranger-env"]["properties"]:
            putSolrRangerPluginProperty = self.putProperty(configurations, "ranger-solr-plugin-properties", services)
            ranger_solr_plugin_enabled = services["configurations"]["ranger-env"]["properties"][
                "ranger-solr-plugin-enabled"]
            putSolrRangerPluginProperty("ranger-solr-plugin-enabled", ranger_solr_plugin_enabled)

        # Determine if the Ranger/Solr Plugin is enabled
        servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
        ranger_plugin_enabled = "RANGER" in servicesList
        # Only if the RANGER service is installed....
        if ranger_plugin_enabled:
            if 'ranger-solr-plugin-properties' in configurations and \
                            'ranger-solr-plugin-enabled' in configurations['ranger-solr-plugin-properties'][
                        'properties']:
                ranger_plugin_enabled = configurations['ranger-solr-plugin-properties']['properties'][
                                            'ranger-solr-plugin-enabled'].lower() == 'yes'
            elif 'ranger-solr-plugin-properties' in services['configurations'] and \
                            'ranger-solr-plugin-enabled' in services['configurations']['ranger-solr-plugin-properties'][
                        'properties']:
                ranger_plugin_enabled = services['configurations']['ranger-solr-plugin-properties']['properties'][
                                            'ranger-solr-plugin-enabled'].lower() == 'yes'

    def updateLlapConfigs(self, configurations, services, hosts, llap_queue_name):
        self.logger.info("DBG: Entered updateLlapConfigs")

        # Determine if we entered here during cluster creation.
        operation = getUserOperationContext(services, "operation")
        is_cluster_create_opr = False
        if operation == self.CLUSTER_CREATE_OPERATION:
            is_cluster_create_opr = True
        self.logger.info("Is cluster create operation ? = {0}".format(is_cluster_create_opr))

        putHiveInteractiveSiteProperty = self.putProperty(configurations, 'hive-interactive-site', services)
        putHiveInteractiveSitePropertyAttribute = self.putPropertyAttribute(configurations, 'hive-interactive-site')
        putHiveInteractiveEnvProperty = self.putProperty(configurations, "hive-interactive-env", services)
        putHiveInteractiveEnvPropertyAttribute = self.putPropertyAttribute(configurations, "hive-interactive-env")
        putTezInteractiveSiteProperty = self.putProperty(configurations, "tez-interactive-site", services)
        llap_daemon_selected_queue_name = None
        selected_queue_is_ambari_managed_llap = None  # Queue named 'llap' at root level is Ambari managed.
        llap_selected_queue_am_percent = None
        DEFAULT_EXECUTOR_TO_AM_RATIO = 20
        MIN_EXECUTOR_TO_AM_RATIO = 10
        MAX_CONCURRENT_QUERIES = 32
        MAX_CONCURRENT_QUERIES_SMALL_CLUSTERS = 4 # Concurrency for clusters with <10 executors
        leafQueueNames = None
        MB_TO_BYTES = 1048576
        hsi_site = self.getServicesSiteProperties(services, 'hive-interactive-site')
        yarn_site = self.getServicesSiteProperties(services, "yarn-site")
        min_memory_required = 0

        # Update 'hive.llap.daemon.queue.name' prop combo entries
        self.setLlapDaemonQueuePropAttributes(services, configurations)

        if not services["changed-configurations"]:
            read_llap_daemon_yarn_cont_mb = long(self.get_yarn_min_container_size(services, configurations))
            putHiveInteractiveSiteProperty("hive.llap.daemon.yarn.container.mb", read_llap_daemon_yarn_cont_mb)

        if hsi_site and "hive.llap.daemon.queue.name" in hsi_site:
            llap_daemon_selected_queue_name = hsi_site["hive.llap.daemon.queue.name"]

        # Update Visibility of 'num_llap_nodes' slider. Visible only if selected queue is Ambari created 'llap'.
        capacity_scheduler_properties, received_as_key_value_pair = self.getCapacitySchedulerProperties(services)
        if capacity_scheduler_properties:
            # Get all leaf queues.
            leafQueueNames = self.getAllYarnLeafQueues(capacity_scheduler_properties)
            self.logger.info("YARN leaf Queues = {0}".format(leafQueueNames))
            if len(leafQueueNames) == 0:
                self.logger.error("Queue(s) couldn't be retrieved from capacity-scheduler.")
                return

            # Check if it's 1st invocation after enabling Hive Server Interactive (config: enable_hive_interactive).
            changed_configs_has_enable_hive_int = self.isConfigPropertiesChanged(services, "hive-interactive-env", ['enable_hive_interactive'], False)
            llap_named_queue_selected_in_curr_invocation = None
            # Check if its : 1. 1st invocation from UI ('enable_hive_interactive' in changed-configurations)
            # OR 2. 1st invocation from BP (services['changed-configurations'] should be empty in this case)
            if (changed_configs_has_enable_hive_int or  0 == len(services['changed-configurations'])) \
                    and services['configurations']['hive-interactive-env']['properties']['enable_hive_interactive']:
                if len(leafQueueNames) == 1 or (len(leafQueueNames) == 2 and llap_queue_name in leafQueueNames):
                    llap_named_queue_selected_in_curr_invocation = True
                    putHiveInteractiveSiteProperty('hive.llap.daemon.queue.name', llap_queue_name)
                    putHiveInteractiveSiteProperty('hive.server2.tez.default.queues', llap_queue_name)
                else:
                    first_leaf_queue = list(leafQueueNames)[0]  # 1st invocation, pick the 1st leaf queue and set it as selected.
                    putHiveInteractiveSiteProperty('hive.llap.daemon.queue.name', first_leaf_queue)
                    putHiveInteractiveSiteProperty('hive.server2.tez.default.queues', first_leaf_queue)
                    llap_named_queue_selected_in_curr_invocation = False
            self.logger.info("DBG: llap_named_queue_selected_in_curr_invocation = {0}".format(llap_named_queue_selected_in_curr_invocation))

            if (len(leafQueueNames) == 2 and (llap_daemon_selected_queue_name and llap_daemon_selected_queue_name == llap_queue_name) or
                    llap_named_queue_selected_in_curr_invocation) or \
                    (len(leafQueueNames) == 1 and llap_daemon_selected_queue_name == 'default' and llap_named_queue_selected_in_curr_invocation):
                self.logger.info("DBG: Setting 'num_llap_nodes' config's  READ ONLY attribute as 'False'.")
                putHiveInteractiveEnvPropertyAttribute("num_llap_nodes", "read_only", "false")
                selected_queue_is_ambari_managed_llap = True
                self.logger.info("DBG: Selected YARN queue for LLAP is : '{0}'. Current YARN queues : {1}. Setting 'Number of LLAP nodes' "
                                 "slider visibility to 'True'".format(llap_queue_name, list(leafQueueNames)))
            else:
                self.logger.info("DBG: Setting 'num_llap_nodes' config's  READ ONLY attribute as 'True'.")
                putHiveInteractiveEnvPropertyAttribute("num_llap_nodes", "read_only", "true")
                self.logger.info("Selected YARN queue for LLAP is : '{0}'. Current YARN queues : {1}. Setting 'Number of LLAP nodes' "
                                 "visibility to 'False'.".format(llap_daemon_selected_queue_name, list(leafQueueNames)))
                selected_queue_is_ambari_managed_llap = False

            if not llap_named_queue_selected_in_curr_invocation:  # We would be creating the 'llap' queue later. Thus, cap-sched doesn't have
                # state information pertaining to 'llap' queue.
                # Check: State of the selected queue should not be STOPPED.
                if llap_daemon_selected_queue_name:
                    llap_selected_queue_state = self.__getQueueStateFromCapacityScheduler(capacity_scheduler_properties, llap_daemon_selected_queue_name)
                    if llap_selected_queue_state is None or llap_selected_queue_state == "STOPPED":
                        self.logger.error("Selected LLAP app queue '{0}' current state is : '{1}'. Setting LLAP configs to default "
                                          "values.".format(llap_daemon_selected_queue_name, llap_selected_queue_state))
                        self.recommendDefaultLlapConfiguration(configurations, services, hosts)
                        return
                else:
                    self.logger.error("Retrieved LLAP app queue name is : '{0}'. Setting LLAP configs to default values."
                                      .format(llap_daemon_selected_queue_name))
                    self.recommendDefaultLlapConfiguration(configurations, services, hosts)
                    return
        else:
            self.logger.error("Couldn't retrieve 'capacity-scheduler' properties while doing YARN queue adjustment for Hive Server Interactive."
                              " Not calculating LLAP configs.")
            return

        changed_configs_in_hive_int_env = None
        llap_concurrency_in_changed_configs = None
        llap_daemon_queue_in_changed_configs = None
        # Calculations are triggered only if there is change in any one of the following props :
        # 'num_llap_nodes', 'enable_hive_interactive', 'hive.server2.tez.sessions.per.default.queue'
        # or 'hive.llap.daemon.queue.name' has change in value selection.
        # OR
        # services['changed-configurations'] is empty implying that this is the Blueprint call. (1st invocation)
        if 'changed-configurations' in services.keys():
            config_names_to_be_checked = set(['num_llap_nodes', 'enable_hive_interactive'])
            changed_configs_in_hive_int_env = self.isConfigPropertiesChanged(services, "hive-interactive-env", config_names_to_be_checked, False)

            # Determine if there is change detected in "hive-interactive-site's" configs based on which we calculate llap configs.
            llap_concurrency_in_changed_configs = self.isConfigPropertiesChanged(services, 'hive-interactive-site', ['hive.server2.tez.sessions.per.default.queue'], False)
            llap_daemon_queue_in_changed_configs = self.isConfigPropertiesChanged(services, 'hive-interactive-site', ['hive.llap.daemon.queue.name'], False)

        if not changed_configs_in_hive_int_env and not llap_concurrency_in_changed_configs and \
                not llap_daemon_queue_in_changed_configs and services["changed-configurations"]:
            self.logger.info("DBG: LLAP parameters not modified. Not adjusting LLAP configs.")
            self.logger.info("DBG: Current 'changed-configuration' received is : {0}".format(services["changed-configurations"]))
            return

        self.logger.info("\nDBG: Performing LLAP config calculations ......")
        node_manager_host_list = self.getHostsForComponent(services, "YARN", "NODEMANAGER")
        node_manager_cnt = len(node_manager_host_list)
        yarn_nm_mem_in_mb = self.get_yarn_nm_mem_in_mb(services, configurations)
        total_cluster_capacity = node_manager_cnt * yarn_nm_mem_in_mb
        self.logger.info("DBG: Calculated total_cluster_capacity : {0}, using following : node_manager_cnt : {1}, "
                         "yarn_nm_mem_in_mb : {2}".format(total_cluster_capacity, node_manager_cnt, yarn_nm_mem_in_mb))
        yarn_min_container_size = float(self.get_yarn_min_container_size(services, configurations))
        tez_am_container_size = self.calculate_tez_am_container_size(services, long(total_cluster_capacity), is_cluster_create_opr,
                                                                     changed_configs_has_enable_hive_int)
        normalized_tez_am_container_size = self._normalizeUp(tez_am_container_size, yarn_min_container_size)

        if yarn_site and "yarn.nodemanager.resource.cpu-vcores" in yarn_site:
            cpu_per_nm_host = float(yarn_site["yarn.nodemanager.resource.cpu-vcores"])
        else:
            self.recommendDefaultLlapConfiguration(configurations, services, hosts)
            return
        self.logger.info("DBG Calculated normalized_tez_am_container_size : {0}, using following : tez_am_container_size : {1}, "
                         "total_cluster_capacity : {2}".format(normalized_tez_am_container_size, tez_am_container_size,
                                                               total_cluster_capacity))

        # Calculate the available memory for LLAP app
        yarn_nm_mem_in_mb_normalized = self._normalizeDown(yarn_nm_mem_in_mb, yarn_min_container_size)
        mem_per_thread_for_llap = float(self.calculate_mem_per_thread_for_llap(services, yarn_nm_mem_in_mb_normalized, cpu_per_nm_host,
                                                                               is_cluster_create_opr, changed_configs_has_enable_hive_int))
        self.logger.info("DBG: Calculated mem_per_thread_for_llap : {0}, using following: yarn_nm_mem_in_mb_normalized : {1}, "
                         "cpu_per_nm_host : {2}".format(mem_per_thread_for_llap, yarn_nm_mem_in_mb_normalized, cpu_per_nm_host))


        if mem_per_thread_for_llap is None:
            self.recommendDefaultLlapConfiguration(configurations, services, hosts)
            return

        # Get calculated value for Slider AM container Size
        slider_am_container_size = self._normalizeUp(self.calculate_slider_am_size(yarn_min_container_size),
                                                     yarn_min_container_size)
        self.logger.info("DBG: Calculated 'slider_am_container_size' : {0}, using following: yarn_min_container_size : "
                         "{1}".format(slider_am_container_size, yarn_min_container_size))

        min_memory_required = normalized_tez_am_container_size + slider_am_container_size + self._normalizeUp(mem_per_thread_for_llap, yarn_min_container_size)
        self.logger.info("DBG: Calculated 'min_memory_required': {0} using following : slider_am_container_size: {1}, "
                         "normalized_tez_am_container_size : {2}, mem_per_thread_for_llap : {3}, yarn_min_container_size : "
                         "{4}".format(min_memory_required, slider_am_container_size, normalized_tez_am_container_size, mem_per_thread_for_llap, yarn_min_container_size))

        min_nodes_required = int(math.ceil( min_memory_required / yarn_nm_mem_in_mb_normalized))
        self.logger.info("DBG: Calculated 'min_node_required': {0}, using following : min_memory_required : {1}, yarn_nm_mem_in_mb_normalized "
                         ": {2}".format(min_nodes_required, min_memory_required, yarn_nm_mem_in_mb_normalized))
        if min_nodes_required > node_manager_cnt:
            self.logger.warning("ERROR: Not enough memory/nodes to run LLAP");
            self.recommendDefaultLlapConfiguration(configurations, services, hosts)
            return

        mem_per_thread_for_llap = float(mem_per_thread_for_llap)

        self.logger.info("DBG: selected_queue_is_ambari_managed_llap = {0}".format(selected_queue_is_ambari_managed_llap))
        if not selected_queue_is_ambari_managed_llap:
            llap_daemon_selected_queue_cap = self.__getSelectedQueueTotalCap(capacity_scheduler_properties, llap_daemon_selected_queue_name, total_cluster_capacity)

            if llap_daemon_selected_queue_cap <= 0:
                self.logger.warning("'{0}' queue capacity percentage retrieved = {1}. Expected > 0.".format(
                    llap_daemon_selected_queue_name, llap_daemon_selected_queue_cap))
                self.recommendDefaultLlapConfiguration(configurations, services, hosts)
                return

            total_llap_mem_normalized = self._normalizeDown(llap_daemon_selected_queue_cap, yarn_min_container_size)
            self.logger.info("DBG: Calculated '{0}' queue available capacity : {1}, using following: llap_daemon_selected_queue_cap : {2}, "
                             "yarn_min_container_size : {3}".format(llap_daemon_selected_queue_name, total_llap_mem_normalized,
                                                                    llap_daemon_selected_queue_cap, yarn_min_container_size))
            '''Rounding up numNodes so that we run more daemons, and utilitze more CPUs. The rest of the calcaulations will take care of cutting this down if required'''
            num_llap_nodes_requested = math.ceil(total_llap_mem_normalized / yarn_nm_mem_in_mb_normalized)
            self.logger.info("DBG: Calculated 'num_llap_nodes_requested' : {0}, using following: total_llap_mem_normalized : {1}, "
                             "yarn_nm_mem_in_mb_normalized : {2}".format(num_llap_nodes_requested, total_llap_mem_normalized, yarn_nm_mem_in_mb_normalized))
            # Pouplate the 'num_llap_nodes_requested' in config 'num_llap_nodes', a read only config for non-Ambari managed queue case.
            putHiveInteractiveEnvProperty('num_llap_nodes', num_llap_nodes_requested)
            self.logger.info("Setting config 'num_llap_nodes' as : {0}".format(num_llap_nodes_requested))
            queue_am_fraction_perc = float(self.__getQueueAmFractionFromCapacityScheduler(capacity_scheduler_properties, llap_daemon_selected_queue_name))
            hive_tez_am_cap_available = queue_am_fraction_perc * total_llap_mem_normalized
            self.logger.info("DBG: Calculated 'hive_tez_am_cap_available' : {0}, using following: queue_am_fraction_perc : {1}, "
                             "total_llap_mem_normalized : {2}".format(hive_tez_am_cap_available, queue_am_fraction_perc, total_llap_mem_normalized))
        else:  # Ambari managed 'llap' named queue at root level.
            # Set 'num_llap_nodes_requested' for 1st invocation, as it gets passed as 1 otherwise, read from config.

            # Check if its : 1. 1st invocation from UI ('enable_hive_interactive' in changed-configurations)
            # OR 2. 1st invocation from BP (services['changed-configurations'] should be empty in this case and 'num_llap_nodes' not defined)
            if (changed_configs_has_enable_hive_int
                or (0 == len(services['changed-configurations'])
                    and not services['configurations']['hive-interactive-env']['properties']['num_llap_nodes'])) \
                    and services['configurations']['hive-interactive-env']['properties']['enable_hive_interactive']:
                num_llap_nodes_requested = min_nodes_required
            else:
                num_llap_nodes_requested = self.get_num_llap_nodes(services, configurations) #Input
            total_llap_mem = num_llap_nodes_requested * yarn_nm_mem_in_mb_normalized
            self.logger.info("DBG: Calculated 'total_llap_mem' : {0}, using following: num_llap_nodes_requested : {1}, "
                             "yarn_nm_mem_in_mb_normalized : {2}".format(total_llap_mem, num_llap_nodes_requested, yarn_nm_mem_in_mb_normalized))
            total_llap_mem_normalized = float(self._normalizeDown(total_llap_mem, yarn_min_container_size))
            self.logger.info("DBG: Calculated 'total_llap_mem_normalized' : {0}, using following: total_llap_mem : {1}, "
                             "yarn_min_container_size : {2}".format(total_llap_mem_normalized, total_llap_mem, yarn_min_container_size))

            # What percent is 'total_llap_mem' of 'total_cluster_capacity' ?
            llap_named_queue_cap_fraction = math.ceil(total_llap_mem_normalized / total_cluster_capacity * 100)
            self.logger.info("DBG: Calculated '{0}' queue capacity percent = {1}.".format(llap_queue_name, llap_named_queue_cap_fraction))

            if llap_named_queue_cap_fraction > 100:
                self.logger.warning("Calculated '{0}' queue size = {1}. Cannot be > 100.".format(llap_queue_name, llap_named_queue_cap_fraction))
                self.recommendDefaultLlapConfiguration(configurations, services, hosts)
                return

            # Adjust capacity scheduler for the 'llap' named queue.
            self.checkAndManageLlapQueue(services, configurations, hosts, llap_queue_name, llap_named_queue_cap_fraction)
            hive_tez_am_cap_available = total_llap_mem_normalized
            self.logger.info("DBG: hive_tez_am_cap_available : {0}".format(hive_tez_am_cap_available))

        # Common calculations now, irrespective of the queue selected.

        llap_mem_for_tezAm_and_daemons = total_llap_mem_normalized - slider_am_container_size
        self.logger.info("DBG: Calculated 'llap_mem_for_tezAm_and_daemons' : {0}, using following : total_llap_mem_normalized : {1}, "
                         "slider_am_container_size : {2}".format(llap_mem_for_tezAm_and_daemons, total_llap_mem_normalized, slider_am_container_size))

        if llap_mem_for_tezAm_and_daemons < 2 * yarn_min_container_size:
            self.logger.warning("Not enough capacity available on the cluster to run LLAP")
            self.recommendDefaultLlapConfiguration(configurations, services, hosts)
            return

        # Calculate llap concurrency (i.e. Number of Tez AM's)
        max_executors_per_node = self.get_max_executors_per_node(yarn_nm_mem_in_mb_normalized, cpu_per_nm_host, mem_per_thread_for_llap)

        # Read 'hive.server2.tez.sessions.per.default.queue' prop if it's in changed-configs, else calculate it.
        if not llap_concurrency_in_changed_configs:
            if max_executors_per_node <= 0:
                self.logger.warning("Calculated 'max_executors_per_node' = {0}. Expected value >= 1.".format(max_executors_per_node))
                self.recommendDefaultLlapConfiguration(configurations, services, hosts)
                return

            self.logger.info("DBG: Calculated 'max_executors_per_node' : {0}, using following: yarn_nm_mem_in_mb_normalized : {1}, cpu_per_nm_host : {2}, "
                             "mem_per_thread_for_llap: {3}".format(max_executors_per_node, yarn_nm_mem_in_mb_normalized, cpu_per_nm_host, mem_per_thread_for_llap))

            # Default 1 AM for every 20 executor threads.
            # The second part of the min calculates based on mem required for DEFAULT_EXECUTOR_TO_AM_RATIO executors + 1 AM,
            # making use of total memory. However, it's possible that total memory will not be used - and the numExecutors is
            # instead limited by #CPUs. Use maxPerNode to factor this in.
            llap_concurreny_limit = min(math.floor(max_executors_per_node * num_llap_nodes_requested / DEFAULT_EXECUTOR_TO_AM_RATIO), MAX_CONCURRENT_QUERIES)
            self.logger.info("DBG: Calculated 'llap_concurreny_limit' : {0}, using following : max_executors_per_node : {1}, num_llap_nodes_requested : {2}, DEFAULT_EXECUTOR_TO_AM_RATIO "
                             ": {3}, MAX_CONCURRENT_QUERIES : {4}".format(llap_concurreny_limit, max_executors_per_node, num_llap_nodes_requested, DEFAULT_EXECUTOR_TO_AM_RATIO, MAX_CONCURRENT_QUERIES))
            llap_concurrency = min(llap_concurreny_limit, math.floor(llap_mem_for_tezAm_and_daemons / (DEFAULT_EXECUTOR_TO_AM_RATIO * mem_per_thread_for_llap + normalized_tez_am_container_size)))
            self.logger.info("DBG: Calculated 'llap_concurrency' : {0}, using following : llap_concurreny_limit : {1}, llap_mem_for_tezAm_and_daemons : "
                             "{2}, DEFAULT_EXECUTOR_TO_AM_RATIO : {3}, mem_per_thread_for_llap : {4}, normalized_tez_am_container_size : "
                             "{5}".format(llap_concurrency, llap_concurreny_limit, llap_mem_for_tezAm_and_daemons, DEFAULT_EXECUTOR_TO_AM_RATIO,
                                          mem_per_thread_for_llap, normalized_tez_am_container_size))
            if llap_concurrency == 0:
                llap_concurrency = 1
                self.logger.info("DBG: Readjusted 'llap_concurrency' to : 1. Earlier calculated value : 0")

            if llap_concurrency * normalized_tez_am_container_size > hive_tez_am_cap_available:
                llap_concurrency = long(math.floor(hive_tez_am_cap_available / normalized_tez_am_container_size))
                self.logger.info("DBG: Readjusted 'llap_concurrency' to : {0}, as llap_concurrency({1}) * normalized_tez_am_container_size({2}) > hive_tez_am_cap_available({3}))"
                                 .format(llap_concurrency, llap_concurrency, normalized_tez_am_container_size, hive_tez_am_cap_available))

                if llap_concurrency <= 0:
                    self.logger.warning("DBG: Calculated 'LLAP Concurrent Queries' = {0}. Expected value >= 1.".format(llap_concurrency))
                    self.recommendDefaultLlapConfiguration(configurations, services, hosts)
                    return
                self.logger.info("DBG: Adjusted 'llap_concurrency' : {0}, using following: hive_tez_am_cap_available : {1}, normalized_tez_am_container_size: "
                                 "{2}".format(llap_concurrency, hive_tez_am_cap_available, normalized_tez_am_container_size))
        else:
            # Read current value
            if 'hive.server2.tez.sessions.per.default.queue' in hsi_site:
                llap_concurrency = long(hsi_site['hive.server2.tez.sessions.per.default.queue'])
                if llap_concurrency <= 0:
                    self.logger.warning("'hive.server2.tez.sessions.per.default.queue' current value : {0}. Expected value : >= 1".format(llap_concurrency))
                    self.recommendDefaultLlapConfiguration(configurations, services, hosts)
                    return
                self.logger.info("DBG: Read 'llap_concurrency' : {0}".format(llap_concurrency ))
            else:
                llap_concurrency = 1
                self.logger.warning("Couldn't retrieve Hive Server interactive's 'hive.server2.tez.sessions.per.default.queue' config. Setting default value 1.")
                self.recommendDefaultLlapConfiguration(configurations, services, hosts)
                return

        # Calculate 'Max LLAP Consurrency', irrespective of whether 'llap_concurrency' was read or calculated.
        max_llap_concurreny_limit = min(math.floor(max_executors_per_node * num_llap_nodes_requested / MIN_EXECUTOR_TO_AM_RATIO), MAX_CONCURRENT_QUERIES)
        self.logger.info("DBG: Calculated 'max_llap_concurreny_limit' : {0}, using following : max_executors_per_node : {1}, num_llap_nodes_requested "
                         ": {2}, MIN_EXECUTOR_TO_AM_RATIO : {3}, MAX_CONCURRENT_QUERIES : {4}".format(max_llap_concurreny_limit, max_executors_per_node,
                                                                                                      num_llap_nodes_requested, MIN_EXECUTOR_TO_AM_RATIO,
                                                                                                      MAX_CONCURRENT_QUERIES))
        max_llap_concurreny = long(min(max_llap_concurreny_limit, math.floor(llap_mem_for_tezAm_and_daemons / (MIN_EXECUTOR_TO_AM_RATIO *
                                                                                                               mem_per_thread_for_llap + normalized_tez_am_container_size))))
        self.logger.info("DBG: Calculated 'max_llap_concurreny' : {0}, using following : max_llap_concurreny_limit : {1}, llap_mem_for_tezAm_and_daemons : "
                         "{2}, MIN_EXECUTOR_TO_AM_RATIO : {3}, mem_per_thread_for_llap : {4}, normalized_tez_am_container_size : "
                         "{5}".format(max_llap_concurreny, max_llap_concurreny_limit, llap_mem_for_tezAm_and_daemons, MIN_EXECUTOR_TO_AM_RATIO,
                                      mem_per_thread_for_llap, normalized_tez_am_container_size))
        if int(max_llap_concurreny) < MAX_CONCURRENT_QUERIES_SMALL_CLUSTERS:
            self.logger.info("DBG: Adjusting 'max_llap_concurreny' from {0} to {1}".format(max_llap_concurreny, MAX_CONCURRENT_QUERIES_SMALL_CLUSTERS))
            max_llap_concurreny = MAX_CONCURRENT_QUERIES_SMALL_CLUSTERS

        if (max_llap_concurreny * normalized_tez_am_container_size) > hive_tez_am_cap_available:
            max_llap_concurreny = math.floor(hive_tez_am_cap_available / normalized_tez_am_container_size)
            if max_llap_concurreny <= 0:
                self.logger.warning("Calculated 'Max. LLAP Concurrent Queries' = {0}. Expected value > 1".format(max_llap_concurreny))
                self.recommendDefaultLlapConfiguration(configurations, services, hosts)
                return
            self.logger.info("DBG: Adjusted 'max_llap_concurreny' : {0}, using following: hive_tez_am_cap_available : {1}, normalized_tez_am_container_size: "
                             "{2}".format(max_llap_concurreny, hive_tez_am_cap_available, normalized_tez_am_container_size))

        # Calculate value for 'num_llap_nodes', an across cluster config.
        tez_am_memory_required = llap_concurrency * normalized_tez_am_container_size
        self.logger.info("DBG: Calculated 'tez_am_memory_required' : {0}, using following : llap_concurrency : {1}, normalized_tez_am_container_size : "
                         "{2}".format(tez_am_memory_required, llap_concurrency, normalized_tez_am_container_size))
        llap_mem_daemon_size = llap_mem_for_tezAm_and_daemons - tez_am_memory_required

        if llap_mem_daemon_size < yarn_min_container_size:
            self.logger.warning("Calculated 'LLAP Daemon Size = {0}'. Expected >= 'YARN Minimum Container Size' ({1})'".format(
                llap_mem_daemon_size, yarn_min_container_size))
            self.recommendDefaultLlapConfiguration(configurations, services, hosts)
            return

        if llap_mem_daemon_size < mem_per_thread_for_llap or llap_mem_daemon_size < yarn_min_container_size:
            self.logger.warning("Not enough memory available for executors.")
            self.recommendDefaultLlapConfiguration(configurations, services, hosts)
            return
        self.logger.info("DBG: Calculated 'llap_mem_daemon_size' : {0}, using following : llap_mem_for_tezAm_and_daemons : {1}, tez_am_memory_required : "
                         "{2}".format(llap_mem_daemon_size, llap_mem_for_tezAm_and_daemons, tez_am_memory_required))

        llap_daemon_mem_per_node = self._normalizeDown(llap_mem_daemon_size / num_llap_nodes_requested, yarn_min_container_size)
        # This value takes into account total cluster capacity, and may not have left enough capcaity on each node to launch an AM.
        self.logger.info("DBG: Calculated 'llap_daemon_mem_per_node' : {0}, using following : llap_mem_daemon_size : {1}, num_llap_nodes_requested : {2}, "
                         "yarn_min_container_size: {3}".format(llap_daemon_mem_per_node, llap_mem_daemon_size, num_llap_nodes_requested, yarn_min_container_size))
        if llap_daemon_mem_per_node == 0:
            # Small cluster. No capacity left on a node after running AMs.
            llap_daemon_mem_per_node = self._normalizeUp(mem_per_thread_for_llap, yarn_min_container_size)
            num_llap_nodes = math.floor(llap_mem_daemon_size / llap_daemon_mem_per_node)
            self.logger.info("DBG: 'llap_daemon_mem_per_node' : 0, adjusted 'llap_daemon_mem_per_node' : {0}, 'num_llap_nodes' : {1}, using following: llap_mem_daemon_size : {2}, "
                             "mem_per_thread_for_llap : {3}".format(llap_daemon_mem_per_node, num_llap_nodes, llap_mem_daemon_size, mem_per_thread_for_llap))
        elif llap_daemon_mem_per_node < mem_per_thread_for_llap:
            # Previously computed value of memory per thread may be too high. Cut the number of nodes. (Alternately reduce memory per node)
            llap_daemon_mem_per_node = mem_per_thread_for_llap
            num_llap_nodes = math.floor(llap_mem_daemon_size / mem_per_thread_for_llap)
            self.logger.info("DBG: 'llap_daemon_mem_per_node'({0}) < mem_per_thread_for_llap({1}), adjusted 'llap_daemon_mem_per_node' "
                             ": {2}".format(llap_daemon_mem_per_node, mem_per_thread_for_llap, llap_daemon_mem_per_node))
        else:
            # All good. We have a proper value for memoryPerNode.
            num_llap_nodes = num_llap_nodes_requested
            self.logger.info("DBG: num_llap_nodes : {0}".format(num_llap_nodes))

        # Make sure we have enough memory on each node to run AMs.
        # If nodes vs nodes_requested is different - AM memory is already factored in.
        # If llap_node_count < total_cluster_nodes - assuming AMs can run on a different node.
        # Else factor in min_concurrency_per_node * tez_am_size, and slider_am_size
        # Also needs to factor in whether num_llap_nodes = cluster_node_count
        min_mem_reserved_per_node = 0
        if num_llap_nodes == num_llap_nodes_requested and num_llap_nodes == node_manager_cnt:
            min_mem_reserved_per_node = max(normalized_tez_am_container_size, slider_am_container_size)
            tez_AMs_per_node = llap_concurrency / num_llap_nodes
            tez_AMs_per_node_low = int(math.floor(tez_AMs_per_node))
            tez_AMs_per_node_high = int(math.ceil(tez_AMs_per_node))
            min_mem_reserved_per_node = int(max(tez_AMs_per_node_high * normalized_tez_am_container_size, tez_AMs_per_node_low * normalized_tez_am_container_size + slider_am_container_size))
            self.logger.info("DBG: Determined 'AM reservation per node': {0}, using following : concurrency: {1}, num_llap_nodes: {2}, AMsPerNode: {3}"
                             .format(min_mem_reserved_per_node, llap_concurrency, num_llap_nodes,  tez_AMs_per_node))

        max_single_node_mem_available_for_daemon = self._normalizeDown(yarn_nm_mem_in_mb_normalized - min_mem_reserved_per_node, yarn_min_container_size)
        if max_single_node_mem_available_for_daemon <=0 or max_single_node_mem_available_for_daemon < mem_per_thread_for_llap:
            self.logger.warning("Not enough capacity available per node for daemons after factoring in AM memory requirements. NM Mem: {0}, "
                                "minAMMemPerNode: {1}, available: {2}".format(yarn_nm_mem_in_mb_normalized, min_mem_reserved_per_node, max_single_node_mem_available_for_daemon))
            self.recommendDefaultLlapConfiguration(configurations, services, hosts)

        llap_daemon_mem_per_node = min(max_single_node_mem_available_for_daemon, llap_daemon_mem_per_node)
        self.logger.info("DBG: Determined final memPerDaemon: {0}, using following: concurrency: {1}, numNMNodes: {2}, numLlapNodes: {3} "
                         .format(llap_daemon_mem_per_node, llap_concurrency, node_manager_cnt, num_llap_nodes))

        num_executors_per_node_max = self.get_max_executors_per_node(yarn_nm_mem_in_mb_normalized, cpu_per_nm_host, mem_per_thread_for_llap)
        if num_executors_per_node_max < 1:
            self.logger.warning("Calculated 'Max. Executors per Node' = {0}. Expected values >= 1.".format(num_executors_per_node_max))
            self.recommendDefaultLlapConfiguration(configurations, services, hosts)
            return
        self.logger.info("DBG: Calculated 'num_executors_per_node_max' : {0}, using following : yarn_nm_mem_in_mb_normalized : {1}, cpu_per_nm_host : {2}, "
                         "mem_per_thread_for_llap: {3}".format(num_executors_per_node_max, yarn_nm_mem_in_mb_normalized, cpu_per_nm_host, mem_per_thread_for_llap))

        # NumExecutorsPerNode is not necessarily max - since some capacity would have been reserved for AMs, if this value were based on mem.
        num_executors_per_node = min(math.floor(llap_daemon_mem_per_node / mem_per_thread_for_llap), num_executors_per_node_max)
        if num_executors_per_node <= 0:
            self.logger.warning("Calculated 'Number of Executors Per Node' = {0}. Expected value >= 1".format(num_executors_per_node))
            self.recommendDefaultLlapConfiguration(configurations, services, hosts)
            return
        self.logger.info("DBG: Calculated 'num_executors_per_node' : {0}, using following : llap_daemon_mem_per_node : {1}, num_executors_per_node_max : {2}, "
                         "mem_per_thread_for_llap: {3}".format(num_executors_per_node, llap_daemon_mem_per_node, num_executors_per_node_max, mem_per_thread_for_llap))

        # Now figure out how much of the memory will be used by the executors, and how much will be used by the cache.
        total_mem_for_executors_per_node = num_executors_per_node * mem_per_thread_for_llap
        cache_mem_per_node = llap_daemon_mem_per_node - total_mem_for_executors_per_node
        self.logger.info("DBG: Calculated 'Cache per node' : {0}, using following : llap_daemon_mem_per_node : {1}, total_mem_for_executors_per_node : {2}"
                         .format(cache_mem_per_node, llap_daemon_mem_per_node, total_mem_for_executors_per_node))

        tez_runtime_io_sort_mb = (long((0.8 * mem_per_thread_for_llap) / 3))
        tez_runtime_unordered_output_buffer_size = long(0.8 * 0.075 * mem_per_thread_for_llap)
        # 'hive_auto_convert_join_noconditionaltask_size' value is in bytes. Thus, multiplying it by 1048576.
        hive_auto_convert_join_noconditionaltask_size = (long((0.8 * mem_per_thread_for_llap) / 3)) * MB_TO_BYTES

        # Calculate value for prop 'llap_heap_size'
        llap_xmx = max(total_mem_for_executors_per_node * 0.8, total_mem_for_executors_per_node - self.get_llap_headroom_space(services, configurations))
        self.logger.info("DBG: Calculated llap_app_heap_size : {0}, using following : total_mem_for_executors : {1}".format(llap_xmx, total_mem_for_executors_per_node))

        # Calculate 'hive_heapsize' for Hive2/HiveServer2 (HSI)
        hive_server_interactive_heapsize = None
        hive_server_interactive_hosts = self.getHostsWithComponent("HIVE", "HIVE_SERVER_INTERACTIVE", services, hosts)
        if hive_server_interactive_hosts is None:
            # If its None, read the base service YARN's NODEMANAGER node memory, as are host are considered homogenous.
            hive_server_interactive_hosts = self.getHostsWithComponent("YARN", "NODEMANAGER", services, hosts)
        if hive_server_interactive_hosts is not None and len(hive_server_interactive_hosts) > 0:
            host_mem = long(hive_server_interactive_hosts[0]["Hosts"]["total_mem"])
            hive_server_interactive_heapsize = min(max(2048.0, 400.0*llap_concurrency), 3.0/8 * host_mem)
            self.logger.info("DBG: Calculated 'hive_server_interactive_heapsize' : {0}, using following : llap_concurrency : {1}, host_mem : "
                             "{2}".format(hive_server_interactive_heapsize, llap_concurrency, host_mem))

        # Done with calculations, updating calculated configs.
        self.logger.info("DBG: Applying the calculated values....")

        if is_cluster_create_opr or changed_configs_has_enable_hive_int:
            normalized_tez_am_container_size = long(normalized_tez_am_container_size)
            putTezInteractiveSiteProperty('tez.am.resource.memory.mb', normalized_tez_am_container_size)
            self.logger.info("DBG: Setting 'tez.am.resource.memory.mb' config value as : {0}".format(normalized_tez_am_container_size))

        if not llap_concurrency_in_changed_configs:
            min_llap_concurrency = 1
            putHiveInteractiveSiteProperty('hive.server2.tez.sessions.per.default.queue', long(llap_concurrency))
            putHiveInteractiveSitePropertyAttribute('hive.server2.tez.sessions.per.default.queue', "minimum", min_llap_concurrency)

        # Check if 'max_llap_concurreny' < 'llap_concurrency'.
        if max_llap_concurreny < llap_concurrency:
            self.logger.info("DBG: Adjusting 'max_llap_concurreny' to : {0}, based on 'llap_concurrency' : {1} and "
                             "earlier 'max_llap_concurreny' : {2}. ".format(llap_concurrency, llap_concurrency, max_llap_concurreny))
            max_llap_concurreny = llap_concurrency
        putHiveInteractiveSitePropertyAttribute('hive.server2.tez.sessions.per.default.queue', "maximum", long(max_llap_concurreny))

        num_llap_nodes = long(num_llap_nodes)

        putHiveInteractiveEnvPropertyAttribute('num_llap_nodes', "minimum", min_nodes_required)
        putHiveInteractiveEnvPropertyAttribute('num_llap_nodes', "maximum", node_manager_cnt)
        if (num_llap_nodes != num_llap_nodes_requested):
            self.logger.info("DBG: User requested num_llap_nodes : {0}, but used/adjusted value for calculations is : {1}".format(num_llap_nodes_requested, num_llap_nodes))
        else:
            self.logger.info("DBG: Used num_llap_nodes for calculations : {0}".format(num_llap_nodes_requested))

        # Safeguard for not adding "num_llap_nodes_for_llap_daemons" if it doesnt exist in hive-interactive-site.
        # This can happen if we upgrade from Ambari 2.4 (with HDP 2.5) to Ambari 2.5, as this config is from 2.6 stack onwards only.
        if "hive-interactive-env" in services["configurations"] and \
                        "num_llap_nodes_for_llap_daemons" in services["configurations"]["hive-interactive-env"]["properties"]:
            putHiveInteractiveEnvProperty('num_llap_nodes_for_llap_daemons', num_llap_nodes)
            self.logger.info("DBG: Setting config 'num_llap_nodes_for_llap_daemons' as : {0}".format(num_llap_nodes))

        llap_container_size = long(llap_daemon_mem_per_node)
        putHiveInteractiveSiteProperty('hive.llap.daemon.yarn.container.mb', llap_container_size)

        # Set 'hive.tez.container.size' only if it is read as "SET_ON_FIRST_INVOCATION", implying initialization.
        # Else, we don't (1). Override the previous calculated value or (2). User provided value.
        if is_cluster_create_opr or changed_configs_has_enable_hive_int:
            mem_per_thread_for_llap = long(mem_per_thread_for_llap)
            putHiveInteractiveSiteProperty('hive.tez.container.size', mem_per_thread_for_llap)
            self.logger.info("DBG: Setting 'hive.tez.container.size' config value as : {0}".format(mem_per_thread_for_llap))


        putTezInteractiveSiteProperty('tez.runtime.io.sort.mb', tez_runtime_io_sort_mb)
        if "tez-site" in services["configurations"] and "tez.runtime.sorter.class" in services["configurations"]["tez-site"]["properties"]:
            if services["configurations"]["tez-site"]["properties"]["tez.runtime.sorter.class"] == "LEGACY":
                putTezInteractiveSiteProperty("tez.runtime.io.sort.mb", "maximum", 1800)

        putTezInteractiveSiteProperty('tez.runtime.unordered.output.buffer.size-mb', tez_runtime_unordered_output_buffer_size)
        putHiveInteractiveSiteProperty('hive.auto.convert.join.noconditionaltask.size', hive_auto_convert_join_noconditionaltask_size)

        num_executors_per_node = long(num_executors_per_node)
        self.logger.info("DBG: Putting num_executors_per_node as {0}".format(num_executors_per_node))
        putHiveInteractiveSiteProperty('hive.llap.daemon.num.executors', num_executors_per_node)
        putHiveInteractiveSitePropertyAttribute('hive.llap.daemon.num.executors', "minimum", 1)
        putHiveInteractiveSitePropertyAttribute('hive.llap.daemon.num.executors', "maximum", long(num_executors_per_node_max))

        # 'hive.llap.io.threadpool.size' config value is to be set same as value calculated for
        # 'hive.llap.daemon.num.executors' at all times.
        cache_mem_per_node = long(cache_mem_per_node)

        putHiveInteractiveSiteProperty('hive.llap.io.threadpool.size', num_executors_per_node)
        putHiveInteractiveSiteProperty('hive.llap.io.memory.size', cache_mem_per_node)

        if hive_server_interactive_heapsize is not None:
            putHiveInteractiveEnvProperty("hive_heapsize", int(hive_server_interactive_heapsize))

        llap_io_enabled = 'true' if long(cache_mem_per_node) >= 64 else 'false'
        putHiveInteractiveSiteProperty('hive.llap.io.enabled', llap_io_enabled)

        putHiveInteractiveEnvProperty('llap_heap_size', long(llap_xmx))
        putHiveInteractiveEnvProperty('slider_am_container_mb', long(slider_am_container_size))
        self.logger.info("DBG: Done putting all configs")

    def recommendDefaultLlapConfiguration(self, configurations, services, hosts):
        self.logger.info("DBG: Something likely went wrong. recommendDefaultLlapConfiguration")
        putHiveInteractiveSiteProperty = self.putProperty(configurations, 'hive-interactive-site', services)
        putHiveInteractiveSitePropertyAttribute = self.putPropertyAttribute(configurations, 'hive-interactive-site')

        putHiveInteractiveEnvProperty = self.putProperty(configurations, "hive-interactive-env", services)
        putHiveInteractiveEnvPropertyAttribute = self.putPropertyAttribute(configurations, "hive-interactive-env")

        yarn_min_container_size = long(self.get_yarn_min_container_size(services, configurations))
        slider_am_container_size = long(self.calculate_slider_am_size(yarn_min_container_size))

        node_manager_host_list = self.getHostsForComponent(services, "YARN", "NODEMANAGER")
        node_manager_cnt = len(node_manager_host_list)

        putHiveInteractiveSiteProperty('hive.server2.tez.sessions.per.default.queue', 1)
        putHiveInteractiveSitePropertyAttribute('hive.server2.tez.sessions.per.default.queue', "minimum", 1)
        putHiveInteractiveSitePropertyAttribute('hive.server2.tez.sessions.per.default.queue', "maximum", 1)
        putHiveInteractiveEnvProperty('num_llap_nodes', 0)

        # Safeguard for not adding "num_llap_nodes_for_llap_daemons" if it doesnt exist in hive-interactive-site.
        # This can happen if we upgrade from Ambari 2.4 (with HDP 2.5) to Ambari 2.5, as this config is from 2.6 stack onwards only.
        if "hive-interactive-env" in services["configurations"] and \
                        "num_llap_nodes_for_llap_daemons" in services["configurations"]["hive-interactive-env"]["properties"]:
            putHiveInteractiveEnvProperty('num_llap_nodes_for_llap_daemons', 0)

        putHiveInteractiveEnvPropertyAttribute('num_llap_nodes', "minimum", 1)
        putHiveInteractiveEnvPropertyAttribute('num_llap_nodes', "maximum", node_manager_cnt)
        putHiveInteractiveSiteProperty('hive.llap.daemon.yarn.container.mb', yarn_min_container_size)
        putHiveInteractiveSitePropertyAttribute('hive.llap.daemon.yarn.container.mb', "minimum", yarn_min_container_size)
        putHiveInteractiveSiteProperty('hive.llap.daemon.num.executors', 0)
        putHiveInteractiveSitePropertyAttribute('hive.llap.daemon.num.executors', "minimum", 1)
        putHiveInteractiveSiteProperty('hive.llap.io.threadpool.size', 0)
        putHiveInteractiveSiteProperty('hive.llap.io.memory.size', 0)
        putHiveInteractiveEnvProperty('llap_heap_size', 0)
        putHiveInteractiveEnvProperty('slider_am_container_mb', slider_am_container_size)

    def get_num_llap_nodes(self, services, configurations):
        """
        Returns current value of number of LLAP nodes in cluster (num_llap_nodes)

        :type services: dict
        :type configurations: dict
        :rtype int
        """
        hsi_env = self.getServicesSiteProperties(services, "hive-interactive-env")
        hsi_env_properties = self.getSiteProperties(configurations, "hive-interactive-env")
        num_llap_nodes = 0

        # Check if 'num_llap_nodes' is modified in current ST invocation.
        if hsi_env_properties and 'num_llap_nodes' in hsi_env_properties:
            num_llap_nodes = hsi_env_properties['num_llap_nodes']
        elif hsi_env and 'num_llap_nodes' in hsi_env:
            num_llap_nodes = hsi_env['num_llap_nodes']
        else:
            self.logger.error("Couldn't retrieve Hive Server 'num_llap_nodes' config. Setting value to {0}".format(num_llap_nodes))

        return float(num_llap_nodes)

    def get_max_executors_per_node(self, nm_mem_per_node_normalized, nm_cpus_per_node, mem_per_thread):
        return min(math.floor(nm_mem_per_node_normalized / mem_per_thread), nm_cpus_per_node)

    def calculate_mem_per_thread_for_llap(self, services, nm_mem_per_node_normalized, cpu_per_nm_host, is_cluster_create_opr=False,
                                          enable_hive_interactive_1st_invocation=False):
        """
        Calculates 'mem_per_thread_for_llap' for 1st time initialization. Else returns 'hive.tez.container.size' read value.
        """
        hive_tez_container_size = self.get_hive_tez_container_size(services)
        if is_cluster_create_opr or enable_hive_interactive_1st_invocation:
            if nm_mem_per_node_normalized <= 1024:
                calculated_hive_tez_container_size = min(512, nm_mem_per_node_normalized)
            elif nm_mem_per_node_normalized <= 4096:
                calculated_hive_tez_container_size = 1024
            elif nm_mem_per_node_normalized <= 10240:
                calculated_hive_tez_container_size = 2048
            elif nm_mem_per_node_normalized <= 24576:
                calculated_hive_tez_container_size = 3072
            else:
                calculated_hive_tez_container_size = 4096

            self.logger.info("DBG: Calculated and returning 'hive_tez_container_size' : {0}".format(calculated_hive_tez_container_size))
            return calculated_hive_tez_container_size
        else:
            self.logger.info("DBG: Returning 'hive_tez_container_size' : {0}".format(hive_tez_container_size))
            return hive_tez_container_size

    def get_hive_tez_container_size(self, services):
        """
        Gets HIVE Tez container size (hive.tez.container.size).
        """
        hive_container_size = None
        hsi_site = self.getServicesSiteProperties(services, 'hive-interactive-site')
        if hsi_site and 'hive.tez.container.size' in hsi_site:
            hive_container_size = hsi_site['hive.tez.container.size']

        if not hive_container_size:
            # This can happen (1). If config is missing in hive-interactive-site or (2). its an
            # upgrade scenario from Ambari 2.4 to Ambari 2.5 with HDP 2.5 installed. Read it
            # from hive-site.
            #
            # If Ambari 2.5 after upgrade from 2.4 is managing HDP 2.6 here, this config would have
            # already been added in hive-interactive-site as part of HDP upgrade from 2.5 to 2.6,
            # and we wont end up in this block to look up in hive-site.
            hive_site = self.getServicesSiteProperties(services, "hive-site")
            if hive_site and 'hive.tez.container.size' in hive_site:
                hive_container_size = hive_site['hive.tez.container.size']
        return hive_container_size

    def get_llap_headroom_space(self, services, configurations):
        """
        Gets HIVE Server Interactive's 'llap_headroom_space' config. (Default value set to 6144 bytes).
        """
        llap_headroom_space = None
        # Check if 'llap_headroom_space' is modified in current SA invocation.
        if 'hive-interactive-env' in configurations and 'llap_headroom_space' in configurations['hive-interactive-env']['properties']:
            hive_container_size = float(configurations['hive-interactive-env']['properties']['llap_headroom_space'])
            self.logger.info("'llap_headroom_space' read from configurations as : {0}".format(llap_headroom_space))

        if llap_headroom_space is None:
            # Check if 'llap_headroom_space' is input in services array.
            if 'llap_headroom_space' in services['configurations']['hive-interactive-env']['properties']:
                llap_headroom_space = float(services['configurations']['hive-interactive-env']['properties']['llap_headroom_space'])
                self.logger.info("'llap_headroom_space' read from services as : {0}".format(llap_headroom_space))
        if not llap_headroom_space or llap_headroom_space < 1:
            llap_headroom_space = 6144 # 6GB
            self.logger.info("Couldn't read 'llap_headroom_space' from services or configurations. Returing default value : 6144 bytes")

        return llap_headroom_space

    def get_yarn_min_container_size(self, services, configurations):
        """
        Gets YARN's minimum container size (yarn.scheduler.minimum-allocation-mb).
        Reads from:
          - configurations (if changed as part of current Stack Advisor invocation (output)), and services["changed-configurations"]
            is empty, else
          - services['configurations'] (input).

        services["changed-configurations"] would be empty if Stack Advisor call is made from Blueprints (1st invocation). Subsequent
        Stack Advisor calls will have it non-empty. We do this because in subsequent invocations, even if Stack Advisor calculates this
        value (configurations), it is finally not recommended, making 'input' value to survive.

        :type services dict
        :type configurations dict
        :rtype str
        """
        yarn_min_container_size = None
        yarn_min_allocation_property = "yarn.scheduler.minimum-allocation-mb"
        yarn_site = self.getSiteProperties(configurations, "yarn-site")
        yarn_site_properties = self.getServicesSiteProperties(services, "yarn-site")

        # Check if services["changed-configurations"] is empty and 'yarn.scheduler.minimum-allocation-mb' is modified in current ST invocation.
        if not services["changed-configurations"] and yarn_site and yarn_min_allocation_property in yarn_site:
            yarn_min_container_size = yarn_site[yarn_min_allocation_property]
            self.logger.info("DBG: 'yarn.scheduler.minimum-allocation-mb' read from output as : {0}".format(yarn_min_container_size))

        # Check if 'yarn.scheduler.minimum-allocation-mb' is input in services array.
        elif yarn_site_properties and yarn_min_allocation_property in yarn_site_properties:
            yarn_min_container_size = yarn_site_properties[yarn_min_allocation_property]
            self.logger.info("DBG: 'yarn.scheduler.minimum-allocation-mb' read from services as : {0}".format(yarn_min_container_size))

        if not yarn_min_container_size:
            self.logger.error("{0} was not found in the configuration".format(yarn_min_allocation_property))

        return yarn_min_container_size

    def calculate_slider_am_size(self, yarn_min_container_size):
        """
        Calculates the Slider App Master size based on YARN's Minimum Container Size.

        :type yarn_min_container_size int
        """
        if yarn_min_container_size >= 1024:
            return 1024
        else:
            return 512

    def get_yarn_nm_mem_in_mb(self, services, configurations):
        """
        Gets YARN NodeManager memory in MB (yarn.nodemanager.resource.memory-mb).
        Reads from:
          - configurations (if changed as part of current Stack Advisor invocation (output)), and services["changed-configurations"]
            is empty, else
          - services['configurations'] (input).

        services["changed-configurations"] would be empty is Stack Advisor call if made from Blueprints (1st invocation). Subsequent
        Stack Advisor calls will have it non-empty. We do this because in subsequent invocations, even if Stack Advsior calculates this
        value (configurations), it is finally not recommended, making 'input' value to survive.
        """
        yarn_nm_mem_in_mb = None

        yarn_site = self.getServicesSiteProperties(services, "yarn-site")
        yarn_site_properties = self.getSiteProperties(configurations, "yarn-site")

        # Check if services["changed-configurations"] is empty and 'yarn.nodemanager.resource.memory-mb' is modified in current ST invocation.
        if not services["changed-configurations"] and yarn_site_properties and 'yarn.nodemanager.resource.memory-mb' in yarn_site_properties:
            yarn_nm_mem_in_mb = float(yarn_site_properties['yarn.nodemanager.resource.memory-mb'])
        elif yarn_site and 'yarn.nodemanager.resource.memory-mb' in yarn_site:
            # Check if 'yarn.nodemanager.resource.memory-mb' is input in services array.
            yarn_nm_mem_in_mb = float(yarn_site['yarn.nodemanager.resource.memory-mb'])

        if yarn_nm_mem_in_mb <= 0.0:
            self.logger.warning("'yarn.nodemanager.resource.memory-mb' current value : {0}. Expected value : > 0".format(yarn_nm_mem_in_mb))

        return yarn_nm_mem_in_mb

    def calculate_tez_am_container_size(self, services, total_cluster_capacity, is_cluster_create_opr=False, enable_hive_interactive_1st_invocation=False):
        """
        Calculates Tez App Master container size (tez.am.resource.memory.mb) for tez_hive2/tez-site on initialization if values read is 0.
        Else returns the read value.
        """
        tez_am_resource_memory_mb = self.get_tez_am_resource_memory_mb(services)
        calculated_tez_am_resource_memory_mb = None
        if is_cluster_create_opr or enable_hive_interactive_1st_invocation:
            if total_cluster_capacity <= 4096:
                calculated_tez_am_resource_memory_mb = 512
            elif total_cluster_capacity > 4096 and total_cluster_capacity <= 98304:
                calculated_tez_am_resource_memory_mb = 1024
            elif total_cluster_capacity > 98304:
                calculated_tez_am_resource_memory_mb = 4096

            self.logger.info("DBG: Calculated and returning 'tez_am_resource_memory_mb' as : {0}".format(calculated_tez_am_resource_memory_mb))
            return float(calculated_tez_am_resource_memory_mb)
        else:
            self.logger.info("DBG: Returning 'tez_am_resource_memory_mb' as : {0}".format(tez_am_resource_memory_mb))
            return float(tez_am_resource_memory_mb)

    def get_tez_am_resource_memory_mb(self, services):
        """
        Gets Tez's AM resource memory (tez.am.resource.memory.mb) from services.
        """
        tez_am_resource_memory_mb = None
        if 'tez.am.resource.memory.mb' in services['configurations']['tez-interactive-site']['properties']:
            tez_am_resource_memory_mb = services['configurations']['tez-interactive-site']['properties']['tez.am.resource.memory.mb']

        return tez_am_resource_memory_mb

    def min_queue_perc_reqd_for_llap_and_hive_app(self, services, hosts, configurations):
        """
        Calculate minimum queue capacity required in order to get LLAP and HIVE2 app into running state.
        """
        # Get queue size if sized at 20%
        node_manager_hosts = self.getHostsForComponent(services, "YARN", "NODEMANAGER")
        yarn_rm_mem_in_mb = self.get_yarn_nm_mem_in_mb(services, configurations)
        total_cluster_cap = len(node_manager_hosts) * yarn_rm_mem_in_mb
        total_queue_size_at_20_perc = 20.0 / 100 * total_cluster_cap

        # Calculate based on minimum size required by containers.
        yarn_min_container_size = long(self.get_yarn_min_container_size(services, configurations))
        slider_am_size = self.calculate_slider_am_size(float(yarn_min_container_size))
        hive_tez_container_size = long(self.get_hive_tez_container_size(services))
        tez_am_container_size = self.calculate_tez_am_container_size(services, long(total_cluster_cap))
        normalized_val = self._normalizeUp(slider_am_size, yarn_min_container_size) \
                         + self._normalizeUp(hive_tez_container_size, yarn_min_container_size) \
                         + self._normalizeUp(tez_am_container_size, yarn_min_container_size)

        min_required = max(total_queue_size_at_20_perc, normalized_val)
        min_required_perc = min_required * 100 / total_cluster_cap

        return int(math.ceil(min_required_perc))

    def _normalizeDown(self, val1, val2):
        """
        Normalize down 'val2' with respect to 'val1'.
        """
        tmp = math.floor(val1 / val2)
        if tmp < 1.00:
            return 0
        return tmp * val2

    def _normalizeUp(self, val1, val2):
        """
        Normalize up 'val2' with respect to 'val1'.
        """
        tmp = math.ceil(val1 / val2)
        return tmp * val2

    def checkAndManageLlapQueue(self, services, configurations, hosts, llap_queue_name, llap_queue_cap_perc):
        """
        Checks and (1). Creates 'llap' queue if only 'default' queue exist at leaf level and is consuming 100% capacity OR
                   (2). Updates 'llap' queue capacity and state, if current selected queue is 'llap', and only 2 queues exist
                        at root level : 'default' and 'llap'.
        """
        self.logger.info("Determining creation/adjustment of 'capacity-scheduler' for 'llap' queue.")
        putHiveInteractiveEnvProperty = self.putProperty(configurations, "hive-interactive-env", services)
        putHiveInteractiveSiteProperty = self.putProperty(configurations, 'hive-interactive-site', services)
        putHiveInteractiveEnvPropertyAttribute = self.putPropertyAttribute(configurations, "hive-interactive-env")
        putCapSchedProperty = self.putProperty(configurations, "capacity-scheduler", services)
        leafQueueNames = None
        hsi_site = self.getServicesSiteProperties(services, 'hive-interactive-site')

        capacity_scheduler_properties, received_as_key_value_pair = self.getCapacitySchedulerProperties(services)
        if capacity_scheduler_properties:
            leafQueueNames = self.getAllYarnLeafQueues(capacity_scheduler_properties)
            cap_sched_config_keys = capacity_scheduler_properties.keys()

            yarn_default_queue_capacity = -1
            if 'yarn.scheduler.capacity.root.default.capacity' in cap_sched_config_keys:
                yarn_default_queue_capacity = float(capacity_scheduler_properties.get('yarn.scheduler.capacity.root.default.capacity'))

            # Get 'llap' queue state
            currLlapQueueState = ''
            if 'yarn.scheduler.capacity.root.'+llap_queue_name+'.state' in cap_sched_config_keys:
                currLlapQueueState = capacity_scheduler_properties.get('yarn.scheduler.capacity.root.'+llap_queue_name+'.state')

            # Get 'llap' queue capacity
            currLlapQueueCap = -1
            if 'yarn.scheduler.capacity.root.'+llap_queue_name+'.capacity' in cap_sched_config_keys:
                currLlapQueueCap = int(float(capacity_scheduler_properties.get('yarn.scheduler.capacity.root.'+llap_queue_name+'.capacity')))

            updated_cap_sched_configs_str = ''

            enabled_hive_int_in_changed_configs = self.isConfigPropertiesChanged(services, "hive-interactive-env", ['enable_hive_interactive'], False)
            """
            We create OR "modify 'llap' queue 'state and/or capacity' " based on below conditions:
             - if only 1 queue exists at root level and is 'default' queue and has 100% cap -> Create 'llap' queue,  OR
             - if 2 queues exists at root level ('llap' and 'default') :
                 - Queue selected is 'llap' and state is STOPPED -> Modify 'llap' queue state to RUNNING, adjust capacity, OR
                 - Queue selected is 'llap', state is RUNNING and 'llap_queue_capacity' prop != 'llap' queue current running capacity ->
                    Modify 'llap' queue capacity to 'llap_queue_capacity'
            """
            if 'default' in leafQueueNames and \
                    ((len(leafQueueNames) == 1 and int(yarn_default_queue_capacity) == 100) or \
                             ((len(leafQueueNames) == 2 and llap_queue_name in leafQueueNames) and \
                                      ((currLlapQueueState == 'STOPPED' and enabled_hive_int_in_changed_configs) or (currLlapQueueState == 'RUNNING' and currLlapQueueCap != llap_queue_cap_perc)))):
                adjusted_default_queue_cap = str(100 - llap_queue_cap_perc)

                hive_user = '*'  # Open to all
                if 'hive_user' in services['configurations']['hive-env']['properties']:
                    hive_user = services['configurations']['hive-env']['properties']['hive_user']

                llap_queue_cap_perc = str(llap_queue_cap_perc)

                # If capacity-scheduler configs are received as one concatenated string, we deposit the changed configs back as
                # one concatenated string.
                updated_cap_sched_configs_as_dict = False
                if not received_as_key_value_pair:
                    for prop, val in capacity_scheduler_properties.items():
                        if llap_queue_name not in prop:
                            if prop == 'yarn.scheduler.capacity.root.queues':
                                updated_cap_sched_configs_str = updated_cap_sched_configs_str \
                                                                + prop + "=default,llap\n"
                            elif prop == 'yarn.scheduler.capacity.root.default.capacity':
                                updated_cap_sched_configs_str = updated_cap_sched_configs_str \
                                                                + prop + "=" + adjusted_default_queue_cap + "\n"
                            elif prop == 'yarn.scheduler.capacity.root.default.maximum-capacity':
                                updated_cap_sched_configs_str = updated_cap_sched_configs_str \
                                                                + prop + "=" + adjusted_default_queue_cap + "\n"
                            elif prop == 'yarn.scheduler.capacity.root.ordering-policy':
                                # Don't put this in again. We're re-writing the llap section.
                                pass
                            elif prop.startswith('yarn.') and '.llap.' not in prop:
                                updated_cap_sched_configs_str = updated_cap_sched_configs_str + prop + "=" + val + "\n"

                    # Now, append the 'llap' queue related properties
                    updated_cap_sched_configs_str += """yarn.scheduler.capacity.root.ordering-policy=priority-utilization
    yarn.scheduler.capacity.root.{0}.user-limit-factor=1
    yarn.scheduler.capacity.root.{0}.state=RUNNING
    yarn.scheduler.capacity.root.{0}.ordering-policy=fifo
    yarn.scheduler.capacity.root.{0}.priority=10
    yarn.scheduler.capacity.root.{0}.minimum-user-limit-percent=100
    yarn.scheduler.capacity.root.{0}.maximum-capacity={1}
    yarn.scheduler.capacity.root.{0}.capacity={1}
    yarn.scheduler.capacity.root.{0}.acl_submit_applications={2}
    yarn.scheduler.capacity.root.{0}.acl_administer_queue={2}
    yarn.scheduler.capacity.root.{0}.maximum-am-resource-percent=1""".format(llap_queue_name, llap_queue_cap_perc, hive_user)

                    putCapSchedProperty("capacity-scheduler", updated_cap_sched_configs_str)
                    self.logger.info("Updated 'capacity-scheduler' configs as one concatenated string.")
                else:
                    # If capacity-scheduler configs are received as a  dictionary (generally 1st time), we deposit the changed
                    # values back as dictionary itself.
                    # Update existing configs in 'capacity-scheduler'.
                    for prop, val in capacity_scheduler_properties.items():
                        if llap_queue_name not in prop:
                            if prop == 'yarn.scheduler.capacity.root.queues':
                                putCapSchedProperty(prop, 'default,llap')
                            elif prop == 'yarn.scheduler.capacity.root.default.capacity':
                                putCapSchedProperty(prop, adjusted_default_queue_cap)
                            elif prop == 'yarn.scheduler.capacity.root.default.maximum-capacity':
                                putCapSchedProperty(prop, adjusted_default_queue_cap)
                            elif prop == 'yarn.scheduler.capacity.root.ordering-policy':
                                # Don't put this in again. We're re-writing the llap section.
                                pass
                            elif prop.startswith('yarn.') and '.llap.' not in prop:
                                putCapSchedProperty(prop, val)

                    # Add new 'llap' queue related configs.
                    putCapSchedProperty("yarn.scheduler.capacity.root.ordering-policy", "priority-utilization")
                    putCapSchedProperty("yarn.scheduler.capacity.root." + llap_queue_name + ".user-limit-factor", "1")
                    putCapSchedProperty("yarn.scheduler.capacity.root." + llap_queue_name + ".state", "RUNNING")
                    putCapSchedProperty("yarn.scheduler.capacity.root." + llap_queue_name + ".ordering-policy", "fifo")
                    putCapSchedProperty("yarn.scheduler.capacity.root." + llap_queue_name + ".priority", "10")
                    putCapSchedProperty("yarn.scheduler.capacity.root." + llap_queue_name + ".minimum-user-limit-percent", "100")
                    putCapSchedProperty("yarn.scheduler.capacity.root." + llap_queue_name + ".maximum-capacity", llap_queue_cap_perc)
                    putCapSchedProperty("yarn.scheduler.capacity.root." + llap_queue_name + ".capacity", llap_queue_cap_perc)
                    putCapSchedProperty("yarn.scheduler.capacity.root." + llap_queue_name + ".acl_submit_applications", hive_user)
                    putCapSchedProperty("yarn.scheduler.capacity.root." + llap_queue_name + ".acl_administer_queue", hive_user)
                    putCapSchedProperty("yarn.scheduler.capacity.root." + llap_queue_name + ".maximum-am-resource-percent", "1")

                    self.logger.info("Updated 'capacity-scheduler' configs as a dictionary.")
                    updated_cap_sched_configs_as_dict = True

                if updated_cap_sched_configs_str or updated_cap_sched_configs_as_dict:
                    if len(leafQueueNames) == 1: # 'llap' queue didn't exist before
                        self.logger.info("Created YARN Queue : '{0}' with capacity : {1}%. Adjusted 'default' queue capacity to : {2}%" \
                                         .format(llap_queue_name, llap_queue_cap_perc, adjusted_default_queue_cap))
                    else: # Queue existed, only adjustments done.
                        self.logger.info("Adjusted YARN Queue : '{0}'. Current capacity : {1}%. State: RUNNING.".format(llap_queue_name, llap_queue_cap_perc))
                        self.logger.info("Adjusted 'default' queue capacity to : {0}%".format(adjusted_default_queue_cap))

                    # Update Hive 'hive.llap.daemon.queue.name' prop to use 'llap' queue.
                    putHiveInteractiveSiteProperty('hive.llap.daemon.queue.name', llap_queue_name)
                    putHiveInteractiveSiteProperty('hive.server2.tez.default.queues', llap_queue_name)
                    # Update 'hive.llap.daemon.queue.name' prop combo entries and llap capacity slider visibility.
                    self.setLlapDaemonQueuePropAttributes(services, configurations)
            else:
                self.logger.debug("Not creating/adjusting {0} queue. Current YARN queues : {1}".format(llap_queue_name, list(leafQueueNames)))
        else:
            self.logger.error("Couldn't retrieve 'capacity-scheduler' properties while doing YARN queue adjustment for Hive Server Interactive.")

    """
    Checks and sees (1). If only two leaf queues exist at root level, namely: 'default' and 'llap',
                and (2). 'llap' is in RUNNING state.
    
    If yes, performs the following actions:   (1). 'llap' queue state set to STOPPED,
                                              (2). 'llap' queue capacity set to 0 %,
                                              (3). 'default' queue capacity set to 100 %
    """
    def checkAndStopLlapQueue(self, services, configurations, llap_queue_name):
        putCapSchedProperty = self.putProperty(configurations, "capacity-scheduler", services)
        putHiveInteractiveSiteProperty = self.putProperty(configurations, 'hive-interactive-site', services)
        capacity_scheduler_properties, received_as_key_value_pair = self.getCapacitySchedulerProperties(services)
        updated_default_queue_configs = ''
        updated_llap_queue_configs = ''
        if capacity_scheduler_properties:
            # Get all leaf queues.
            leafQueueNames = self.getAllYarnLeafQueues(capacity_scheduler_properties)

            if len(leafQueueNames) == 2 and llap_queue_name in leafQueueNames and 'default' in leafQueueNames:
                # Get 'llap' queue state
                currLlapQueueState = 'STOPPED'
                if 'yarn.scheduler.capacity.root.'+llap_queue_name+'.state' in capacity_scheduler_properties.keys():
                    currLlapQueueState = capacity_scheduler_properties.get('yarn.scheduler.capacity.root.'+llap_queue_name+'.state')
                else:
                    self.logger.error("{0} queue 'state' property not present in capacity scheduler. Skipping adjusting queues.".format(llap_queue_name))
                    return
                if currLlapQueueState == 'RUNNING':
                    DEFAULT_MAX_CAPACITY = '100'
                    for prop, val in capacity_scheduler_properties.items():
                        # Update 'default' related configs in 'updated_default_queue_configs'
                        if llap_queue_name not in prop:
                            if prop == 'yarn.scheduler.capacity.root.default.capacity':
                                # Set 'default' capacity back to maximum val
                                updated_default_queue_configs = updated_default_queue_configs \
                                                                + prop + "="+DEFAULT_MAX_CAPACITY + "\n"
                            elif prop == 'yarn.scheduler.capacity.root.default.maximum-capacity':
                                # Set 'default' max. capacity back to maximum val
                                updated_default_queue_configs = updated_default_queue_configs \
                                                                + prop + "="+DEFAULT_MAX_CAPACITY + "\n"
                            elif prop == 'yarn.scheduler.capacity.root.ordering-policy':
                                # Don't set this property. The default will be picked up.
                                pass
                            elif prop.startswith('yarn.'):
                                updated_default_queue_configs = updated_default_queue_configs + prop + "=" + val + "\n"
                        else: # Update 'llap' related configs in 'updated_llap_queue_configs'
                            if prop == 'yarn.scheduler.capacity.root.'+llap_queue_name+'.state':
                                updated_llap_queue_configs = updated_llap_queue_configs \
                                                             + prop + "=STOPPED\n"
                            elif prop == 'yarn.scheduler.capacity.root.'+llap_queue_name+'.capacity':
                                updated_llap_queue_configs = updated_llap_queue_configs \
                                                             + prop + "=0\n"
                            elif prop == 'yarn.scheduler.capacity.root.'+llap_queue_name+'.maximum-capacity':
                                updated_llap_queue_configs = updated_llap_queue_configs \
                                                             + prop + "=0\n"
                            elif prop.startswith('yarn.'):
                                updated_llap_queue_configs = updated_llap_queue_configs + prop + "=" + val + "\n"
                else:
                    self.logger.debug("{0} queue state is : {1}. Skipping adjusting queues.".format(llap_queue_name, currLlapQueueState))
                    return

                if updated_default_queue_configs and updated_llap_queue_configs:
                    putCapSchedProperty("capacity-scheduler", updated_default_queue_configs+updated_llap_queue_configs)
                    self.logger.info("Changed YARN '{0}' queue state to 'STOPPED', and capacity to 0%. Adjusted 'default' queue capacity to : {1}%" \
                                     .format(llap_queue_name, DEFAULT_MAX_CAPACITY))

                    # Update Hive 'hive.llap.daemon.queue.name' prop to use 'default' queue.
                    putHiveInteractiveSiteProperty('hive.llap.daemon.queue.name', self.YARN_ROOT_DEFAULT_QUEUE_NAME)
                    putHiveInteractiveSiteProperty('hive.server2.tez.default.queues', self.YARN_ROOT_DEFAULT_QUEUE_NAME)
            else:
                self.logger.debug("Not removing '{0}' queue as number of Queues not equal to 2. Current YARN queues : {1}".format(llap_queue_name, list(leafQueueNames)))
        else:
            self.logger.error("Couldn't retrieve 'capacity-scheduler' properties while doing YARN queue adjustment for Hive Server Interactive.")

    def setLlapDaemonQueuePropAttributes(self, services, configurations):
        """
        Checks and sets the 'Hive Server Interactive' 'hive.llap.daemon.queue.name' config Property Attributes.  Takes into
        account that 'capacity-scheduler' may have changed (got updated) in current Stack Advisor invocation.
        """
        self.logger.info("Determining 'hive.llap.daemon.queue.name' config Property Attributes.")
        #TODO Determine if this is doing the right thing if some queue is setup with capacity=0, or is STOPPED. Maybe don't list it.
        putHiveInteractiveSitePropertyAttribute = self.putPropertyAttribute(configurations, 'hive-interactive-site')

        capacity_scheduler_properties = dict()

        # Read 'capacity-scheduler' from configurations if we modified and added recommendation to it, as part of current
        # StackAdvisor invocation.
        if "capacity-scheduler" in configurations:
            cap_sched_props_as_dict = configurations["capacity-scheduler"]["properties"]
            if 'capacity-scheduler' in cap_sched_props_as_dict:
                cap_sched_props_as_str = configurations['capacity-scheduler']['properties']['capacity-scheduler']
                if cap_sched_props_as_str:
                    cap_sched_props_as_str = str(cap_sched_props_as_str).split('\n')
                    if len(cap_sched_props_as_str) > 0 and cap_sched_props_as_str[0] != 'null':
                        # Got 'capacity-scheduler' configs as one "\n" separated string
                        for property in cap_sched_props_as_str:
                            key, sep, value = property.partition("=")
                            capacity_scheduler_properties[key] = value
                        self.logger.info("'capacity-scheduler' configs is set as a single '\\n' separated string in current invocation. "
                                         "count(configurations['capacity-scheduler']['properties']['capacity-scheduler']) = "
                                         "{0}".format(len(capacity_scheduler_properties)))
                    else:
                        self.logger.info("Read configurations['capacity-scheduler']['properties']['capacity-scheduler'] is : {0}".format(cap_sched_props_as_str))
                else:
                    self.logger.info("configurations['capacity-scheduler']['properties']['capacity-scheduler'] : {0}.".format(cap_sched_props_as_str))

            # if 'capacity_scheduler_properties' is empty, implies we may have 'capacity-scheduler' configs as dictionary
            # in configurations, if 'capacity-scheduler' changed in current invocation.
            if not capacity_scheduler_properties:
                if isinstance(cap_sched_props_as_dict, dict) and len(cap_sched_props_as_dict) > 1:
                    capacity_scheduler_properties = cap_sched_props_as_dict
                    self.logger.info("'capacity-scheduler' changed in current Stack Advisor invocation. Retrieved the configs as dictionary from configurations.")
                else:
                    self.logger.info("Read configurations['capacity-scheduler']['properties'] is : {0}".format(cap_sched_props_as_dict))
        else:
            self.logger.info("'capacity-scheduler' not modified in the current Stack Advisor invocation.")


        # if 'capacity_scheduler_properties' is still empty, implies 'capacity_scheduler' wasn't change in current
        # SA invocation. Thus, read it from input : 'services'.
        if not capacity_scheduler_properties:
            capacity_scheduler_properties, received_as_key_value_pair = self.getCapacitySchedulerProperties(services)
            self.logger.info("'capacity-scheduler' not changed in current Stack Advisor invocation. Retrieved the configs from services.")

        # Get set of current YARN leaf queues.
        leafQueueNames = self.getAllYarnLeafQueues(capacity_scheduler_properties)
        if leafQueueNames:
            leafQueues = [{"label": str(queueName), "value": queueName} for queueName in leafQueueNames]
            leafQueues = sorted(leafQueues, key=lambda q: q['value'])
            putHiveInteractiveSitePropertyAttribute("hive.llap.daemon.queue.name", "entries", leafQueues)
            self.logger.info("'hive.llap.daemon.queue.name' config Property Attributes set to : {0}".format(leafQueues))
        else:
            self.logger.error("Problem retrieving YARN queues. Skipping updating HIVE Server Interactve "
                              "'hive.server2.tez.default.queues' property attributes.")

    def __getQueueCapacityKeyFromCapacityScheduler(self, capacity_scheduler_properties, llap_daemon_selected_queue_name):
        """
        Retrieves the passed in queue's 'capacity' related key from Capacity Scheduler.
        """
        # Identify the key which contains the capacity for 'llap_daemon_selected_queue_name'.
        cap_sched_keys = capacity_scheduler_properties.keys()
        llap_selected_queue_cap_key =  None
        current_selected_queue_for_llap_cap = None
        for key in cap_sched_keys:
            # Expected capacity prop key is of form : 'yarn.scheduler.capacity.<one or more queues in path separated by '.'>.[llap_daemon_selected_queue_name].capacity'
            if key.endswith(llap_daemon_selected_queue_name+".capacity") and key.startswith("yarn.scheduler.capacity.root"):
                self.logger.info("DBG: Selected queue name as: " + key)
                llap_selected_queue_cap_key = key
                break;
        return llap_selected_queue_cap_key

    def __getQueueStateFromCapacityScheduler(self, capacity_scheduler_properties, llap_daemon_selected_queue_name):
        """
        Retrieves the passed in queue's 'state' from Capacity Scheduler.
        """
        # Identify the key which contains the state for 'llap_daemon_selected_queue_name'.
        cap_sched_keys = capacity_scheduler_properties.keys()
        llap_selected_queue_state_key =  None
        llap_selected_queue_state = None
        for key in cap_sched_keys:
            if key.endswith(llap_daemon_selected_queue_name+".state"):
                llap_selected_queue_state_key = key
                break;
        llap_selected_queue_state = capacity_scheduler_properties.get(llap_selected_queue_state_key)
        return llap_selected_queue_state

    def __getQueueAmFractionFromCapacityScheduler(self, capacity_scheduler_properties, llap_daemon_selected_queue_name):
        """
        Retrieves the passed in queue's 'AM fraction' from Capacity Scheduler. Returns default value of 0.1 if AM Percent
        pertaining to passed-in queue is not present.
        """
        # Identify the key which contains the AM fraction for 'llap_daemon_selected_queue_name'.
        cap_sched_keys = capacity_scheduler_properties.keys()
        llap_selected_queue_am_percent_key = None
        for key in cap_sched_keys:
            if key.endswith("."+llap_daemon_selected_queue_name+".maximum-am-resource-percent"):
                llap_selected_queue_am_percent_key = key
                self.logger.info("AM percent key got for '{0}' queue is : '{1}'".format(llap_daemon_selected_queue_name, llap_selected_queue_am_percent_key))
                break;
        if llap_selected_queue_am_percent_key is None:
            self.logger.info("Returning default AM percent value : '0.1' for queue : {0}".format(llap_daemon_selected_queue_name))
            return 0.1 # Default value to use if we couldn't retrieve queue's corresponding AM Percent key.
        else:
            llap_selected_queue_am_percent = capacity_scheduler_properties.get(llap_selected_queue_am_percent_key)
            self.logger.info("Returning read value for key '{0}' as : '{1}' for queue : '{2}'".format(llap_selected_queue_am_percent_key,
                                                                                                      llap_selected_queue_am_percent,
                                                                                                      llap_daemon_selected_queue_name))
            return llap_selected_queue_am_percent

    def __getSelectedQueueTotalCap(self, capacity_scheduler_properties, llap_daemon_selected_queue_name, total_cluster_capacity):
        """
        Calculates the total available capacity for the passed-in YARN queue of any level based on the percentages.
        """
        self.logger.info("Entered __getSelectedQueueTotalCap fn() with llap_daemon_selected_queue_name= '{0}'.".format(llap_daemon_selected_queue_name))
        available_capacity = total_cluster_capacity
        queue_cap_key = self.__getQueueCapacityKeyFromCapacityScheduler(capacity_scheduler_properties, llap_daemon_selected_queue_name)
        if queue_cap_key:
            queue_cap_key = queue_cap_key.strip()
            if len(queue_cap_key) >= 34:  # len('yarn.scheduler.capacity.<single letter queue name>.capacity') = 34
                # Expected capacity prop key is of form : 'yarn.scheduler.capacity.<one or more queues (path)>.capacity'
                queue_path = queue_cap_key[24:]  # Strip from beginning 'yarn.scheduler.capacity.'
                queue_path = queue_path[0:-9]  # Strip from end '.capacity'
                queues_list = queue_path.split('.')
                self.logger.info("Queue list : {0}".format(queues_list))
                if queues_list:
                    for queue in queues_list:
                        queue_cap_key = self.__getQueueCapacityKeyFromCapacityScheduler(capacity_scheduler_properties, queue)
                        queue_cap_perc = float(capacity_scheduler_properties.get(queue_cap_key))
                        available_capacity = queue_cap_perc / 100 * available_capacity
                        self.logger.info("Total capacity available for queue {0} is : {1}".format(queue, available_capacity))

        # returns the capacity calculated for passed-in queue in 'llap_daemon_selected_queue_name'.
        return available_capacity

    """
      Calculate YARN config 'apptimelineserver_heapsize' in MB.
    """
    def calculate_yarn_apptimelineserver_heapsize(self, host_mem, yarn_timeline_app_cache_size):
        ats_heapsize = None
        if host_mem < 4096:
            ats_heapsize = 1024
        else:
            ats_heapsize = long(min(math.floor(host_mem/2), long(yarn_timeline_app_cache_size) * 500 + 3072))
        return ats_heapsize

    """
    Calculates for YARN config 'yarn.timeline-service.entity-group-fs-store.app-cache-size', based on YARN's NodeManager size.
    """
    def calculate_yarn_apptimelineserver_cache_size(self, host_mem):
        yarn_timeline_app_cache_size = None
        if host_mem < 4096:
            yarn_timeline_app_cache_size = 3
        elif host_mem >= 4096 and host_mem < 8192:
            yarn_timeline_app_cache_size = 7
        elif host_mem >= 8192:
            yarn_timeline_app_cache_size = 10
        self.logger.info("Calculated and returning 'yarn_timeline_app_cache_size' : {0}".format(yarn_timeline_app_cache_size))
        return yarn_timeline_app_cache_size


    """
    Reads YARN config 'yarn.timeline-service.entity-group-fs-store.app-cache-size'.
    """
    def read_yarn_apptimelineserver_cache_size(self, services):
        """
        :type services dict
        :rtype str
        """
        yarn_ats_app_cache_size = None
        yarn_ats_app_cache_size_config = "yarn.timeline-service.entity-group-fs-store.app-cache-size"
        yarn_site_in_services = self.getServicesSiteProperties(services, "yarn-site")

        if yarn_site_in_services and yarn_ats_app_cache_size_config in yarn_site_in_services:
            yarn_ats_app_cache_size = yarn_site_in_services[yarn_ats_app_cache_size_config]
            self.logger.info("'yarn.scheduler.minimum-allocation-mb' read from services as : {0}".format(yarn_ats_app_cache_size))

        if not yarn_ats_app_cache_size:
            self.logger.error("'{0}' was not found in the services".format(yarn_ats_app_cache_size_config))

        return yarn_ats_app_cache_size
