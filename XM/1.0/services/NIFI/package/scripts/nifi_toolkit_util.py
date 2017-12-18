#!/usr/bin/env python
"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

import json, nifi_constants, os
from resource_management.core import sudo
from resource_management.core.resources.system import File
from resource_management.core.utils import PasswordString

import params
files_dir = params.install_dir_toolkit


def load(config_json):
    if sudo.path_isfile(config_json):
        contents = sudo.read_file(config_json)
        if len(contents) > 0:
            return json.loads(contents)
    return {}


def dump(config_json, config_dict, nifi_user, nifi_group):
    File(config_json,
         owner=nifi_user,
         group=nifi_group,
         mode=0600,
         content=PasswordString(json.dumps(config_dict, sort_keys=True, indent=4))
         )


def overlay(config_dict, overlay_dict):
    for k, v in overlay_dict.iteritems():
        if (k not in config_dict) or not (overlay_dict[k] == config_dict[k]):
            config_dict[k] = v


def get_toolkit_script(scriptName, scriptDir=files_dir):
    nifiToolkitDir = None
    for dir in os.listdir(scriptDir):
        if dir.startswith('nifi-toolkit-'):
            nifiToolkitDir = os.path.join(scriptDir, dir)

    if nifiToolkitDir is None:
        raise Exception("Couldn't find nifi toolkit directory in " + scriptDir)
    result = nifiToolkitDir + '/bin/' + scriptName
    if not sudo.path_isfile(result):
        raise Exception("Couldn't find file " + result)
    return result


def update_nifi_properties(client_dict, nifi_properties):
    nifi_properties[nifi_constants.NIFI_SECURITY_KEYSTORE_TYPE] = client_dict['keyStoreType']
    nifi_properties[nifi_constants.NIFI_SECURITY_KEYSTORE_PASSWD] = client_dict['keyStorePassword']
    nifi_properties[nifi_constants.NIFI_SECURITY_KEY_PASSWD] = client_dict['keyPassword']
    nifi_properties[nifi_constants.NIFI_SECURITY_TRUSTSTORE_TYPE] = client_dict['trustStoreType']
    nifi_properties[nifi_constants.NIFI_SECURITY_TRUSTSTORE_PASSWD] = client_dict['trustStorePassword']


def store_exists(client_dict, key):
    if key not in client_dict:
        return False
    return sudo.path_isfile(client_dict[key])


def different(one, two, key, usingJsonConfig=False):
    if key not in one:
        return False
    if len(one[key]) == 0 and usingJsonConfig:
        return False
    if key not in two:
        return False
    if len(two[key]) == 0 and usingJsonConfig:
        return False
    return one[key] != two[key]


def changed_keystore_truststore(orig_client_dict, new_client_dict, usingJsonConfig=False):
    if not (store_exists(new_client_dict, 'keyStore') or store_exists(new_client_dict, 'trustStore')):
        return False
    elif different(orig_client_dict, new_client_dict, 'keyStoreType', usingJsonConfig):
        return True
    elif different(orig_client_dict, new_client_dict, 'keyStorePassword', usingJsonConfig):
        return True
    elif different(orig_client_dict, new_client_dict, 'keyPassword', usingJsonConfig):
        return True
    elif different(orig_client_dict, new_client_dict, 'trustStoreType', usingJsonConfig):
        return True
    elif different(orig_client_dict, new_client_dict, 'trustStorePassword', usingJsonConfig):
        return True


def move_keystore_truststore(client_dict):
    move_store(client_dict, 'keyStore')
    move_store(client_dict, 'trustStore')


def move_store(client_dict, key):
    if store_exists(client_dict, key):
        num = 0
        name = client_dict[key]
        while sudo.path_isfile(name + '.bak.' + str(num)):
            num += 1
        sudo.copy(name, name + '.bak.' + str(num))
        sudo.unlink(name)


def save_config_version(version_file, version_type, version_num, nifi_user, nifi_group):
    version = {}
    if sudo.path_isfile(version_file):
        contents = sudo.read_file(version_file)
        version = json.loads(contents)
        version[version_type] = version_num
        sudo.unlink(version_file)
    else:
        version[version_type] = version_num

    File(version_file,
         owner=nifi_user,
         group=nifi_group,
         mode=0600,
         content=json.dumps(version))


def get_config_version(version_file, version_type):
    if sudo.path_isfile(version_file):
        contents = sudo.read_file(version_file)
        version = json.loads(contents)
        if version_type in version:
            return version[version_type]
        else:
            return None


def remove_config_version(version_file, version_type, nifi_user, nifi_group):
    if sudo.path_isfile(version_file):
        contents = sudo.read_file(version_file)
        version = json.loads(contents)
        version.pop(version_type, None)
        sudo.unlink(version_file)

        File(version_file,
             owner=nifi_user,
             group=nifi_group,
             mode=0600,
             content=json.dumps(version))


def get_config_by_version(config_path, config_name, version):
    import fnmatch
    if version is not None:
        for file in os.listdir(config_path):
            if fnmatch.fnmatch(file, 'command-*.json'):
                contents = sudo.read_file(config_path + '/' + file)
                version_config = json.loads(contents)
                if config_name in version_config['configurationTags'] and \
                                version_config['configurationTags'][config_name]['tag'] == version:
                    return version_config

    return {}


def convert_properties_to_dict(prop_file):
    dict = {}
    if sudo.path_isfile(prop_file):
        lines = sudo.read_file(prop_file).split('\n')
        for line in lines:
            props = line.rstrip().split('=')
            if len(props) == 2:
                dict[props[0]] = props[1]
            elif len(props) == 1:
                dict[props[0]] = ''
    return dict


def populate_ssl_properties(old_prop, new_prop, params):
    if old_prop and len(old_prop) > 0:

        newKeyPasswd = new_prop['nifi.security.keyPasswd'].replace('{{nifi_keyPasswd}}', params.nifi_keyPasswd)
        newKeystorePasswd = new_prop['nifi.security.keystorePasswd'].replace('{{nifi_keystorePasswd}}',
                                                                             params.nifi_keystorePasswd)
        newTruststorePasswd = new_prop['nifi.security.truststorePasswd'].replace('{{nifi_truststorePasswd}}',
                                                                                 params.nifi_truststorePasswd)

        if len(newKeyPasswd) == 0 and len(old_prop['nifi.security.keyPasswd']) > 0:
            new_prop['nifi.security.keyPasswd'] = old_prop['nifi.security.keyPasswd']
            if 'nifi.security.keyPasswd.protected' in old_prop:
                new_prop['nifi.security.keyPasswd.protected'] = old_prop['nifi.security.keyPasswd.protected']

        if len(newKeystorePasswd) == 0 and len(old_prop['nifi.security.keystorePasswd']) > 0:
            new_prop['nifi.security.keystorePasswd'] = old_prop['nifi.security.keystorePasswd']
            if 'nifi.security.keystorePasswd.protected' in old_prop:
                new_prop['nifi.security.keystorePasswd.protected'] = old_prop['nifi.security.keystorePasswd.protected']

        if len(newTruststorePasswd) == 0 and len(old_prop['nifi.security.truststorePasswd']) > 0:
            new_prop['nifi.security.truststorePasswd'] = old_prop['nifi.security.truststorePasswd']
            if 'nifi.security.truststorePasswd.protected' in old_prop:
                new_prop['nifi.security.truststorePasswd.protected'] = old_prop[
                    'nifi.security.truststorePasswd.protected']

    return new_prop


def get_nifi_ca_client_dict(config, params):
    if len(config) == 0:
        return {}
    else:
        nifi_keystore = config['configurations']['nifi-ambari-ssl-config']['nifi.security.keystore']
        nifi_keystoreType = config['configurations']['nifi-ambari-ssl-config']['nifi.security.keystoreType']
        nifi_keystorePasswd = config['configurations']['nifi-ambari-ssl-config']['nifi.security.keystorePasswd']
        nifi_keyPasswd = config['configurations']['nifi-ambari-ssl-config']['nifi.security.keyPasswd']
        nifi_truststore = config['configurations']['nifi-ambari-ssl-config']['nifi.security.truststore']
        nifi_truststoreType = config['configurations']['nifi-ambari-ssl-config']['nifi.security.truststoreType']
        nifi_truststorePasswd = config['configurations']['nifi-ambari-ssl-config']['nifi.security.truststorePasswd']
        nifi_truststore = nifi_truststore.replace('{nifi_node_ssl_host}', params.nifi_node_host)
        nifi_truststore = nifi_truststore.replace('{{nifi_config_dir}}', params.nifi_config_dir)
        nifi_keystore = nifi_keystore.replace('{nifi_node_ssl_host}', params.nifi_node_host)
        nifi_keystore = nifi_keystore.replace('{{nifi_config_dir}}', params.nifi_config_dir)

        # default keystore/truststore type if empty
        nifi_keystoreType = 'jks' if len(nifi_keystoreType) == 0 else nifi_keystoreType
        nifi_truststoreType = 'jks' if len(nifi_truststoreType) == 0 else nifi_truststoreType

        nifi_toolkit_dn_prefix = config['configurations']['nifi-ambari-ssl-config']['nifi.toolkit.dn.prefix']
        nifi_toolkit_dn_suffix = config['configurations']['nifi-ambari-ssl-config']['nifi.toolkit.dn.suffix']

        nifi_ca_client_config = {
            "days": int(config['configurations']['nifi-ambari-ssl-config']['nifi.toolkit.tls.helper.days']),
            "keyStore": nifi_keystore,
            "keyStoreType": nifi_keystoreType,
            "keyStorePassword": nifi_keystorePasswd,
            "keyPassword": nifi_keyPasswd,
            "token": config['configurations']['nifi-ambari-ssl-config']['nifi.toolkit.tls.token'],
            "dn": nifi_toolkit_dn_prefix + params.nifi_node_host + nifi_toolkit_dn_suffix,
            "port": int(config['configurations']['nifi-ambari-ssl-config']['nifi.toolkit.tls.port']),
            "caHostname": params.nifi_ca_host,
            "trustStore": nifi_truststore,
            "trustStoreType": nifi_truststoreType,
            "trustStorePassword": nifi_truststorePasswd
        }

        return nifi_ca_client_config


def get_last_sensitive_props_key(config_version_file, nifi_properties):
    last_encrypt_config_version = get_config_version(config_version_file, 'encrypt')
    if last_encrypt_config_version:
        last_encrypt_config = get_config_by_version('/var/lib/ambari-agent/data', 'nifi-ambari-config',
                                                    last_encrypt_config_version)
        return last_encrypt_config['configurations']['nifi-ambari-config']['nifi.sensitive.props.key']
    else:
        return nifi_properties['nifi.sensitive.props.key']


def contains_providers(login_provider_file):
    from xml.dom.minidom import parseString
    import xml.dom.minidom

    if sudo.path_isfile(login_provider_file):
        content = sudo.read_file(login_provider_file)
        dom = xml.dom.minidom.parseString(content)
        collection = dom.documentElement
        if collection.getElementsByTagName("provider"):
            return True
        else:
            return False

    else:
        return False
