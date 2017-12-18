# -*- coding: utf-8 -*-
import os
import ast

from resource_management.core.resources.system import Execute


def create_connectors(node_properties, connectors_to_add):
    if not connectors_to_add:
        return
    Execute('mkdir -p {0}'.format(node_properties['plugin.config-dir']))
    connectors_dict = ast.literal_eval(connectors_to_add)
    for connector in connectors_dict:
        connector_file = os.path.join(node_properties['plugin.config-dir'], connector + '.properties')
        with open(connector_file, 'w') as f:
            for lineitem in connectors_dict[connector]:
                f.write('{0}\n'.format(lineitem))


def delete_connectors(node_properties, connectors_to_delete):
    if not connectors_to_delete:
        return
    connectors_list = ast.literal_eval(connectors_to_delete)
    for connector in connectors_list:
        connector_file_name = os.path.join(node_properties['plugin.config-dir'], connector + '.properties')
        Execute('rm -f {0}'.format(connector_file_name))
