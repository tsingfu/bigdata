#!/usr/bin/env python

import re


def escape_yaml_propetry(value):
    unquouted = False
    unquouted_values = ["null", "Null", "NULL", "true", "True", "TRUE", "false", "False", "FALSE", "YES", "Yes", "yes",
                        "NO", "No", "no", "ON", "On", "on", "OFF", "Off", "off"]
    if value in unquouted_values:
        unquouted = True

    # if is list [a,b,c]
    if re.match('^\w*\[.+\]\w*$', value):
        unquouted = True

    try:
        int(value)
        unquouted = True
    except ValueError:
        pass

    try:
        float(value)
        unquouted = True
    except ValueError:
        pass

    if not unquouted:
        value = value.replace("'", "''")
        value = "'" + value + "'"

    return value
