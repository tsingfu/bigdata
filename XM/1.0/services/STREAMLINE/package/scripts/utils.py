#!/usr/bin/env python

import re


def get_bare_principal(normalized_principal_name):
    """
    Given a normalized principal name returns just the
    primary component (nimbus)
    :param normalized_principal_name: a string containing the principal name to process
    :return: a string containing the primary component value or None if not valid
    """

    bare_principal = None

    if normalized_principal_name:
        match = re.match(r"([^/@]+)(?:/[^@])?(?:@.*)?", normalized_principal_name)

    if match:
        bare_principal = match.group(1)

    return bare_principal
