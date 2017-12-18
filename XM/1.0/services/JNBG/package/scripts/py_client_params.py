#!/usr/bin/env python

from resource_management.libraries.script.script import Script
from resource_management.libraries.functions.format import format
from jkg_toree_params import py_executable, py_venv_pathprefix, py_venv_restrictive, venv_owner, ambarisudo
import jnbg_helpers as helpers

# Server configurations
config = Script.get_config()
stack_root = '/opt'

package_dir = helpers.package_dir()
cmd_file_name = "pythonenv_setup.sh"
cmd_file_path = format("{package_dir}files/{cmd_file_name}")

# Sequence of commands executed in py_client.py
commands = [ambarisudo + ' ' +
            cmd_file_path + ' ' +
            py_executable + ' ' +
            py_venv_pathprefix + ' ' +
            venv_owner]
