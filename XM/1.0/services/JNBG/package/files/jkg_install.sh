#!/usr/bin/env bash

set -x

PY_EXEC=$1
PY_VENV_PATH_PREFIX=$2
PY_VENV_OWNER=$3
KINIT_CMD=$4

checkPipInstall()
{
  pip show $1 2>&1 > /dev/null
}

checkSuccess()
{
  if [ $? != 0 ]; then
    set +x
    echo "Error encountered at line $1 while attempting to: "
    if [ -n "$2" ]; then
      echo $2
    fi
    echo Exiting.
    exit 1
  fi
  set -x
}

PLATFORM=`uname -p`
rhver=7

if [ "$PLATFORM" == "x86_64" ]
then
  if [ -x /usr/bin/lsb_release ]; then
    rhver=$(/usr/bin/lsb_release -rs | cut -f1 -d.)
  fi
fi

pyver=`echo $(${PY_EXEC} -V 2>&1 | awk '{ print $2 }') | sed -e 's/\.//g'`
if [ "$pyver" -lt 270 ]; then
  echo "Installation failed; Ensure that the specified python_interpreter_path is Python version 2.7."
  exit 1
fi

easy_install pip
checkSuccess $LINENO "-  easy_install pip"
pip -V

pip install virtualenv --upgrade
checkPipInstall virtualenv
checkSuccess $LINENO "-  pip install virtualenv"

if [ -d "${PY_VENV_PATH_PREFIX}/python2.7" ]; then
  # Warning only to tolerate pre-existing virtual env. from failed installs
  echo "Installation warning: ${PY_VENV_PATH_PREFIX}/python2.7 exists."
  echo "This might indicate remnants from a prior or failed installation."
  echo "Check specified property value for python_virtualenv_path_prefix."
fi

if [ ! -x "${PY_EXEC}" ]; then
  echo "Installation failed: ${PY_EXEC} does not appear to be a valid python executable; Use a different python_interpreter_path."
  exit 1
fi

virtualenv -p ${PY_EXEC} ${PY_VENV_PATH_PREFIX}/python2.7
checkSuccess $LINENO "-  create virtualenv using ${PY_EXEC}"

source ${PY_VENV_PATH_PREFIX}/python2.7/bin/activate
pip -V

# Use --index-url and not --extra-index-url as we are trying to install
# specific package versions
pip install setuptools --upgrade
checkPipInstall setuptools
checkSuccess $LINENO "- pip install setuptools"

# Using --upgrade enables updating missing dependencies after failed installs
pip install jupyter_kernel_gateway --upgrade
checkPipInstall jupyter_kernel_gateway
checkSuccess $LINENO "- pip install jupyter_kernel_gateway"

# Set ownership of the created virtualenv if configured via python_virtualenv_restrictive
if [ "${PY_VENV_OWNER}" != "root" ]; then
  echo ====== Virtualenv owner = $PY_VENV_OWNER =========
  chown -R ${PY_VENV_OWNER}: ${PY_VENV_PATH_PREFIX}/python2.7
fi
