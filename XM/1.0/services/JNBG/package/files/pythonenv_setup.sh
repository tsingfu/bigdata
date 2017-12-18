#!/usr/bin/env bash

set -x

PY_EXEC=$1
PY_VENV_PATH_PREFIX=$2
PY_VENV_OWNER=$3

checkPipInstall()
{
  pip show $1 2>&1 > /dev/null
}

checkSuccess()
{
  if [ $? != 0 ]
  then
    set +x
    echo "Error encountered at line $1 while attempting to: "
    if [ -n "$2" ]
    then
      echo $2
    fi
    echo Exiting.
    exit 1
  fi
  set -x
}

if [ -d "${PY_VENV_PATH_PREFIX}/python2.7" ]; then
  echo "Python client installation detected. Nothing to do."
  exit 0
fi

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

pip install --no-cache-dir virtualenv --upgrade
checkPipInstall virtualenv
checkSuccess $LINENO "-  pip install virtualenv"

virtualenv -p ${PY_EXEC} ${PY_VENV_PATH_PREFIX}/python2.7
checkSuccess $LINENO "-  create virtualenv using ${PY_EXEC}"

# Set ownership of the created virtualenv if configured via python_virtualenv_restrictive
if [ "${PY_VENV_OWNER}" != "root" ]; then
  echo ====== Virtualenv owner = $PY_VENV_OWNER =========
  chown -R ${PY_VENV_OWNER}: ${PY_VENV_PATH_PREFIX}/python2.7
fi
