#!/usr/bin/env bash

mysqldservice=$1
mysqldbuser=$2
mysqldbpasswd=$3
userhost=$4

# The restart (not start) is required to pick up mysql configuration changes made by sed
# during install, in case mysql is already started. The changes are required by Hive later on.
/var/lib/ambari-agent/ambari-sudo.sh service $mysqldservice restart

# MySQL 5.7 installed in non-interactive way uses a socket authentication plugin.
# "mysql -u root" should be executed from root user
echo "Adding user $mysqldbuser@% and removing users with empty name"
/var/lib/ambari-agent/ambari-sudo.sh mysql -u root -e "CREATE USER '$mysqldbuser'@'%' IDENTIFIED BY '$mysqldbpasswd';"
/var/lib/ambari-agent/ambari-sudo.sh mysql -u root -e "GRANT ALL PRIVILEGES ON *.* TO '$mysqldbuser'@'%';"
/var/lib/ambari-agent/ambari-sudo.sh mysql -u root -e "DELETE FROM mysql.user WHERE user='';"
/var/lib/ambari-agent/ambari-sudo.sh mysql -u root -e "flush privileges;"
/var/lib/ambari-agent/ambari-sudo.sh service $mysqldservice stop
