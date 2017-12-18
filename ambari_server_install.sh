#!/bin/env bash
wget http://yum.example.com/hadoop/hadoop.repo -O /etc/yum.repos.d/hadoop.repo

yum install -y ambari-server krb5-workstation
yum install -y jdk1.8.0_144 unzip curl jdk openldap-clients nss-pam-ldapd pam_ldap pam_krb5 authconfig krb5-libs libcgroup
if [ ! -f /usr/lib/ambari-server/mysql-connector-java-5.1.40-bin.jar ]; then
    wget http://yum.example.com/hadoop/mysql-connector-java-5.1.40-bin.jar -O /usr/lib/ambari-server/mysql-connector-java-5.1.40-bin.jar
fi
if [ ! -f /usr/share/java/mysql-connector-java-5.1.40-bin.jar ] ; then
    wget http://yum.example.com/hadoop/mysql-connector-java-5.1.40-bin.jar -O /usr/share/java/mysql-connector-java-5.1.40-bin.jar
    cp /usr/share/java/mysql-connector-java-5.1.40-bin.jar /usr/share/java/mysql-connector-java.jar
fi

ambari-server setup --jdbc-db=mysql --jdbc-driver=/usr/share/java/mysql-connector-java.jar

wget http://yum.example.com/hadoop/XM.zip -O /tmp/XM.zip
cd /var/lib/ambari-server/resources/stacks &&sudo rm -rf HDP && unzip /tmp/XM.zip && rm -rf /tmp/XM.zip
cp /var/lib/ambari-server/resources/stacks/XM/stack_advisor.py /var/lib/ambari-server/resources/stacks/stack_advisor.py

wget http://yum.example.com/hadoop/jce_policy-8.zip -O /tmp/jce_policy-8.zip
unzip -o -j -q jce_policy-8.zip -d  /usr/java/default/jre/lib/security/
ambari-server setup

chkconfig ambari-server on

ambari-server start

#/usr/sbin/authconfig --enablekrb5 --enableshadow --useshadow --enablelocauthorize --enableldap --enableldapauth --ldapserver="ldap://XM-eagle-32-137.example.com/ ldap://XM-eagle-32-138.example.com/" --ldapbasedn="dc=example,dc=com" --update