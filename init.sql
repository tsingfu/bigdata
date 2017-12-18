CREATE DATABASE IF NOT EXISTS Ambari DEFAULT CHARSET utf8;
GRANT ALL ON Ambari.* TO 'ambari'@'%' IDENTIFIED BY 'XM_example.com_123';
GRANT ALL PRIVILEGES ON Ambari.* TO 'ambari'@'%' WITH GRANT OPTION;
USE Ambari;
SOURCE /var/lib/ambari-server/resources/Ambari-DDL-MySQL-CREATE.sql;

#ranger
CREATE DATABASE IF NOT EXISTS ranger DEFAULT CHARACTER SET utf8;
GRANT ALL PRIVILEGES ON ranger.* TO 'rangeradmin'@'%' IDENTIFIED BY 'XM_example.com_123';

#druid
CREATE DATABASE IF NOT EXISTS druid DEFAULT CHARACTER SET utf8;
GRANT ALL ON druid.* TO 'druid'@'%'IDENTIFIED BY 'XM_example.com_123';
CREATE DATABASE IF NOT EXISTS superset DEFAULT CHARACTER SET utf8;
GRANT ALL PRIVILEGES ON superset.* TO 'superset'@'%' IDENTIFIED BY 'XM_example.com_123';

#airflow
CREATE DATABASE IF NOT EXISTS airflow DEFAULT CHARACTER SET utf8;
GRANT ALL PRIVILEGES ON airflow.* TO 'airflow'@'%' IDENTIFIED BY 'XM_example.com_123';

#hdf
CREATE DATABASE IF NOT EXISTS registry DEFAULT CHARACTER SET utf8;
CREATE DATABASE IF NOT EXISTS streamline DEFAULT CHARACTER SET utf8;
GRANT ALL PRIVILEGES ON registry.* TO 'registry'@'%' IDENTIFIED BY 'XM_example.com_123' WITH GRANT OPTION;
GRANT ALL PRIVILEGES ON streamline.* TO 'streamline'@'%' IDENTIFIED BY 'XM_example.com_123'WITH GRANT OPTION;

FLUSH PRIVILEGES;

#delete hdp-select alert
USE Ambari;
delete from alert_current where definition_id in (select definition_id  from alert_definition WHERE definition_name='ambari_agent_version_select');
delete from alert_grouping  where definition_id in (select definition_id  from alert_definition WHERE definition_name='ambari_agent_version_select');
delete from alert_history where alert_definition_id in (select definition_id  from alert_definition WHERE definition_name='ambari_agent_version_select');
DELETE FROM alert_definition WHERE definition_name='ambari_agent_version_select';