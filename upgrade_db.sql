alter table request add column cluster_host_info LONGBLOB;

alter table host_role_command  add column is_background SMALLINT DEFAULT 0 NOT NULL;

update `metainfo` set `metainfo_value`='2.5.1' where `metainfo_key`='version';

update `hoststate`  set `agent_version`='{"version":"2.5.1.0"}';