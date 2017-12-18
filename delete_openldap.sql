use Ambari;
delete from hostcomponentstate where service_name in ('KRB5','OPENLDAP');
delete from hostcomponentdesiredstate where service_name in ('KRB5','OPENLDAP');
delete from servicecomponentdesiredstate where service_name in ('KRB5','OPENLDAP');
delete from clusterservices where service_name in ("OPENLDAP","KRB5");
delete from serviceconfig where service_name in ('OPENLDAP','KRB5');