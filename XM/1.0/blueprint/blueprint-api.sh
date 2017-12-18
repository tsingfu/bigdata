#/bin/bash

# list blueprints
# curl -H "X-Requested-By: ambari" -X GET -u admin:admin localhost:8080/api/v1/blueprints

# list hosts
# curl -H "X-Requested-By: ambari" -X GET -u admin:admin localhost:8080/api/v1/hosts

# export blueprint
# curl -H "X-Requested-By: ambari" -X GET -u admin:admin localhost:8080/api/v1/clusters/spark?format=blueprint

# import blueprint
# curl -H "X-Requested-By: ambari" -X POST -d @spark/spark.json -u admin:admin localhost:8080/api/v1/blueprints/spark

# create cluster
# curl -H "X-Requested-By: ambari" -X POST -d @spark/spark-template.json -u admin:admin localhost:8080/api/v1/clusters/spark

