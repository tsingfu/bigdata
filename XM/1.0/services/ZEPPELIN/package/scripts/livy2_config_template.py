#!/usr/bin/env python

template = '''
{
  "id": "2C8A4SZ9T_livy2",
  "status": "READY",
  "group": "livy",
  "name": "livy2",
  "properties": {
    "zeppelin.livy.keytab": "",
    "zeppelin.livy.spark.sql.maxResult": "1000",
    "livy.spark.executor.instances": "",
    "livy.spark.executor.memory": "",
    "livy.spark.dynamicAllocation.enabled": "",
    "livy.spark.dynamicAllocation.cachedExecutorIdleTimeout": "",
    "livy.spark.dynamicAllocation.initialExecutors": "",
    "zeppelin.livy.session.create_timeout": "120",
    "livy.spark.driver.memory": "",
    "zeppelin.livy.displayAppInfo": "false",
    "livy.spark.jars.packages": "",
    "livy.spark.dynamicAllocation.maxExecutors": "",
    "zeppelin.livy.concurrentSQL": "false",
    "zeppelin.livy.principal": "",
    "livy.spark.executor.cores": "",
    "zeppelin.livy.url": "http://localhost:8998",
    "zeppelin.livy.pull_status.interval.millis": "1000",
    "livy.spark.driver.cores": "",
    "livy.spark.dynamicAllocation.minExecutors": ""
  },
  "interpreterGroup": [
    {
      "class": "org.apache.zeppelin.livy.LivySparkInterpreter",
      "editor": {
        "editOnDblClick": false,
        "language": "scala"
      },
      "name": "spark",
      "defaultInterpreter": false
    },
    {
      "class": "org.apache.zeppelin.livy.LivySparkSQLInterpreter",
      "editor": {
        "editOnDblClick": false,
        "language": "sql"
      },
      "name": "sql",
      "defaultInterpreter": false
    },
    {
      "class": "org.apache.zeppelin.livy.LivyPySparkInterpreter",
      "editor": {
        "editOnDblClick": false,
        "language": "python"
      },
      "name": "pyspark",
      "defaultInterpreter": false
              },
    {
      "class": "org.apache.zeppelin.livy.LivyPySpark3Interpreter",
      "editor": {
        "editOnDblClick": false,
        "language": "python"
      },
      "name": "pyspark3",
      "defaultInterpreter": false
    },
    {
      "class": "org.apache.zeppelin.livy.LivySparkRInterpreter",
      "editor": {
        "editOnDblClick": false,
        "language": "r"
      },
      "name": "sparkr",
      "defaultInterpreter": false
    }
  ],
  "dependencies": [],
  "option": {
    "setPermission": false,
    "remote": true,
    "users": [],
    "isExistingProcess": false,
    "perUser": "scoped",
    "isUserImpersonate": false,
    "perNote": "shared",
    "port": -1
  }
}
'''
