
HDFS_HOST='34.207.139.167'
HDFS_PORT='50070'
HDFS_USER='hdfs'
DOMAIN = {  "domain": { "name": "AutomationTests", "description": "DF tests using TPC-DS data"  },  "users": [    "manohar@infoworks.io"  ],  "sources": [    "DF_TPC_DS" ]}
HIVE_METASTORE_HOST ='127.0.0.1'
HIVE_METASTORE_USER='root'
HIVE_METASTORE_PWD=''
HIVE_METASTORE_DB='dev1'

DB_CFG ="""[hive]
hive_driver = org.apache.hive.jdbc.HiveDriver
hive_ip = localhost
hive_port = 10000
hive_username =
hive_password =
hive_schema = default
hive_config_var =
"""

MONGO_HOST='34.207.139.167'
MONGO_PORT=27017
MONGO_USER='infoworks'
MONGO_PWD='IN11**rk'
MONGO_DBNAME='infoworks-new'