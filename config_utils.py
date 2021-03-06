
HDFS_HOST='54.159.107.60'
HDFS_PORT='50070'
HDFS_USER='hdfs'
DOMAIN = {  "domain": { "name": "DF_TPC_DS", "description": "DF tests using TPC-DS data"  },  "users": [    "manohar@infoworks.io"  ],  "sources": [    "DF_TPC_DS" ]}
HIVE_METASTORE_HOST ='127.0.0.1'
HIVE_METASTORE_USER='root'
HIVE_METASTORE_PWD=''
HIVE_METASTORE_DB='cdh2'

DB_CFG ="""[hive]
hive_driver = org.apache.hive.jdbc.HiveDriver
hive_ip = localhost
hive_port = 10000
hive_username =
hive_password =
hive_schema = default
hive_config_var =
"""

MONGO_HOST='54.159.107.60'
MONGO_PORT=27017
MONGO_USER='infoworks'
MONGO_PWD='IN11**rk'
MONGO_DBNAME='infoworks-new'
