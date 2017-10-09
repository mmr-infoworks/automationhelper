from simpleutils import *
from config_utils import *
import MySQLdb
import sys

schemaSqlTmpl = """ 
SELECT  COLUMN_NAME,TYPE_NAME 
FROM COLUMNS_V2  c JOIN TBLS t
ON c.CD_ID = t.TBL_ID AND
LOWER(t.TBL_NAME) = LOWER('{}')
JOIN DBS d on d.DB_ID = t.DB_ID 
AND LOWER(d.NAME) = LOWER('{}')
order by c.INTEGER_IDX 
"""  

partitionSqlTmpl = """ 
SELECT  p.PART_NAME from PARTITIONS p JOIN TBLS t
on p.TBL_ID = t.TBL_ID 
and LOWER(t.TBL_NAME) = LOWER('{}')
JOIN DBS d on d.DB_ID = t.DB_ID 
AND LOWER(d.NAME) = LOWER('{}')
order by p.PART_ID 
"""
def getvalidationmap():
    validationMap={   "type": "HIVE",   "hive_schema_name": "automation",   "hive_table_name": "REPLACEFROMINPUT",   "ignore_columns": [     "ziw_created_timestamp",     "ziw_updated_timestamp"   ],      "partitions": [   ],   "cluster_columns": [     "ziw_row_id"   ],   "number_of_secondary_partitions": 1,   "actual_data_path": "###ROSIE_DATASET_PATH###",   "hive_conf": "###HIVE_DB_CONF###" }
    return validationMap

def createvalidationmap(targettable):
    validationJsonMap = getvalidationmap()
    tableName = targettable['name']
    schema = targettable['schema']
    db = MySQLdb.connect(HIVE_METASTORE_HOST,HIVE_METASTORE_USER,HIVE_METASTORE_PWD,HIVE_METASTORE_DB ) 
    schemaSql = schemaSqlTmpl.format(tableName,schema)
    partitionSql = partitionSqlTmpl.format(tableName,schema)
    columns = []
    partitions = []
    cursorSchema = db.cursor()
    cursorPartition = db.cursor()
    try:
        cursorSchema.execute(schemaSql)
        results=cursorSchema.fetchall()
        for row in results:
            columns.append({"name":row[0],"type":row[1]})
        cursorPartition.execute(partitionSql)
        results = cursorPartition.fetchall()
        for row in results:
            partitions.append(row[0])
    except:
       print "error in sql "  +        schemaSql
    db.close()   
    validationJsonMap['columns'] = columns
    validationJsonMap['partitions'] = partitions
    validationJsonMap['hive_table_name'] = tableName
    validationJsonMap['hive_schema_name'] = schema
    return validationJsonMap


if __name__ == "__main__":
    targetTable ={}
    targetTable['name'] = sys.argv[1]
    targetTable['schema'] = sys.argv[2]
    json = createvalidationmap(targetTable)
    print json_pretty(json)