from pymongo import *
from config_utils import *
import json

entityMap = {
    "entity_type": "pipeline",
    "entity_id": {
      "$type": "oid",
      "$value": "e361a226e39dca1d0451f374"
    },
    "entity_name": "filter_overwrite"
  }

nameMap= {"name": "filter_overwrite",
  "isAdvancedPipeline": True,
  "description": "",
  "domainId": {
    "$type": "oid",
    "$value": "b957b69324d0e78e4bc05cb7"
  }} 

def getPipelineFromDb(pipelineName):
    try:
        host_base_url = 'mongodb://{user_name}:{password}@{ip}:{port}/{db}'.format(user_name=MONGO_USER,
                                                                              password=MONGO_PWD,
                                                                              ip=MONGO_HOST, port=MONGO_PORT, db=MONGO_DBNAME)
        mongodb = MongoClient(host=host_base_url)[MONGO_DBNAME]
        pipeline=mongodb.pipelines.find_one({"name":pipelineName})
        return pipeline
    except errors.ConnectionFailure,e:
         print "Could not connect to MongoDB: %s" % e

def makePipeLineMap(pipelineName):
    modelMap=getPipelineFromDb(pipelineName)['model']
    pipeLineJsonMap={'pipeline':{'model':modelMap}}
    iwMapArray=createIwMappingArray(modelMap)
    pipeLineJsonMap['iw_mappings']=iwMapArray
    entityMap['entity_name']=pipelineName
    nameMap['name']=pipelineName
    pipeLineJsonMap['entity']=entityMap
    for k in nameMap:
        pipeLineJsonMap[k]=nameMap[k]
    return pipeLineJsonMap    

def createIwMappingArray(modelMap):

    iwMappings=[]
    for node in modelMap['nodes']:
        nodeVal=modelMap['nodes'][node]
        if nodeVal['type'] =='SOURCE_TABLE':
           iwMapping={
                "entity_type": "table",
                "entity_id": {
                    "$type": "oid",
                    "$value": "ADD_HERE"
                },
                "recommendation": {
                    "table_name": "ADD_HERE",
                    "source_name": "DF_TPC_DS"
                }
                } 
           iwMapping['recommendation']['table_name']=nodeVal['table']
           iwMapping['entity_id']['$value']=nodeVal['properties']['table_id']
           iwMappings.append(iwMapping)
           
    return iwMappings       



if __name__ == '__main__':
    p=makePipeLineMap('derive_overwrite')
    print json.dumps(p)