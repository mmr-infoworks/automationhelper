import sys
import hdfshelper
from simpleutils import *
from config_utils import *
import hivehelper
import mongo_helper

""" helper functions to create some files relevant to automation 
    input will be full path to  pipeline spec 
    configurations to hive/hadoop
"""
pipeLineMapExtra={"isAdvancedPipeline": True ,"description": "","domainId" : { "$type":  "oid", "$value": "b957b69324d0e78e4bc05cb7"}}



def create_pipelinemap(jsonMap):
    pipeLineMap = {}
    for key in pipeLineMapExtra:
        pipeLineMap[key] = pipeLineMapExtra[key]
    # support both type of exports ui export and script export
    if 'configuration' in jsonMap:
        for key in jsonMap['configuration']:
            pipeLineMap[key] = jsonMap['configuration'][key]
    else:
        pipeLineMap=jsonMap


    pipeLineMap['name'] = pipeLineMap['entity']['entity_name']
    return pipeLineMap

def create_pipelinespec(pipeLineMap):
    return json.dumps(pipeLineMap)    



def get_targettables_from_pipeline(pipeLineJson):
    targetTables = []
    nodes = pipeLineJson['pipeline']['model']['nodes']
    for key in nodes:
        if nodes[key]['type'] == 'TARGET':
            tableMap = {}
            try:
                tableMap['name']=nodes[key]['properties']['hive_table_name']
                tableMap['schema']=nodes[key]['properties']['hive_schema_name']
                tableMap['hdfs'] = nodes[key]['properties']['hdfs_path']
            except:
                print ("no hdfs table uses REFERENCE TABLE exiting!!")
                sys.exit(1)
            targetTables.append(tableMap)
    return targetTables


def do_all(pipelineName,targetPath):
    try:
        pipeLineMap = mongo_helper.makePipeLineMap(pipelineName)
        targetLocalDir = targetPath +"/" + pipelineName
        targets = get_targettables_from_pipeline(pipeLineMap)
        #copy hdfs files
        for target in targets:
            table = target['name']
            hdfs_path = target['hdfs']
            tableTarget = targetLocalDir + "/" + table
            hdfshelper.copyDir(tableTarget +"/data", hdfs_path)
            #copy validate.json
            validate_file = open(tableTarget+"/validate.json", 'w')
            validateMap = hivehelper.createvalidationmap(target)
            validate_file.write(json_pretty(validateMap))
            validate_file.close()


        #create pipeline.json
            pipeline_file = open(targetLocalDir+"/pipeline.json", 'w')
            pipeline_file.write(json_pretty(pipeLineMap))
            pipeline_file.close()
        #copy db.cfg
            cfg_file = open(targetLocalDir +"/db.cfg", 'w')
            cfg_file.write(DB_CFG)
            cfg_file.close()
       #copy domain.json
            domain_file = open(targetLocalDir + "/domain.json", 'w')
            domain_file.write(json_pretty(DOMAIN))
            domain_file.close()

    except IOError as e:
        print "no such file exiting" + str(e)
        sys.exit(1)    



#createValidationJson(sys.argv[1])  
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print "need pipelinejson and targetdirectory to write as inputs exiting "
        sys.exit(1)
    pipeLineFile = sys.argv[1]
    targetDir = sys.argv[2]    
    do_all(pipeLineFile, trim_path(targetDir))
