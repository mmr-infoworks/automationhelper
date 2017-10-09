prerequisities

    one time
        pip install pywebhdfs
        brew install mysql **
        pip install MysqlDB-python ( mayneed mysql install for some libraries hence above mysql is required)

    every time
        change config_utils.py to reflect values on server you are using for building pipeline first time
        script will pull data from HDFS_PATH
        HIVE_METASTORE** values will be used to extract schema for the table
        (HIVE_METASTORE connections may need to be  port forwarded as remote mysql logging is not currently permitted in dev servers)
            ssh -L 33000:localhost:3306 ec2-user@dev2 forwards dev2 3306 port to localhost 33000


   usage
     python pipelinehelper.py < full path to pipeline.json> < full path to dataset directory>
     eg:python pipelinehelper.py /Users/manoharm/infoworks/utils/backup/AutomationSpecs_MM_pipelines/UseMultipleTargetsInPipeline.json /Users/manoharm/github/rosie/content/datasets/df_manohar/
