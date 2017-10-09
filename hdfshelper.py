from pywebhdfs.webhdfs import PyWebHdfsClient
import sys,os
from simpleutils import trim_path
from config_utils import *

# TODO pick from config
hdfs = PyWebHdfsClient(host=HDFS_HOST, port=HDFS_PORT,user_name=HDFS_USER) 

def copyDir(localFilePath,hdfsDirectory):
    dirs = hdfs.list_dir(hdfsDirectory)
    for fileStatus in dirs['FileStatuses']['FileStatus']:
        if "full" in fileStatus['pathSuffix']:
            copyInternalDir(localFilePath ,hdfsDirectory + "/"+ fileStatus['pathSuffix'])

def copyInternalDir(localFilePath,hdfsDirectory):
    try:
        os.makedirs(localFilePath)
    except:
        pass
    dirs = hdfs.list_dir(hdfsDirectory)
    for fileStatus in dirs['FileStatuses']['FileStatus']:
        if fileStatus['type'] == 'DIRECTORY':
            childDirectoryLocal=localFilePath +"/"+ fileStatus['pathSuffix']
            childDirectoryRemote=hdfsDirectory +"/"+ fileStatus['pathSuffix']
            copyInternalDir(childDirectoryLocal,childDirectoryRemote)
        elif fileStatus['type'] == 'FILE':
             fileNameLocal = localFilePath + "/" + fileStatus['pathSuffix']
             fileNameRemote = hdfsDirectory + "/" + fileStatus['pathSuffix']
             writeFile = open(fileNameLocal,'w')
             data= hdfs.read_file(fileNameRemote)
             writeFile.write(data)
             writeFile.close()      



if __name__ == "__main__":
    if len(sys.argv)< 2:
        print "need two arguments  localdirectory and remote directory exiting"
    sys.exit
    localDir = trim_path(sys.argv[1])
    remoteDir = trim_path(sys.argv[2])
    print localDir +","  + remoteDir
    copyDir(localDir,remoteDir)



