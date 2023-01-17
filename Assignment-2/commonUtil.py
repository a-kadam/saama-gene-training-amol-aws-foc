#Extracting zip files

import zipfile, boto3, gzip, io, os, re
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from io import BytesIO
from pyspark.sql.functions import col
import yaml
from pyspark.sql.types import *

bucketName='saama-gene-training-data-bucket'
s3 = boto3.client('s3')
response = s3.get_object(Bucket=bucketName, Key="amolkadam/Assignment-two/Code/aws_config.yaml")
config = yaml.safe_load(response["Body"])

RootDir = config["aws.s3.RootDir"]
InboundDir = config["aws.s3.InboundDir"]
LandingDir = config["aws.s3.LandingDir"]
TempDir = config["aws.s3.TempDir"]
ArchiveDir = config["aws.s3.ArchiveDir"]

def getZipFileList(bucketName,inPrefix):
    s3 = boto3.resource('s3')
    bucketContentList = s3.Bucket(bucketName)
    fl_list = []
    for objectDetails in bucketContentList.objects.filter(Prefix=inPrefix):
        if objectDetails.key.endswith(".zip") or objectDetails.key.endswith(".gz") or objectDetails.key.endswith(".gzip"):
            fl_list.append(objectDetails.key)
    return fl_list

def getFileList(bucketName,inPrefix):
    s3 = boto3.resource('s3')
    bucketContentList = s3.Bucket(bucketName)
    fl_list = []
    for objectDetails in bucketContentList.objects.filter(Prefix=inPrefix):
        if objectDetails.key.endswith(".csv") or objectDetails.key.endswith(".xls") or objectDetails.key.endswith(".xlsx"):
            fl_list.append(objectDetails.key)
    return fl_list

def unzip_file(bucketName, zippedObj,TargetDir):
    s3Obj = boto3.resource('s3')
    session = boto3.session.Session()
    s3client = session.client('s3')
    zipObj = s3Obj.Object(bucket_name=bucketName, key=zippedObj)
    
    if zippedObj.endswith(".zip"):
        buffer = BytesIO(zipObj.get()["Body"].read())
        z = zipfile.ZipFile(buffer)
        for filename in z.namelist():
            #fileInfo = z.getinfo(filename)
            s3Obj.meta.client.upload_fileobj(z.open(filename),Bucket=bucketName,Key=TargetDir+"/"+filename)
    elif zippedObj.endswith(".gz") or zippedObj.endswith(".gzip"):
        with gzip.GzipFile(fileobj=zipObj.get()["Body"],mode="r") as gzipfile:
            filename=os.path.basename(zippedObj).split(".")[0]+"."+os.path.basename(zippedObj).split(".")[1]
            content=gzipfile.read()
            s3client.put_object(Body=content,Bucket=bucketName,Key=TargetDir+"/"+filename)
            
def copyS3File(srcBucketName,sourceKey,tgtBucketName,targetKey):
    s3 = boto3.resource('s3')
    copySource={
        'Bucket': srcBucketName,
        'Key': sourceKey
    }
    s3.meta.client.copy(copySource,tgtBucketName,targetKey)
    print("Copied File : {0}".format(targetKey))

def convertDatatypeOfSpecificType(df,srcDataType="int",tgtDataType="int"):
    for col_val in df.dtypes:
        #print(col_val[1],srcDataType,tgtDataType)
        if str(col_val[1])==srcDataType:
            df=df.withColumn(str(col_val[0]),col(col_val[0]).cast(tgtDataType))
    return df

def deleteS3Object(bucketName,files_to_delete):
    session = boto3.session.Session()
    s3client = session.client('s3')
    response = s3client.delete_objects(Bucket=bucketName, Delete={"Objects": files_to_delete})
    print("File Deleted : {0}".format(files_to_delete))
    
def createS3BucketDir(bucketName,folderName):
    session = boto3.session.Session()
    s3client = session.client('s3')
    s3client.put_object(Bucket=bucketName, Key=(folderName+'/'))
    print("Directory created : {0}".format(folderName))
    
def moveS3File(srcBucketName,sourceKey,tgtBucketName,targetKey):
    files_to_delete = []
    copyS3File(srcBucketName,sourceKey,tgtBucketName,targetKey)
    files_to_delete.append({"Key": file})
    deleteS3Object(srcBucketName,files_to_delete)
    
def readCsvFile(spark,bucketName,fileName,delimiter=","):
    file_name="s3://"+os.path.join(bucketName,fileName)
    return(spark.read.options(header="True",sep=delimiter).csv(file_name))

def writeCsvFile(bucketName,df,fileName,delimiter=","):
    file_name="s3://"+os.path.join(bucketName,fileName)
    df.toPandas().to_csv(file_name,header=True,index=False,sep=delimiter)
    print("Created csv file : {0}".format(file_name))

def convertDatatypeOfColumn(df,columnName,tgtDataType):
    return(df.withColumn(columnName,col(columnName).cast(tgtDataType)))

def archiveOldFiles():    
    s3 = boto3.resource('s3')
    yearMonthFolderList=[]

    for file in getFileList(bucketName,TempDir):
        folderName=os.path.join(LandingDir,os.path.basename(file).split("_")[-1].split(".")[0])
        yearMonthFolderList.append(folderName)

    yearMonthFolderList=list(set(yearMonthFolderList))
    for i in yearMonthFolderList:
        print(i)
        for OldFile in getFileList(bucketName,i):
            for CurrFile in getFileList(bucketName,TempDir):
                if os.path.basename(OldFile)==os.path.basename(CurrFile) and os.path.basename(file).split("_")[-1].split(".")[0] in CurrFile:
                    ZipFileDir=os.path.join(ArchiveDir,os.path.basename(OldFile).split("_")[-1].split(".")[0])
                    ZipFileName=os.path.join(ZipFileDir,os.path.basename(OldFile))+".zip"
                    createS3BucketDir(bucketName,ZipFileDir)
                    #copyS3File(bucketName,OldFile,bucketName,os.path.join(ZipFileDir,os.path.basename(OldFile)))
                    #print(ZipFileName)
                    zip_buffer = io.BytesIO()
                    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zipper:
                        infile_object = s3.Object(bucketName,OldFile) 
                        infile_content = infile_object.get()['Body'].read()
                        zipper.writestr(ZipFileName, infile_content)
                        s3.Object(bucketName,ZipFileName).put(Body=zip_buffer.getvalue())
                        #s3.put(Bucket=bucketName, Key=os.path.join(ZipDirPath,os.path.basename(OldFile)), Body=zip_buffer.getvalue())

def convertDtypeOfNumericColumnToDecimal(df,tgtDatatype):
    val=list(map(lambda x: x[0], df.dtypes))
    for i in range(0,len(df.first())):
        columnName=str(val[i])
        decimal_pattern="\d+\.\d+"
        numeric_pattern="^[0-9]+$"    
        if re.match(decimal_pattern,str(df.first()[i])) or re.match(numeric_pattern,str(df.first()[i])):
            df=convertDatatypeOfColumn(df,columnName,tgtDatatype)
    return df       

def loadFilestoLandingFolderwise(spark,bucketName,file):    
    folderName=os.path.join(LandingDir,os.path.basename(file).split("_")[-1].split(".")[0])
    createS3BucketDir(bucketName,folderName)
    df=readCsvFile(spark,bucketName,file)
    df=convertDtypeOfNumericColumnToDecimal(df,"decimal(15,6)")
    writeCsvFile(bucketName,df,os.path.join(folderName,os.path.basename(file)),delimiter=",")
    files_to_delete = []
    files_to_delete.append({"Key": file})
    deleteS3Object(bucketName,files_to_delete)