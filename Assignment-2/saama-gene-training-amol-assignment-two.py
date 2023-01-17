#Extracting zip files

import zipfile, boto3, gzip, io, os
import pandas as pd
from pyspark.sql import SparkSession
from commonUtil import *
from pyspark.context import SparkContext
from io import BytesIO
from pyspark.sql.functions import col
import yaml

spark=SparkSession.builder.appName("Assignment_2").getOrCreate()

for file in getZipFileList(bucketName,InboundDir):
    print("Inbound file ={0}".format(file))
    unzip_file(bucketName,file,TempDir)

for file in getZipFileList(bucketName,TempDir):
    unzip_file(bucketName,file,TempDir)
    
archiveOldFiles()
for file in getFileList(bucketName,TempDir):
    loadFilestoLandingFolderwise(spark,bucketName,file)