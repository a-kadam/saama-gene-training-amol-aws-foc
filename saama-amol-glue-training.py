from commonUtil import *
#from awsglue.context import GlueContext
import boto3
import pandas as pd
import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
import sys
import os
import io
from pyspark.sql import window
from pyspark.sql.window import *
from datetime import datetime
from botocore.exceptions import ClientError

spark=SparkSession.builder.appName("Assignment_1").getOrCreate()

for file in getFileList(bucketName,InboundPrefixString):
    if getFileExtension(file)=="xls" or getFileExtension(file)==".xlsx" or getFileExtension(file)=="csv":
        copyFile(bucketName,file,bucketName,PreprocessPrefixString+"/"+file.split("/")[-2]+"/"+os.path.basename(file))
        
for file in getFileList(bucketName,PreprocessPrefixString):
    loadToLanding(spark,file)
    
for file in getFileList(bucketName,LandingPrefixString):
    loadToStandard(spark,file)
        
print(start_a_crawler("saama-gene-training-amol-crawler"))
print(start_a_crawler("saama-gene-training-amol-parquet-crawler"))

print("Data Summary load started")
for file in getFileList(bucketName,LandingPrefixString):
    loadSummary(spark,bucketName,file)

print("Data Summary load Finished")