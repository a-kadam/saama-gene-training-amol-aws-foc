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
from pyspark.sql.functions import *
from botocore.exceptions import ClientError
from datetime import datetime

bucketName="saama-gene-training-data-bucket"
InboundPrefixString="amolkadam/Inbound"
PreprocessPrefixString="amolkadam/Preprocess"
LandingPrefixString="amolkadam/Landing"
StandardizedPrefixString="amolkadam/Standardized"
OutboundPrefixString="amolkadam/Outbound"


def start_a_crawler(crawler_name):
    session = boto3.session.Session()
    glue_client = session.client('glue')
    try:
        response = glue_client.start_crawler(Name=crawler_name)
        return response
    except ClientError as e:
        raise Exception("boto3 client error in start_a_crawler: " + e.__str__())
    except Exception as e:
        raise Exception("Unexpected error in start_a_crawler: " + e.__str__())

def deleteS3Object(bucketName,files_to_delete):
    s3_client = boto3.client("s3")
    response = s3_client.delete_objects(Bucket=bucketName, Delete={"Objects": files_to_delete})
    print("{0} file has been deleted".format(files_to_delete))

def loadToLanding(spark,fileName):
    if (getFileExtension(fileName)=="xls" or getFileExtension(fileName)=="xlsx"):
        df=readExcelFile(spark,bucketName,fileName)
    elif getFileExtension(fileName)=="csv":
        df=readCsvFile(spark,bucketName,fileName)
    if fileValidateCountHeader(df,fileName):
        print("{0} file is valid".format(fileName))
        tgtfileName=LandingPrefixString+"/"+fileName.split("/")[-2]+"/"+os.path.basename(fileName).split(".")[0]+".csv"
        fileConvertToCsv(bucketName,df,tgtfileName,tgtDelimeter="|")
    else:
        print("{0} file is not valid".format(fileName))
        
def loadToStandard(spark,fileName):
    df=readCsvFile(spark,bucketName,fileName,"|")
    df=df.withColumnRenamed("Adj Close","Adj_Close").withColumnRenamed("Script name","Script_name").withColumnRenamed("Scrip name","Script_name")
    tgtfileName=StandardizedPrefixString+"/"+"ParquetFiles/"+os.path.basename(fileName).split(".")[0]
    writeParquetFile(bucketName,df,tgtfileName,partitionColNameList="Script_name")
 
def readExcelFile(spark,bucketName,fileName):
    file_name="s3://"+bucketName+"/"+fileName
    return(spark.createDataFrame(pd.read_excel(file_name)))
    
def readCsvFile(spark,bucketName,fileName,delimiter=","):
    file_name="s3://"+bucketName+"/"+fileName
    return(spark.read.options(header="True",inferSchema="True",sep=delimiter).csv(file_name))
    
def readTextFile(bucketName,path):
    s3_client = boto3.client("s3")
    response = s3_client.get_object(Bucket=bucketName, Key=path)
    return response['Body'].read().decode("utf-8")

def writeCsvFile(bucketName,df,fileName,delimiter=","):
    file_name="s3://"+bucketName+"/"+fileName
    df.toPandas().to_csv(file_name,header=True,index=False,sep=delimiter)
    print("Created csv file : {0}".format(file_name))
    
def writeParquetFile(bucketName,df,fileName,partitionColNameList=""):
    file_name="s3://"+bucketName+"/"+fileName
    df.write.partitionBy(partitionColNameList).mode("overwrite").parquet(file_name)
    print("Created parquet file : {0}".format(file_name))

def fileConvertToCsv(bucketName,df,fileName,tgtDelimeter=","):
    writeCsvFile(bucketName,df,fileName,tgtDelimeter)
    print("\n{0} sucessfully converted to '.CSV'".format(fileName))

def copyFile(srcBucketName,sourceKey,tgtBucketName,targetKey):
    s3 = boto3.resource('s3')
    copySource={
        'Bucket': srcBucketName,
        'Key': sourceKey
    }
    s3.meta.client.copy(copySource,tgtBucketName,targetKey)
    print("{0} File has been Copied".format(targetKey))

def getFileList(bucketName,inPrefix):
    s3 = boto3.resource('s3')
    bucketContentList = s3.Bucket(bucketName)
    fl_list = []
    for objectDetails in bucketContentList.objects.filter(Prefix=inPrefix):
        if objectDetails.key.endswith(".csv") or objectDetails.key.endswith(".xls") or objectDetails.key.endswith(".xlsx"):
            fl_list.append(objectDetails.key)
    return fl_list
    
def fileValidationInstCheck(par1,par2,valType):
    if par1==par2:
        return True
    else:
        print("{0} is invalid".format(valType))
        return False
        
def getFileExtension(fileName):
    return(os.path.basename(fileName).split(".")[1])
    
def fileValidateCountHeader(df,file):
    folderName=os.path.split(file)[0]
    counterFilePath=f"{folderName}/COUNTER.txt"
    indicatorFilePath=f"{folderName}/INDICATOR.txt"
    headerFilePath=f"{folderName}/HEADER.txt"
    
    cnt=int(readTextFile(bucketName,counterFilePath))
    indicator=str(readTextFile(bucketName,indicatorFilePath))
    header=str(readTextFile(bucketName,headerFilePath))
    #print(str(cnt)+" "+indicator+" "+header)
    
    if fileValidationInstCheck(df.count(),cnt,"count") and fileValidationInstCheck("|".join(df.columns),header,"header") and fileValidationInstCheck(indicator,"indicator_file","indicator"):
        return True
    else:
        return False

def validateDate(dateText,dateFormat):
    try:
        if dateText != datetime.strptime(dateText, dateFormat).strftime(dateFormat):
            raise ValueError
        return True
    except ValueError:
        return False

def getDateFormatforSpecificFile(df):    
    dateDelimiter='-'
    
    spec2 = Window.partitionBy(df.Script_name)
    
    df=df.withColumn("new_date",concat_ws(dateDelimiter,(concat_ws(dateDelimiter,max(substring_index(col('Date'), dateDelimiter, 1).cast("int")).over(spec2),max(substring_index(substring_index(col('Date'), dateDelimiter, 2), dateDelimiter, -1).cast("int")).over(spec2))),max(substring_index(col('Date'), dateDelimiter, -1).cast("int")).over(spec2)))
    
    sampleDataString=df.first()['new_date']    
    df=df.drop("new_date")
    
    if validateDate(sampleDataString,"%Y-%m-%d"):
        return 'yyyy-m-d'
    elif validateDate(sampleDataString,"%Y-%d-%m"):
        return 'yyyy-d-m'
    elif validateDate(sampleDataString,"%d-%m-%Y"):
        return 'd-m-yyyy'
    elif validateDate(sampleDataString,"%m-%d-%Y"):
        return 'm-d-yyyy'
        
def loadSummary(spark,bucketName,fileName):
    df=readCsvFile(spark,bucketName,fileName,delimiter="|")
    
    df = df.withColumnRenamed("Script name","Script_name")\
    .withColumnRenamed("Scrip name","Script_name")\
    .withColumnRenamed("Adj Close","Adj_Close")\
    .withColumn("Date",regexp_replace("Date","/","-"))
    
    dateFormat=getDateFormatforSpecificFile(df)
    
    df=df.withColumn("Date",to_date(col("date"),dateFormat))\
    .withColumn("Week_num",concat(lit("Week"),weekofyear(to_date(col("date"),dateFormat))))\
    .withColumn("PartCol",concat(year(to_date(col("date"),dateFormat)),concat(lit("Week"),weekofyear(to_date(col("date"),dateFormat)))))
    
    spec1 = Window.partitionBy(df.PartCol)

    df=df.withColumn('Volume',sum('Volume').over(spec1))\
    .withColumn('Low',min('low').over(spec1))\
    .withColumn('High',max('High').over(spec1))\
    .withColumn('St_Date',min('Date').over(spec1))\
    .withColumn('End_Date',max('Date').over(spec1))\
    .withColumn('Open',first('Open').over(spec1))\
    .withColumn('Close',last('Close').over(spec1))\
    .withColumn('Adj Close',avg('Adj_Close').over(spec1))
    
    df=df.select(['Script_name','Week_num','St_Date','End_Date','Open','High','Low','Close','Adj Close','Volume']).distinct().orderBy(col('St_Date'))
    
    fileName=OutboundPrefixString+"/"+fileName.split("/")[-2]+"/"+os.path.basename(fileName)
    writeCsvFile(bucketName,df,fileName)
    
def loadSummaryUsingSparkSql(spark,bucketName,fileName):
    if 'SBIN' in fileName:
        dateFormat='d/m/yyyy'
    elif 'INFY' in fileName:
        dateFormat='mm-dd-yyyy'
    
    df=readCsvFile(spark,bucketName,fileName,delimiter="|")
    df = df.withColumnRenamed("Script name","Script_name")\
    .withColumnRenamed("Scrip name","Script_name")\
    .withColumnRenamed("Adj Close","Adj_Close")\
    .withColumn("Date",regexp_replace("Date","/","-"))
    dateFormat=getDateFormatforSpecificFile(df)
    df=df.withColumn("Date",to_date(col("date"),dateFormat))
    df.createOrReplaceTempView('SRCTEMP')
    spark.sql('select Date,PartCol,Volume,sum(Volume) over(partition by PartCol) as Volume from SRCTEMP where PartCol="2002week1"').show(20)