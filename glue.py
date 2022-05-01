from awsglue.utils import getResolvedOptions     # here we are just importing getResolvedOptions
import sys
from pyspark.sql.functions import when
from pyspark.sql import SparkSession             #here we are importing the SparkSession

args = getResolvedOptions(sys.argv,['s3_target_path_key','s3_target_path_bucket'])  #In this we are getting the arguments from lambda_function.py into the sys.argv and getting the bucketname and filename from that arguments mentioned in resolved in lambda_function.py
bucket = args['s3_target_path_bucket']
fileName = args['s3_target_path_key']

print(bucket, fileName)

spark = SparkSession.builder.appName("CDC").getOrCreate()   #here we are creating the SparkSession with the name "CDC"
inputFilePath = f"s3a://{bucket}/{fileName}"                # It'll take the bucket and filename from the specific S3 bucket
finalFilePath = f"s3a://cdc-final-output-pyspark/output"    # In this newly created S3 bucket, final data/output will be stored in output directory.

if "LOAD" in fileName:      #if the filename contains the word "LOAD" mean it is a full load and here we are just dumping whole code from that input bucket to the final path of S3 bucket in output directory 
    full_load_DF = spark.read.csv(inputFilePath)
    full_load_DF = full_load_DF.withColumnRenamed("_c0","ID").withColumnRenamed("_c1","FullName").withColumnRenamed("_c2","City")
    full_load_DF.write.mode("overwrite").csv(finalFilePath)
else:       # it mean if updation 
    updated_DF = spark.read.csv(inputFilePath)
    updated_DF = updated_DF.withColumnRenamed("_c0","action").withColumnRenamed("_c1","ID").withColumnRenamed("_c2","FullName").withColumnRenamed("_c3","City")
    final_file_DF = spark.read.csv(finalFilePath)
    final_file_DF = final_file_DF.withColumnRenamed("_c0","ID").withColumnRenamed("_c1","FullName").withColumnRenamed("_c2","City")
    
    for row in updated_DF.collect(): 
      if row["action"] == 'U':      #code for updation
        final_file_DF = final_file_DF.withColumn("FullName", when(final_file_DF["ID"] == row["ID"], row["FullName"]).otherwise(final_file_DF["FullName"]))      
        final_file_DF = final_file_DF.withColumn("City", when(final_file_DF["ID"] == row["ID"], row["City"]).otherwise(final_file_DF["City"]))
    
      if row["action"] == 'I':      #code for insertion
        insertedRow = [list(row)[1:]]
        columns = ['ID', 'FullName', 'City']
        newdf = spark.createDataFrame(insertedRow, columns)
        final_file_DF = final_file_DF.union(newdf)
    
      if row["action"] == 'D':      #code for deletion
        final_file_DF = final_file_DF.filter(final_file_DF.ID != row["ID"])
        
    final_file_DF.write.mode("overwrite").csv(finalFilePath)   
