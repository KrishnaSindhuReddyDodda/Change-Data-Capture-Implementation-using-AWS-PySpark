# Change-Data-Capture-Implementation-using-AWS-PySpark

**Overview implementation of the project**
In CDC(Change Data Capture), we have a database inside RDS and in this we just want to replicate all the changes happening inside the RDS database into AWS S3 storage. The main goal is to create the data lake out of the database and for every change that is being happened whether that's the insertion/update/deletion we just have to take all those changes from the database develop a pipeline that will consider all those changes and put that inside the final S3 storage.
**From the above attachements, there's a file with project architecture.**
1) Firstly, input data will be in RDS, inside the RDS -> MySQL database and the final output storage will be S3.
2) Now if there's any change happening in the MySQL DB, each change should be reflected into the final output storage.
3) In the second step, we'll use DMS(Data Migration Service) in AWS which contains two endpoints (source endpoint and destination endpoint), here source endpoint will take the data from the target i.e, RDS, and load/write this to the destination; now the destination endpoint will load the data into the temporary S3 bucket.
4) Once if the data/file loads into the temporary bucket, it will invokes/triggers the lambda function by providing the filename and bucket name.
5) Here cloudWatch Event service is associated with our lambda functions with IAM role. In the log groups, we can see that seperate log streams were created for each execution, in which we can see execution outputs that's happening in lambda.
6) Now lambda function will invokes the PySpark job in glue by providing the file name and bucket name. Glue is the name of the servive inside which we can run the PySpark jobs
7) PySpark job will read the data from temporary s3 bucket and load/write it into the final S3 bucket.
