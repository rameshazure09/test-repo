# Databricks notebook source
# DBTITLE 1,connect to storage account
storage_account_name = "adls-storage-acct"

# should be at the storage account level and not the container
storage_account_access_key = "key="
connection_string = "DefaultEndpointsProtocol=https;AccountName=adls-storage-acct;AccountKey=key++;EndpointSuffix=core.windows.net"

# COMMAND ----------

spark.conf.set("fs.azure.account.key."+storage_account_name+".blob.core.windows.net", storage_account_access_key)

# COMMAND ----------

# file_type = "csv"
# file_name = "export (3).csv"
# blob_container = "rawstream"
# folder_path = "test/"
# file_location = "wasbs://" + blob_container + "@" + storage_account_name + ".blob.core.windows.net/" +folder_path+ file_name

# COMMAND ----------

#df = spark.read.format(file_type).option("inferSchema", "true").load(file_location)

#df = spark.read.option("inferSchema", "true").option("header", "true").csv(file_location).withColumn("filename", input_file_name())

# COMMAND ----------

# import pandas as pd
# df = pd.DataFrame(
#     columns=['filename', 'filepath', 'filearrivaltime'])
# for fileinfo in file_list:
#   filename = [t[1] for t in file_list]
#   filepath = [t[0] for t in file_list]
#   filearrivaltime = [t[3] for t in file_list]
#   #temporary_df = pd.DataFrame(filename, columns=['filename'])
#   temporary_df = pd.DataFrame({'filename':filename,'filepath':filepath,'filearrivaltime':filearrivaltime})
#   df = df.append(temporary_df, ignore_index=True)
# print(df)

# COMMAND ----------

# MAGIC %md ##test sample data

# COMMAND ----------

df_test_sample_data = sql("select b.VehicleId as VehicleId, cast(a.da as date) as date,a.sid,round(AVG(a.ld)) as load, b.WheelPosition from kodiak.rawtesttelemetry a join kodiak.testvehiclemap b on a.sid = b.SensorID  group by a.da,a.sid,b.WheelPosition,b.VehicleId order by VehicleId,a.da,b.WheelPosition")
#df.createOrReplaceTempView("LoadTable")

# COMMAND ----------

# DBTITLE 1,write sample data as a csv file into storage
df_test_sample_data.write.format("csv").save("wasbs://rawstream@adls-storage-acct.blob.core.windows.net/test/testloaddata_csv.csv")

# COMMAND ----------

# DBTITLE 1,creating table with the csv file
# MAGIC %sql
# MAGIC create table test.testloaddata_csv using csv location "wasbs://rawstream@adls-storage-acct.blob.core.windows.net/test/testloaddata_csv.csv"

# COMMAND ----------

# MAGIC %md ## writing sampledata as parquet file in storage account

# COMMAND ----------

df_test_sample_data.write.format("parquet").save("wasbs://rawstream@adls-storage-acct.blob.core.windows.net/test/testloaddata.parquet")

# COMMAND ----------

# MAGIC %md ## creating table with the parquet file

# COMMAND ----------

# MAGIC %sql
# MAGIC create table test.testloaddata using parquet location "wasbs://rawstream@adls-storage-acct.blob.core.windows.net/test/testloaddata.parquet"

# COMMAND ----------

from  pyspark.sql.functions import input_file_name

#df.withColumn("filename", input_file_name())

# COMMAND ----------

# MAGIC %md ## reading CT files from storage acccount to list

# COMMAND ----------

file_list = dbutils.fs.ls("wasbs://rawstream@adls-storage-acct.blob.core.windows.net/test")
file_list

# COMMAND ----------

# DBTITLE 1,Reading metadata of each file in blob into a dataframe
import pandas as pd
tempdf = pd.DataFrame(columns=['filename', 'filepath', 'filearrivaltime'])
filename = [t[1] for t in file_list]
filepath = [t[0] for t in file_list]
filesize = [t[2] for t in file_list]
filearrivaltime = [t[3] for t in file_list]
first_df = pd.DataFrame({'filename':filename,'filepath':filepath,'filearrivaltime':filearrivaltime})
tempdf = tempdf.append(first_df, ignore_index=True)
tempdf_py = spark.createDataFrame(tempdf)
from pyspark.sql.functions import lit
from datetime import datetime
df_final = pd.DataFrame(columns=['filename', 'filepath', 'filearrivaltime'])
for row in tempdf_py.rdd.collect():
    #print(row.filename)
    if (row.filename.find('csv') != -1):
      print("Contains substring 'csv'")
      filearrivaltime = datetime.fromtimestamp(row.filearrivaltime / 1000)
      temporary_df= pd.DataFrame({'filename':[row.filename],'filepath':[row.filepath],'filearrivaltime':filearrivaltime})
      df_final = df_final.append(temporary_df, ignore_index=True)
      df_final_py = spark.createDataFrame(df_final).withColumn("isprocessed",lit('False'))

# COMMAND ----------

display(df_final_py)

# COMMAND ----------

# DBTITLE 1,reading the processed file details into a dataframe
df_processed_files = spark.read.format("parquet").load("wasbs://rawstream@adls-storage-acct.blob.core.windows.net/test/filesmetadata.parquet")

# COMMAND ----------

df_processed_files.count()

# COMMAND ----------

display(df_processed_files)

# COMMAND ----------

# DBTITLE 1,filter the processed files from existing files
df_tobe_processed = df_final_py.join(df_processed_files, ['filename'], 'left_anti')
display(df_tobe_processed)

# COMMAND ----------

# DBTITLE 1,create a empty dataframe for data processing
# Importing PySpark and the SparkSession
# DataType functionality
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
 
# Creating a spark session
spark_session = SparkSession.builder.appName(
    'Spark_Session').getOrCreate()
emp_RDD = spark_session.sparkContext.emptyRDD()
 
# Defining the schema of the DataFrame
columns1 = StructType([StructField('VehicleId', StringType(), False),
                       StructField('date', DateType(), False),
                      StructField('sid', StringType(), False),
                      StructField('load', DoubleType(), False),
                      StructField('WheelPosition', StringType(), False),
                      StructField('filename', StringType(), False),
                      StructField('filereceivedtime', DateType(), False)])
 
# Creating an empty DataFrame
first_df = spark_session.createDataFrame(data=emp_RDD,
                                         schema=columns1)

# COMMAND ----------

# DBTITLE 1,read each csv file into a dataframe which is not processed
from pyspark.sql.functions import lit
for row in df_tobe_processed.rdd.collect():
  temporary_df = spark.read.format("csv").load(row.filepath,inferschema = True, header = True).withColumn("filename",lit(row.filename)).withColumn("filereceivedtime",lit(row.filearrivaltime))
  first_df = first_df.union(temporary_df)

# COMMAND ----------

display(first_df)

# COMMAND ----------

# DBTITLE 1,ranking each latest record based on filearrivaltime
from pyspark.sql.functions import *
from pyspark.sql.window import Window

w = Window.partitionBy('VehicleId','date','sid','WheelPosition').orderBy(desc('filereceivedtime'))
df = first_df.withColumn('Rank',dense_rank().over(w))

finaldf = df.filter(df.Rank == 1).drop(df.Rank)

# COMMAND ----------

display(finaldf)

# COMMAND ----------

finaldf =finaldf.drop("filename").drop("filereceivedtime")

# COMMAND ----------

# DBTITLE 1,reading existing data file into dataframe
df_test_data = spark.read.format("parquet").load("wasbs://rawstream@adls-storage-acct.blob.core.windows.net/test/testloaddata.parquet")

# COMMAND ----------

df_test_data.count()

# COMMAND ----------

# DBTITLE 1,upsert and merge latest records
import pyspark.sql.functions as f

test_final_dataframe = df_test_data.alias('a').join(
    finaldf.alias('b'), ['VehicleId', 'date', 'sid','WheelPosition'], how='outer'
).select('VehicleId', 'date', 'sid', 
    f.coalesce('b.load', 'a.load').alias('load'), 
    'WheelPosition' 
)

# COMMAND ----------

display(test_final_dataframe)

# COMMAND ----------

# DBTITLE 1,write final dataframe as parquet file in to storage account
test_final_dataframe.write.format("parquet").mode("overwrite).save("wasbs://rawstream@adls-storage-acct.blob.core.windows.net/test/testloaddata.parquet")

# COMMAND ----------

# MAGIC %sql
# MAGIC Refresh table test.testloaddata

# COMMAND ----------

# DBTITLE 1,updating isprocessed column value to True after files processing
df_processed = df_tobe_processed.withColumn("isprocessed",lit("True"))
display(df_processed)

# COMMAND ----------

# DBTITLE 1,merge processed files dataframe and previously processed files dataframe
#df_merge = df_processed.union(df_processed_files)

# COMMAND ----------

# DBTITLE 1,append the processed files dataframe as a parquet file in to blob
df_processed.write.format("parquet").mode("append").save("wasbs://rawstream@adls-storage-acct.blob.core.windows.net/test/filesmetadata.parquet")

# COMMAND ----------

# MAGIC %md # rough work

# COMMAND ----------

from pyspark.sql.functions import col, hash

df1 = df1.withColumn('hash_value', hash('id', 'name', 'city') 
df2 = df2.withColumn('hash_value', hash('id', 'name', 'city') 

df_updates = df1 .alias('a').join(df2.alias('b'), (\
            (col('a.id') == col('b.id')) &\
            (col('a.hash_value') != col('b.hash_value')) \
            ) , how ='inner'
        )

df_updates = df_updates.select(b.*) 

# COMMAND ----------

from datetime import datetime
filearrivaltime = [t[3] for t in file_list]
filetime = []
for time in filearrivaltime:
  unixtime = datetime.fromtimestamp(time / 1000)
  filetime.append(unixtime)
filename = [t[1] for t in received_files_list]
for file in filename:
  #if filename.find(filetype) != -1:
  if (file.find('csv') != -1):
    print("Contains substring 'csv'")
    for t in filetime:
      print(t)
    
    #print("Found!")
#   if 'testloaddata' in filename:
#     print(filename)
#   #filespath1.append(filepath1)
  else:
    print("file is not a csv")

# COMMAND ----------

from datetime import datetime
filearrivaltime = [t[3] for t in file_list]
temptime = []
for time in filearrivaltime:
  print(time)
  filetime = datetime.fromtimestamp(time / 1000)
  temptime.append(filetime)
  print(filetime)

# COMMAND ----------

print(temptime)

# COMMAND ----------

from datetime import datetime
import pandas as pd
df2 = pd.DataFrame(columns=['filename', 'filepath', 'filetime'])
filename = [t[1] for t in file_list]
filepath = [t[0] for t in file_list]
filesize = [t[2] for t in file_list]
filearrivaltime = [t[3] for t in file_list]
for time in filearrivaltime:
  print(time)
  filetime = datetime.fromtimestamp(time / 1000)
  print(filetime)
  temporary_df2 = pd.DataFrame({'filename':filename,'filepath':filepath,'filetime':filetime})
  df2 = df2.append(temporary_df2, ignore_index=True)
  df2_py = spark.createDataFrame(df2)
  display(df2_py)

# COMMAND ----------

import pandas as pd
df = pd.DataFrame(columns=['filename', 'filepath', 'filearrivaltime'])
filename = [t[1] for t in file_list]
filepath = [t[0] for t in file_list]
filesize = [t[2] for t in file_list]
filearrivaltime = [t[3] for t in file_list]
temporary_df = pd.DataFrame({'filename':filename,'filepath':filepath,'filearrivaltime':filearrivaltime})
df = df.append(temporary_df, ignore_index=True)
df_py = spark.createDataFrame(df)

# COMMAND ----------

df_py.write.format("parquet").save("wasbs://rawstream@adls-storage-acct.blob.core.windows.net/test/filesmetadata.parquet")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table test.filesmetadata using parquet location "wasbs://rawstream@adls-storage-acct.blob.core.windows.net/test/filesmetadata.parquet"

# COMMAND ----------

df1  = sql("select * from test.testloaddata")

# COMMAND ----------

received_files_list = dbutils.fs.ls("wasbs://rawstream@adls-storage-acct.blob.core.windows.net/test")
received_files_list

# COMMAND ----------

# import pandas as pd
# df = pd.DataFrame(
#     columns=['filename', 'filepath', 'filearrivaltime'])
# df = df.append(temporary_df, ignore_index=True)
filetype = '.csv'
filename = [t[1] for t in received_files_list]
print(filename)
for file in filename:
  #if filename.find(filetype) != -1:
  if (file.find('csv') != -1):
    print("Contains substring 'csv'")
    
    print("Found!")
#   if 'testloaddata' in filename:
#     print(filename)
#   #filespath1.append(filepath1)
  else:
    print("file is not a csv")


 # temporary_df = pd.DataFrame({'filename':filename,'filepath':filepath,'filearrivaltime':filearrivaltime})
  #df = df.append(temporary_df, ignore_index=True)
# df_py = spark.createDataFrame(df)
# display(df_py)

# COMMAND ----------

from datetime import datetime
for row in df_py.rdd.collect():
  d=datetime.fromtimestamp(row.filearrivaltime / 1000)
  print(d)

# COMMAND ----------

from pyspark.sql.functions import lit
from datetime import datetime
for row in df_py.rdd.collect():
    #print(row.filename)
    if (row.filename.find('csv') != -1):
      print("Contains substring 'csv'")
      filearrivaltime = datetime.fromtimestamp(row.filearrivaltime / 1000)
      temporary_df_5= spark.read.format("csv").load(row.filepath,inferschema = True, header = True).withColumn("filereceivedtime",lit(filearrivaltime))
      first_df = first_df.union(temporary_df_5)

# COMMAND ----------

display(first_df)

# COMMAND ----------

filename = [t[1] for t in received_files_list]
filepath = [t[0] for t in received_files_list]
for file in filename:
  for file in filepath:
    if (file.find('csv') != -1):
      print("Contains substring 'csv'")
      print(file)
      temporary_df= spark.read.format("csv").load(file, inferschema = True,header = True)
      df = df.union(temporary_df)
  else:
    print("file is not a csv")

# COMMAND ----------

# Importing PySpark and the SparkSession
# DataType functionality
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
 
# Creating a spark session
spark_session = SparkSession.builder.appName(
    'Spark_Session').getOrCreate()
emp_RDD = spark_session.sparkContext.emptyRDD()
 
# Defining the schema of the DataFrame
columns1 = StructType([StructField('VehicleId', StringType(), False),
                       StructField('date', DateType(), False),
                      StructField('sid', StringType(), False),
                      StructField('load', DoubleType(), False),
                      StructField('WheelPosition', StringType(), False),
                      StructField('filereceivedtime', DateType(), False)])
 
# Creating an empty DataFrame
first_df = spark_session.createDataFrame(data=emp_RDD,
                                         schema=columns1)

# COMMAND ----------

filepath = [t[0] for t in received_files_list]
for file_path in filepath:
  #print(file_path)
  if (file_path.find('csv') != -1):
    print("Contains substring 'csv'")
    print(file_path)
    
    temporary_df_3= spark.read.format("csv").load(file,inferschema = True, header = True)
    first_df = first_df.union(temporary_df_3)
  else:
    print("file is not a csv")

# COMMAND ----------

display(first_df)

# COMMAND ----------

display(df)

# COMMAND ----------

filename = [t[1] for t in received_files_list]
print(filename)
filepath = [t[0] for t in file_list]
print(filepath)

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
spark_session = SparkSession.builder.appName('Spark_Session').getOrCreate()
rows = [['', '','','','']]
columns = ['VehicleId', 'date','sid','load','WheelPosition']
df = spark_session.createDataFrame(rows, columns)
# filename = [t[1] for t in received_files_list]
# filepath = [t[0] for t in file_list]
for file_name in filename:
  for file_path in filepath:
    if (file_name.find('csv') != -1):
      print("Contains substring 'csv'")
      print(file_path)
      temporary_df= spark.read.format("csv").load(file_path, inferschema = True,header = True)
      df = df.union(temporary_df)
  else:
    print("file is not a csv")

# COMMAND ----------

# Creating the DataFrame
#second_df = spark_session.createDataFrame(rows, columns)
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
spark_session = SparkSession.builder.appName('Spark_Session').getOrCreate()
#rows =['','','','','','']
rows = [['', '','','','']]
columns = ['VehicleId', 'date','sid','load','WheelPosition']
second_df = spark_session.createDataFrame(rows, columns)

# COMMAND ----------

VehicleId
date
sid
load
WheelPosition

# COMMAND ----------

import pandas as pd
df = pd.DataFrame(columns=['VehicleId', 'date', 'sid','load','WheelPosition'])
filename = [t[1] for t in file_list]
filepath = [t[0] for t in file_list]
filesize = [t[2] for t in file_list]
filearrivaltime = [t[3] for t in file_list]
temporary_df = pd.DataFrame({'filename':filename,'filepath':filepath,'filearrivaltime':filearrivaltime})
df = df.append(temporary_df, ignore_index=True)
df_py = spark.createDataFrame(df)
display(df_py)

# COMMAND ----------

filespath

# COMMAND ----------

import pandas as pd
df = pd.DataFrame(
    columns=['filename', 'filepath', 'filearrivaltime'])
filename = [t[1] for t in file_list]
filepath = [t[0] for t in file_list]
filesize = [t[2] for t in file_list]
filearrivaltime = [t[3] for t in file_list]
temporary_df = pd.DataFrame({'filename':filename,'filepath':filepath,'filearrivaltime':filearrivaltime})
df = df.append(temporary_df, ignore_index=True)
df_py = spark.createDataFrame(df)
display(df_py)
