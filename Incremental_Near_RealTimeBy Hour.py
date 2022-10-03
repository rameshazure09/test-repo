# Databricks notebook source
# MAGIC %md ###create lookup table for the first time

# COMMAND ----------

# %sql
# create table einride_vehiclelog_lookup 
# (date date,
# hour_path int,
# isprocessed varchar(2))

# COMMAND ----------

# MAGIC %md ###get todays date

# COMMAND ----------

# Import date class from datetime module
from datetime import date
# Returns the current local date
today_date = date.today()
today_date = str(today_date)
today_date = "'"+today_date+"'"
print(today_date)

# COMMAND ----------

# MAGIC %md ###get max date from lookup table

# COMMAND ----------

df_max_date = sql("select max(date) as dt from einride_vehiclelog_lookup")
max_date1 = df_max_date.select('dt').collect()
max_date = max_date1[0].dt
print(max_date)

# COMMAND ----------

# MAGIC %md ### insert date and hour path into lookup table if max date and todays are not same

# COMMAND ----------

today_date1 = date.today()
if max_date == today_date1:
  print("both dates are same")
else:
  hour_path = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23]
  for i in hour_path:
    sql("insert into einride_vehiclelog_lookup values ({0},{1},'N')".format(today_date,i)) 
  print("dates are different")

# COMMAND ----------

# MAGIC %md ###converting todays date as filepath including padding leading edge zero for month

# COMMAND ----------

today_date = str(date.today())
day_path1 = str.replace(today_date,'-0','-')
day_path = str.replace(day_path1,'-','/')
print(day_path)

# COMMAND ----------

# MAGIC %md ### select hours path from look up table which are to be processed

# COMMAND ----------

from datetime import datetime
now = datetime.now()
current_hour = now.strftime("%H")
#current_hour = 3
df_hour_path = sql('select hour_path from einride_vehiclelog_lookup where hour_path <{0} and isprocessed = "N" order by hour_path'.format(current_hour))

# COMMAND ----------

# MAGIC %md ###appending filepath and hourpath into a list

# COMMAND ----------

hour_path = [data[0] for data in df_hour_path.select('hour_path').collect()]
filepaths = []
for i in hour_path:
  filepath = day_path+'/'+str(i)
  filepaths.append(filepath)
print(filepaths)  

# COMMAND ----------

# MAGIC %md ###append the above filepaths data to einride.vehiclelog table

# COMMAND ----------

location_delta_path = '/mnt/deltalake/EinRide/Rawstream/VehicleLog'
rootPath = '/mnt/rawstream/EinRide/VehicleLog/'
for i in filepaths:
  locations = i+'/*/'
  locationDF = spark.read.format('parquet').load(rootPath+locations)
  locationDF.write.format("delta").mode("append").save(location_delta_path)
  #print(locations)
sql("Refresh table einride.vehiclelog")

# COMMAND ----------

# MAGIC %md ### updating isprocessed column with "y" for the processed file paths

# COMMAND ----------

for i in hour_path:
  sql('update einride_vehiclelog_lookup set isprocessed = "Y" where hour_path = {0}'.format(i))

# COMMAND ----------

# MAGIC %md ###end of processing

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from einride_vehiclelog_lookup order by hour_path

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from einride.vehiclelog

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from einride.vehiclelog
