// Databricks notebook source
// %sql
// create table test_lookup_new 
// (date timestamp,
// hour_path int,
// isprocessed varchar(2))

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from test_lookup_new order by date

// COMMAND ----------

// MAGIC %sql
// MAGIC insert into test_lookup_new values ('2022-11-30T11:30:00+0000',1,'N')

// COMMAND ----------

val df = sql("select max(date) as min_date from test_lookup_new group by date order by date desc limit 1")

// COMMAND ----------

// MAGIC %sql
// MAGIC SET TIME ZONE LOCAL

// COMMAND ----------

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Date
import java.text.SimpleDateFormat

// COMMAND ----------

val df_cuurent_max_timestamp = sql("SELECT CURRENT_TIMESTAMP as max_time")
var cuurent_max_timestamp1 = df_cuurent_max_timestamp.select("max_time").map(f=>f.getTimestamp(0)).collect.toList
var cuurent_max_timestamp2 = cuurent_max_timestamp1.head
var formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
var cuurent_max_timestamp3 = formatter.format(cuurent_max_timestamp2.getTime())

// COMMAND ----------

var s:String =cuurent_max_timestamp3
var  simpleDateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
var  cuurent_max_timestamp:Date = simpleDateFormat.parse(s);

// COMMAND ----------

var min_timestamp1 = df.select("min_date").map(f=>f.getTimestamp(0)).collect.toList
var min_timestamp = min_timestamp1.head
var formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
var dateFormat = new SimpleDateFormat("yyyy/MM/dd/HH")
var date_folder = dateFormat.format(min_timestamp)
var next_timestamp = formatter.format(min_timestamp.getTime()+ (1000 * 60 * 60 * 1))
var min_timestamp2 = formatter.format(min_timestamp.getTime())

// COMMAND ----------

var s:String =min_timestamp2
var  simpleDateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
var  min_date:Date = simpleDateFormat.parse(s);

// COMMAND ----------

// DBTITLE 1,insert query
sql("insert into test_lookup_new values ("+"'"+next_timestamp+"'"+",1,'N')")

// COMMAND ----------

while( min_date < max_date ){
         println( "Value of min_date: " );
         }

// COMMAND ----------

// MAGIC %md ##ignore below

// COMMAND ----------

// import java.text.SimpleDateFormat
// var min_timestamp1 = df.select("min_date").map(f=>f.getTimestamp(0)).collect.toList
// var min_timestamp = min_timestamp1.head

// COMMAND ----------

//var next_timestamp = formatter.format(min_timestamp.getTime()+ (1000 * 60 * 60 * 1))

// COMMAND ----------

// var s:String =next_timestamp
//  var  simpleDateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//  var  max_date:Date = simpleDateFormat.parse(s);
// //  val ans = new SimpleDateFormat("yyyy/mm/dd").format(date) 
// //  println(ans)

// COMMAND ----------

// var s:String =min_timestamp2
//  var  simpleDateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//  var  min_date:Date = simpleDateFormat.parse(s);
// //  val ans = new SimpleDateFormat("yyyy/mm/dd").format(date) 
// //  println(ans)

// COMMAND ----------

while( min_date < max_date ){
         println( "Value of min_date: " );
         }

// COMMAND ----------

import java.time.LocalDate
import java.time.format.DateTimeFormatter
val format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
val start_dt = LocalDate.parse(min_timestamp, format)
val end_dt = LocalDate.parse(cuurent_max_timestamp, format)
val date_diff = end_dt.toEpochDay() - start_dt.toEpochDay()
//val date_diff = cuurent_max_timestamp.toEpochDay() - min_timestamp.toEpochDay()

// COMMAND ----------

import java.time.LocalDate
import java.time.format.DateTimeFormatter
 
val format = DateTimeFormatter.ofPattern("yyyy-MM-dd")
val start_dt = LocalDate.parse("2020-02-01", format)
val end_dt = LocalDate.parse("2020-02-20", format)
val date_diff = end_dt.toEpochDay() - start_dt.toEpochDay()
 
for (i <- 0l to date_diff by 1) {
  val s = start_dt.plusDays(i)
  println(s)
}

// COMMAND ----------

// import java.text.SimpleDateFormat
// var min_timestamp1 = df.select("min_date").map(f=>f.getTimestamp(0)).collect.toList
// var min_timestamp = min_timestamp1.head
// val formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")
// var date = formatter.format(min_timestamp.getTime())
// val dateFormat = new SimpleDateFormat("yyyy/MM/dd/HH")
// val date_folder = dateFormat.format(min_timestamp)
// // val hourFormat = new SimpleDateFormat("HH")
// // val min_hour = hourFormat.format(min_timestamp)

// COMMAND ----------

// import java.text.SimpleDateFormat
// var min_timestamp1 = df.select("min_date").map(f=>f.getTimestamp(0)).collect.toList
// var min_timestamp = min_timestamp1.head
// var formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")
// //var min_date = formatter.format(min_timestamp.getTime())
// var dateFormat = new SimpleDateFormat("yyyy/MM/dd/HH")
// var date_folder = dateFormat.format(min_timestamp)
// var new_date = formatter.format(min_timestamp.getTime()+ (1000 * 60 * 60 * 1))
// // val hourFormat = new SimpleDateFormat("HH")
// // val min_hour = hourFormat.format(min_timestamp)

// COMMAND ----------

var new_date = formatter.format(min_timestamp.getTime()+ (1000 * 60 * 60 * 1))

// COMMAND ----------

// import java.util.Date;
// import java.text.SimpleDateFormat
 
// var MILLIS_IN_A_DAY = 1000 * 60 * 60 * 24;
// var date = new Date();
// var days: Int = -2;
// var newDate = new Date(date.getTime() + (MILLIS_IN_A_DAY * days));
 
// println("New date: \n" + newDate);
 
// val dateFormat = new SimpleDateFormat("yyyy/MM/dd")
// val date_folder = dateFormat.format(newDate)
 
// print(date_folder)

// COMMAND ----------

//val df = sql("select max(date) as min_date,max(hour_path) as min_hour from test_lookup group by date order by date desc limit 1")

// COMMAND ----------

display(df)

// COMMAND ----------

var min_date1 = df.select("min_date").map(f=>f.getDate(0)).collect.toList
var min_date = min_date1.head

var min_hour1 = df.select("min_hour").map(f=>f.getInt(0)).collect.toList
var min_hour = min_hour1.head
println(min_date)
println(min_hour)

// COMMAND ----------

import java.util.Calendar
var currentdate = java.time.LocalDate.now
var now = Calendar.getInstance()
var currenthour = now.get(Calendar.HOUR_OF_DAY)
println(currentdate)
println(currenthour)

// COMMAND ----------

if (min_hour <=23)
{min_hour = min_hour+1}
{print( min_hour)}

// COMMAND ----------

if (min_date <=currentdate)
//{min_hour = min_hour+1}
{print( min_date)}
