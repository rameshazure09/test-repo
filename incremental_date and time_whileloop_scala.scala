// Databricks notebook source
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Date
import java.text.SimpleDateFormat
import scala.math.Ordering.Implicits._

// COMMAND ----------

val df_min_date = sql("select max(date) as min_date from test_log_lookup_new group by date order by date desc limit 1")
var min_timestamp1 = df_min_date.select("min_date").map(f=>f.getTimestamp(0)).collect.toList
var min_timestamp = min_timestamp1.head
// var formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
// var dateFormat = new SimpleDateFormat("yyyy/MM/dd/HH")
// var date_folder = dateFormat.format(min_timestamp)
// //var next_timestamp = formatter.format(min_timestamp.getTime()+ (1000 * 60 * 60 * 1))
// var min_timestamp2 = formatter.format(min_timestamp.getTime())
// var s:String =min_timestamp2
// var  simpleDateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
// var  min_date:Date = simpleDateFormat.parse(s);

// COMMAND ----------

val df_cuurent_max_timestamp = sql("SELECT CURRENT_TIMESTAMP as max_time")
var cuurent_max_timestamp1 = df_cuurent_max_timestamp.select("max_time").map(f=>f.getTimestamp(0)).collect.toList
var cuurent_max_timestamp = cuurent_max_timestamp1.head
// var formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
// var cuurent_max_timestamp3 = formatter.format(cuurent_max_timestamp2.getTime())
// var s:String =cuurent_max_timestamp3
// var  simpleDateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
// var  max_date:Date = simpleDateFormat.parse(s);

// COMMAND ----------

val df_time_diff = sql("select datediff(HOUR,TIMESTAMP'"+min_timestamp+"'"+",TIMESTAMP'"+cuurent_max_timestamp+"') as time_diff")
var df_time_diff1 = df_time_diff.select("time_diff").map(f=>f.getLong(0)).collect.toList
var diff_num_hrs = df_time_diff1.head
println(diff_num_hrs)

// COMMAND ----------

do {
   val df_min_date = sql("select max(date) as min_date from test_log_lookup_new group by date order by date desc limit 1");
  var min_timestamp1 = df_min_date.select("min_date").map(f=>f.getTimestamp(0)).collect.toList;
  var min_timestamp = min_timestamp1.head;
  var formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  var dateFormat = new SimpleDateFormat("yyyy/MM/dd/HH");
  var date_folder = dateFormat.format(min_timestamp);
  //here comes the dataframe append method//
  var next_timestamp = formatter.format(min_timestamp.getTime()+ (1000 * 60 * 60 * 1));
  println(next_timestamp)
  sql("insert into test_log_lookup_new values ("+"'"+next_timestamp+"'"+",1,'Y')")
//   var min_timestamp2 = formatter.format(min_timestamp.getTime());
//   var s:String =min_timestamp2;
//   var simpleDateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//   var min_date:Date = simpleDateFormat.parse(s)
//   println(min_date)
  //println(max_date)
  diff_num_hrs = diff_num_hrs-1 ;
} 
while( diff_num_hrs >= 1 );

// COMMAND ----------

// MAGIC 







%md ##ignore below

// COMMAND ----------

     do {
         println( "Value of a: " + a );
         a = a + 1;
      }
      while( a < 20 )

// COMMAND ----------

do {
   val df_min_date = sql("select max(date) as min_date from test_log_lookup_new group by date order by date desc limit 1");
  var min_timestamp1 = df_min_date.select("min_date").map(f=>f.getTimestamp(0)).collect.toList;
  var min_timestamp = min_timestamp1.head;
  var formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  var dateFormat = new SimpleDateFormat("yyyy/MM/dd/HH");
  var date_folder = dateFormat.format(min_timestamp);
  //here comes the dataframe append method//
  var next_timestamp = formatter.format(min_timestamp.getTime()+ (1000 * 60 * 60 * 1));
  println(next_timestamp)
  sql("insert into test_log_lookup_new values ("+"'"+next_timestamp+"'"+",1,'N')")
  var min_timestamp2 = formatter.format(min_timestamp.getTime());
  var s:String =min_timestamp2;
  var simpleDateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  var min_date:Date = simpleDateFormat.parse(s)
  println(min_date)
  println(max_date);
} 
while( (min_date: java.util.Date) <= (max_date: java.util.Date) );

// COMMAND ----------

      var a = 10;

      // do loop execution
      do {
         println( "Value of a: " + a );
         a = a + 1;
      }
      while( a < 20 )
   }

// COMMAND ----------



// COMMAND ----------

println(min_date)
println(max_date)

// COMMAND ----------

do {
  val df_min_date = sql("select max(date) as min_date from test_log_lookup_new group by date order by date desc limit 1");
  var min_timestamp1 = df_min_date.select("min_date").map(f=>f.getTimestamp(0)).collect.toList;
  var min_timestamp = min_timestamp1.head;
  //var formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  var min_timestamp2 = formatter.format(min_timestamp.getTime());
  var s:String =min_timestamp2;
  var simpleDateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  var min_date:Date = simpleDateFormat.parse(s)
  println(min_date)
  //println(max_date)
}
while( (min_date: java.util.Date) <= (max_date: java.util.Date) ){
  
  val df_min_date = sql("select max(date) as min_date from test_log_lookup_new group by date order by date desc limit 1");
  var min_timestamp1 = df_min_date.select("min_date").map(f=>f.getTimestamp(0)).collect.toList;
  var min_timestamp = min_timestamp1.head;
  var formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  var dateFormat = new SimpleDateFormat("yyyy/MM/dd/HH");
  var date_folder = dateFormat.format(min_timestamp);
  //here comes the dataframe append method//
  var next_timestamp = formatter.format(min_timestamp.getTime()+ (1000 * 60 * 60 * 1));
  println(next_timestamp)
  sql("insert into test_log_lookup_new values ("+"'"+next_timestamp+"'"+",1,'N')")
  var min_timestamp2 = formatter.format(min_timestamp.getTime());
  var s:String =min_timestamp2;
  var simpleDateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  var min_date:Date = simpleDateFormat.parse(s)
  println(min_date)
  println(max_date)
  //if ( (min_date: java.util.Date) >= max_date ) break ();
         }

// COMMAND ----------

var a = 10

// COMMAND ----------

do {
   val df_min_date = sql("select max(date) as min_date from test_log_lookup_new group by date order by date desc limit 1");
  var min_timestamp1 = df_min_date.select("min_date").map(f=>f.getTimestamp(0)).collect.toList;
  var min_timestamp = min_timestamp1.head;
  var formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  var dateFormat = new SimpleDateFormat("yyyy/MM/dd/HH");
  var date_folder = dateFormat.format(min_timestamp);
  //here comes the dataframe append method//
  var next_timestamp = formatter.format(min_timestamp.getTime()+ (1000 * 60 * 60 * 1));
  println(next_timestamp)
  sql("insert into test_log_lookup_new values ("+"'"+next_timestamp+"'"+",1,'N')")
  var min_timestamp2 = formatter.format(min_timestamp.getTime());
  var s:String =min_timestamp2;
  var simpleDateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  var min_date:Date = simpleDateFormat.parse(s)
  println(min_date)
  println(max_date);
} 
while( (min_date: java.util.Date) <= (max_date: java.util.Date) );

// COMMAND ----------

while( (min_date: java.util.Date) <= (max_date: java.util.Date) ){
  
  val df_min_date = sql("select max(date) as min_date from test_log_lookup_new group by date order by date desc limit 1");
  var min_timestamp1 = df_min_date.select("min_date").map(f=>f.getTimestamp(0)).collect.toList;
  var min_timestamp = min_timestamp1.head;
  var formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  var dateFormat = new SimpleDateFormat("yyyy/MM/dd/HH");
  var date_folder = dateFormat.format(min_timestamp);
  //here comes the dataframe append method//
  var next_timestamp = formatter.format(min_timestamp.getTime()+ (1000 * 60 * 60 * 1));
  println(next_timestamp)
  sql("insert into test_log_lookup_new values ("+"'"+next_timestamp+"'"+",1,'N')")
  var min_timestamp2 = formatter.format(min_timestamp.getTime());
  var s:String =min_timestamp2;
  var simpleDateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  var min_date:Date = simpleDateFormat.parse(s)
  println(min_date)
  println(max_date)
  //if ( (min_date: java.util.Date) >= max_date ) break ();
         }

// COMMAND ----------

var sql_query = "insert into test_log_lookup_new values ("+"'"+next_timestamp+"'"+",1,'N')"

// COMMAND ----------

if( (min_date: java.util.Date) >= (max_date: java.util.Date) ){
  println("min date is less")
    println(min_date)
  println(max_date)
  
}
else { println("out")
       println(min_date)
  println(max_date)}
