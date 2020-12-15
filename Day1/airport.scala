// Databricks notebook source
//https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/4115004882575784/3168575904313771/3522248626321199/latest.html
val data = sc.textFile("/FileStore/tables/airports-1.text")

// COMMAND ----------

data.take(4)

// COMMAND ----------

// DBTITLE 1,Task 1
val t1 = data.filter(x => (x.split(",")(3) == "\"Iceland\"") && (x.split(",")(6).toDouble > 40)) .collect()

// COMMAND ----------

val dataRdd =data.map( line => (line.split(",")(0), line.split(",")(1)))
dataRdd.take(3)

// COMMAND ----------

// DBTITLE 1,airport not in canada
val notCandaRDD = dataRdd.filter(x => x._2 != "\"Canada\"")

// COMMAND ----------

// DBTITLE 1,convert timestamp to uppercase and show along with country
val apnew = data.map(line => (line.split(",")(1),(line.split(",")(11))))
apnew.mapValues(x=> x.toUpperCase()).take(5)


// COMMAND ----------

// DBTITLE 1,Task 2
data.filter(x => (x.split(",")(11) == "\"Pacific/Port_Moresby\"") && (x.split(",")(8).toInt % 2 == 0)) .collect()
