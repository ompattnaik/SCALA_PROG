// Databricks notebook source
val data = sc.textFile("/FileStore/tables/airports-1.text")

// COMMAND ----------

data.take(4)

// COMMAND ----------

data.filter(x => (x.split(",")(3) == "\"Iceland\"") && (x.split(",")(6).toDouble > 40)) .collect()

// COMMAND ----------

data.count(data.filter(x => (x.split(",")(3) == "\"Iceland\"")))

// COMMAND ----------

val dataRdd =data.map( line => (line.split(",")(0), line.split(",")(1)))
dataRdd.take(3)

// COMMAND ----------

// DBTITLE 1,airport not in canada
val notCandaRDD = dataRdd.filter(x => x._2 != "\"Canada\"")

// COMMAND ----------

val tup = ("Om",2020)
tup._2

// COMMAND ----------

val List1 = List("Om 1998","Kaushik 1952","Saurabh 2001","Saroj 1500")
val rddn = sc.parallelize(List1)
val splt = rddn.map(line => (line.split(" ")(0),(line.split(" ")(1).toInt)))
splt.mapValues(x=> x+10).take(4)

// COMMAND ----------

splt.collect()

// COMMAND ----------

val apnew = data.map(line => (line.split(",")(1),(line.split(",")(11))))
apnew.mapValues(x=> x.toUpperCase()).take(5)


// COMMAND ----------


