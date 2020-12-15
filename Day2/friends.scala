// Databricks notebook source
//https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/4115004882575784/3495989257757751/3522248626321199/latest.html
val data =  sc.textFile("/FileStore/tables/FriendsData.csv")

// COMMAND ----------

data.collect()

// COMMAND ----------

val removedHeader = data.filter(x => !x.contains("friends"))
removedHeader.collect()

// COMMAND ----------

val split = removedHeader.map(x => x.split(","))

// COMMAND ----------

val dataRDD = removedHeader.map( x => (x.split(",")(2).toInt , (1,(x.split(",")(3).toInt ))))

// COMMAND ----------

dataRDD.take(20)

// COMMAND ----------

val reducedRDD = dataRDD.reduceByKey( (x,y) => (x._1 + y._1, x._2+ y._2)).take (20)

// COMMAND ----------

reducedRDD.take(20)


// COMMAND ----------

val average = reducedRDD.map { x =>
                           val temp = x._2
                           val total = temp._2
                           val count = temp._1
                           (x._1, total / count)
                           } .take(20)


// COMMAND ----------

// DBTITLE 1,task 1 result
for ((age,avg) <- average.take(50)) println(age + " : "+avg)

// COMMAND ----------

dataRDD.take(20)

// COMMAND ----------

val dataRDD2 = removedHeader.map( x => (x.split(",")(2).toInt , ((x.split(",")(3).toInt ))))
dataRDD2.take(30)

// COMMAND ----------

val reducedRDD2 = dataRDD2.reduceByKey( (x,y) => Math.max(x,y)).take (30)

// COMMAND ----------

// DBTITLE 1,task 2 results
for ((age,maxfrnd) <- reducedRDD2.take(50)) println(age + " : "+maxfrnd)
