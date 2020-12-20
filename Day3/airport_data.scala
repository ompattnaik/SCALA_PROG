// Databricks notebook source
//https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/4115004882575784/59813474555419/3522248626321199/latest.html
val data = sc.textFile("/FileStore/tables/Airport_data_https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/4115004882575784/59813474555419/3522248626321199/latest.htmlcsv___Airport_data_csv.csv")

// COMMAND ----------

data.take(20)

// COMMAND ----------

val split = data.map(x => x.split(",")).take(20)

// COMMAND ----------

val newsplit = split.filter(x => ! x(1).contains("NO_DATA") )

// COMMAND ----------

val dataRdd =split.map( x => (x(0),x(2).toInt)).take(30)
val dataRdd2 = sc.parallelize(dataRdd)

// COMMAND ----------

val maxtemp = dataRdd2.reduceByKey( (x,y) => Math.max(x,y)).take (30)

// COMMAND ----------

val mintemp = dataRdd2.reduceByKey( (x,y) => Math.min(x,y)).take (30)
