// Databricks notebook source
//  /FileStore/tables/nasa_august.tsv
//  /FileStore/tables/nasa_july.

val julyRdd = sc.textFile("/FileStore/tables/nasa_july.tsv")
val augustRdd = sc.textFile("/FileStore/tables/nasa_august.tsv")

// COMMAND ----------

julyRdd.union(augustRdd).take(3)

// COMMAND ----------

val unionRdd = julyRdd.union(augustRdd)


// COMMAND ----------

unionRdd.take(2)

// COMMAND ----------

val header = unionRdd.first

// COMMAND ----------

// DBTITLE 1,first approach
unionRdd.filter(line => line!=header).take(2)

// COMMAND ----------

// DBTITLE 1,second approach
def headerRemover(line : String): Boolean = !(line startsWith("host"))

// COMMAND ----------

unionRdd.filter(x => headerRemover(x)).take(2)

// COMMAND ----------

val task11 = unionRdd.filter( x => (x(5).toInt >= 200) && (x(5).toInt > 1000))

// COMMAND ----------

task11.collect()

// COMMAND ----------

julyRdd.filter(x => x.split("\t"),(1))

// COMMAND ----------

augustRdd.filter(x => x.split("\t"),(1))

// COMMAND ----------

julyRdd.take(3)

// COMMAND ----------

val julyNew =julyRdd.filter(x => headerRemover(x))

// COMMAND ----------

val augNew =augustRdd.filter(x => headerRemover(x))

// COMMAND ----------

val i1 = julyNew.map(x => x.split("\t")(0))

// COMMAND ----------

val i2 = augNew.map(x => x.split("\t")(0))

// COMMAND ----------

val intersection = i1.intersection(i2).collect()
