# Databricks notebook source
#https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/4115004882575784/3933477043819392/3522248626321199/latest.html
total =1440000
print("Eldest\t\t  Middle\tYoungest")
print(((13*total)/36),"\t",((12*total)/36),"\t",((11*total)/36))


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

a=int(input("Enter height of first wall"))
b=int(input("Enter width of first wall"))
c=int(input("Enter height of second wall"))
d=int(input("Enter width of second wall"))
print("Total Price =",((a*b)+(c*d)*120*0.092903)) #sq ft to sq meter conversion

print(a)

# COMMAND ----------

seconds =160000

seconds = seconds % (24 * 3600)
hour = seconds // 3600
seconds %= 3600
minutes = seconds // 60
seconds %= 60
print  (hour,":",minutes,":",seconds)

# COMMAND ----------


