# -*- coding: utf-8 -*-
"""
Created on Tue Jan  7 12:07:27 2020

@author: vicma
"""

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext


#if __name__ == '__main__':


scSpark = SparkSession \
    .builder \
    .appName("reading csv") \
    .config("spark.jars", "postgresql-42.2.9.jar") \
    .getOrCreate()

        
#data_file = 'data*.csv'
#sdfData = scSpark.read.csv(data_file, header=True, sep=",").cache()
#print('Total Records = {}'.format(sdfData.count()))
#sdfData.show()


data_file = 'supermarket_sales.csv'
sdfData = scSpark.read.csv(data_file, header=True, sep=",").cache()
gender = sdfData.groupBy('Gender').count()
#print(gender.show())

sdfData.registerTempTable("sales")
#output =  scSpark.sql('SELECT * from sales')
#output.show(2)

output = scSpark.sql('SELECT * from sales WHERE `Unit Price` < 15 AND Quantity < 10')
#output.show()

#output = scSpark.sql('SELECT COUNT(*) as total, City from sales GROUP BY City')
#output.show()

#output.write.format('json').save('filtered.json')
#
#output.coalesce(1).write.format('json').save('singlefiltered.json')

mode = "overwrite"
url = "jdbc:postgresql://localhost:5432/spark"
properties = {"user": "postgres","password": "admin","driver": "org.postgresql.Driver"}
output.write.jdbc(url=url, table="spark_result", mode=mode, properties=properties)

