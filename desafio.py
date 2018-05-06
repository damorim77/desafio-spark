from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession as spark
from pyspark.sql import SQLContext
from os import getcwd

# configurações do spark
conf = SparkConf().setAppName("desafio")
sc = SparkContext(conf=conf)
sql_context = SQLContext(sc)

# diminui a quantidade de logs na tela
logger = sc._jvm.org.apache.log4j
logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )


# obtém o caminho do diretório atual, onde se supõem estar os datasets da NASA
PATH = getcwd()

#lê os dois datasets

df_aug = (
    sql_context.read
    .format('com.databricks.spark.csv')
    .options(delimiter = " ")
	.options(inferSchema = True)
    .load(PATH + "/NASA_access_log_Aug95")
)

df_jul = (
    sql_context.read
    .format('com.databricks.spark.csv')
    .options(delimiter = " ")
	.options(inferSchema = True)
    .load(PATH + "/NASA_access_log_Jul95")
)

#combina os datasets, e renomeia as colunas.

df = df_aug.union(df_jul) \
		.selectExpr(
			"_c0 as host", 
			"_c3 as date", 
			"_c4 as timezone", 
			"_c5 as request", 
			"_c6 as status", 
			"_c7 as numbytes" )

#Questão 1

counts = df.select("host") \
		.groupBy("host") \
		.count()
counts.show()


#Questão 2

df404 = df.filter(df["status"]==404 ).cache()

counts = df404.groupBy("status")\
		.count()
counts.show()

#Questão 3

counts = df404.groupBy('host')\
			.count()\
			.orderBy('count', ascending=False)\
			.limit(5)
counts.show()


#Questão 4

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def formatDate(x): return x[1:12]

formatDateUdf = udf(lambda y: formatDate(y), StringType())

df2 = df404.withColumn( 'newdate', formatDateUdf(df.date))

counts = df2.groupBy('newdate')\
			.count()
counts.show()

df404.unpersist()


#Questão 5

from pyspark.sql.functions import sum
from pyspark.sql.types import DoubleType

df2 = df.withColumn("bytes", df["numbytes"]\
		.cast(DoubleType()))

a = df2.groupBy()\
	.sum("bytes")\
	.first() 

print(a)