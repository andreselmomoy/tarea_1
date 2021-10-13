from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, udf
from pyspark.sql.types import (DateType, IntegerType, FloatType, StringType,
                               StructField, StructType, TimestampType)

spark = SparkSession.builder.appName("Read Estudiante").getOrCreate()

# Load  file  Estudiante...
estudiante_schema = StructType([StructField('Num_Carnet', IntegerType()),
                          StructField("Name", StringType()),
                         StructField('Career', StringType()),
                         ])

estudiante_df = spark.read.csv("estudiante.csv",
                           schema=estudiante_schema,
                           header=True)

estudiante_df.show()

# Load  file  Curso...
curso_schema = StructType([StructField("Codigo", IntegerType()),
                          StructField("Credit", IntegerType()),
                          StructField("Career", StringType())])

curso_df = spark.read.csv('curso.csv',
                          schema=curso_schema,
                          header=True)


curso_df.show()







