#!/opt/conda/envs/dsenv/bin/python

import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

from model import pipeline

train_path = sys.argv[1]
model_path = sys.argv[2]

schema = StructType(fields=[
    StructField("id", IntegerType()),
    StructField("overall", FloatType()),
    StructField("vote", StringType()),
    StructField("reviewTime", StringType()),
    StructField("reviewerID", StringType()),
    StructField("asin", StringType()),
    StructField("reviewerName", StringType()),
    StructField("reviewText", StringType()),
    StructField("summary", StringType()),
    StructField("unixReviewTime", IntegerType())
])

dataset = spark.read.json(train_path, schema=schema)

dataset = dataset.fillna({'reviewText':''})

pipeline_model = pipeline.fit(dataset)

pipeline_model.write().overwrite().save(model_path)