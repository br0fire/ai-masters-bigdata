#!/opt/conda/envs/dsenv/bin/python
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *

model_path = sys.argv[1]
test_path = sys.argv[2]
save_path = sys.argv[3]

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

from pyspark.ml import Pipeline, PipelineModel

model = PipelineModel.load(model_path)

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

dataset = spark.read.json(test_path, schema=schema)

dataset = dataset.fillna({'reviewText':''})

predictions = model.transform(dataset)

predictions.write().overwrite().save(save_path)