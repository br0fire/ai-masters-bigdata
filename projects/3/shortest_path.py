# preparations
import os
import sys
import pyspark.sql.functions as f
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *

SPARK_HOME = "/usr/lib/spark3"
PYSPARK_PYTHON = "/opt/conda/envs/dsenv/bin/python"
os.environ["PYSPARK_PYTHON"]= PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"]= PYSPARK_PYTHON
os.environ["SPARK_HOME"] = SPARK_HOME

PYSPARK_HOME = os.path.join(SPARK_HOME, "python/lib")
sys.path.insert(0, os.path.join(PYSPARK_HOME, "py4j-0.10.9.3-src.zip"))
sys.path.insert(0, os.path.join(PYSPARK_HOME, "pyspark.zip"))

conf = SparkConf()
spark = SparkSession.builder.config(conf=conf).appName("Pagerank").getOrCreate()


source = int(sys.argv[1])
destination = int(sys.argv[2])
dataset_path = sys.argv[3]


graph = spark.read.csv(
    dataset_path, sep="\t", schema=
        StructType([
            StructField("user_id", IntegerType()),
            StructField("follower_id", IntegerType())
        ])
)       
graph.cache()   

collected_data = spark.createDataFrame(
    [(source, 0, [])], schema=
        StructType([
            StructField("vertex", IntegerType(), False),
            StructField("distance", IntegerType(), False),
            StructField("path", ArrayType(IntegerType()), False),
        ])
)



d = 0

while True:
    forefront = (
        collected_data
            .join(graph, on=(collected_data.vertex==graph.follower_id))
            .cache()
    ) 
    
    forefront = (
        forefront.select(
            forefront['user_id'].alias("vertex"),
            (collected_data['distance'] + 1).alias("distance"),
            f.concat(forefront["path"], f.array(forefront["vertex"])).alias('path')
        )
    )
    with_newly_added = (
        collected_data
            .join(forefront, on="vertex", how="full_outer") 
            .select(
                "vertex",
                f.when(collected_data.distance.isNotNull(), collected_data.distance)
                    .otherwise(forefront.distance)
                    .alias("distance"),
            )
            .persist()
    )

    count = with_newly_added.where(with_newly_added.distance == d + 1).count()

    if count == 0:
        break  

    d += 1            
    collected_data = forefront
    target = with_newly_added.where(with_newly_added.vertex == destination).count()

    if target > 0:
        break
ans = (
    collected_data
        .where(collected_data.vertex == destination)
        .withColumn('last', f.lit(destination))
        .select(
            f.concat_ws(',', f.concat("path", f.array("last"))).alias('path')
        )
)

ans.select('path').write.text(sys.argv[4])