#!/opt/conda/envs/dsenv/bin/python

from pyspark.ml.feature import *
from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression

tokenizer = Tokenizer(inputCol="reviewText", outputCol="words")

hasher = HashingTF(numFeatures=100, inputCol=tokenizer.getOutputCol(), outputCol="word_vector")

lr = LinearRegression(featuresCol=hasher.getOutputCol(), labelCol='overall', maxIter=15, regParam=0.3)

pipeline = Pipeline(stages=[tokenizer, hasher, lr])
