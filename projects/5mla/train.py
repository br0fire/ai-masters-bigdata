#!/opt/conda/envs/dsenv/bin/python

import os, sys
import logging

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import log_loss
from joblib import dump
import mlflow

from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression

#
# Dataset fields
#
numeric_features = ["if"+str(i) for i in range(1,14)]

fields = ["id", "label"] + numeric_features

#
# Model pipeline
#

# We create the preprocessing pipelines for both numeric and categorical data.
numeric_transformer = Pipeline(steps=[
    ('imputer', SimpleImputer(strategy='median'))
])


preprocessor = ColumnTransformer(
    transformers=[
        ('num', numeric_transformer, numeric_features)
    ]
)

# Now we have a full prediction pipeline.
model = Pipeline(steps=[
    ('preprocessor', preprocessor),
    ('linearregression', LogisticRegression())
])



#
# Logging initialization
#
logging.basicConfig(level=logging.DEBUG)
logging.info("CURRENT_DIR {}".format(os.getcwd()))
logging.info("SCRIPT CALLED AS {}".format(sys.argv[0]))
logging.info("ARGS {}".format(sys.argv[1:]))

#
# Read script arguments
#
try:
  train_path = sys.argv[1]
except:
  logging.critical("Need to pass train dataset path and `tol` hyperparam")
  sys.exit(1)


logging.info(f"TRAIN_PATH {train_path}")

#
# Read dataset
#

read_table_opts = dict(sep="\t", names=fields, index_col=False)
df = pd.read_table(train_path, **read_table_opts)
#split train/test
X_train, X_test, y_train, y_test = train_test_split(
    df.iloc[:, 2:], df.iloc[:, 1], test_size=0.33, random_state=42
)

#
# Train the model
#
with mlflow.start_run():
    model.fit(X_train, y_train)

print(f"fit completed")

model_score = model.score(X_test, y_test)

print(f"log_loss on validation: {model_score:.3f}")

mlflow.sklearn.log_model(model, 'model')
mlflow.log_metric("log_loss", model_score)
mlflow.log_param("model_param1", float(sys.argv[2]))