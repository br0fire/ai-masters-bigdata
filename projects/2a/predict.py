#!/opt/conda/envs/dsenv/bin/python

import sys, os
import logging
from joblib import load
import pandas as pd

sys.path.append('.')
from model import fields

#
# Init the logger
#
logging.basicConfig(level=logging.DEBUG)
logging.info("CURRENT_DIR {}".format(os.getcwd()))
logging.info("SCRIPT CALLED AS {}".format(sys.argv[0]))
logging.info("ARGS {}".format(sys.argv[1:]))

#load the model
model = load("2a.joblib")
fields.remove('label')
#read and infere
read_opts=dict(
        sep='\t', names=fields, index_col=False, header=None,
       iterator=True, chunksize=100, na_values='\\N'
) 
# kek = pd.read_csv(sys.stdin, **read_opts)
# print(kek)
for df in pd.read_csv(sys.stdin, **read_opts):
     pred = model.predict_proba(df.drop(['id'], axis=1))
     pred = [x[0] for x in pred]
     out = zip(df.id, pred)
     print("\n".join(["{0}\t{1}".format(*i) for i in out]))

