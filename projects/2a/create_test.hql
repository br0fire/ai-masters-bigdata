CREATE TEMPORARY EXTERNAL TABLE hw2_test(
id INT,
if1 INT,
if2 INT,
if3 INT,
if4 INT,
if5 INT,
if6 INT,
if7 INT,
if8 INT,
if9 INT,
if10 INT,
if11 INT,
if12 INT,
if13 INT,
cf1 VARCHAR(8),
cf2 VARCHAR(8),
cf3 VARCHAR(8),
cf4 VARCHAR(8),
cf5 VARCHAR(8),
cf6 VARCHAR(8),
cf7 VARCHAR(8),
cf8 VARCHAR(8),
cf9 VARCHAR(8),
cf10 VARCHAR(8),
cf11 VARCHAR(8),
cf12 VARCHAR(8),
cf13 VARCHAR(8),
cf14 VARCHAR(8),
cf15 VARCHAR(8),
cf16 VARCHAR(8),
cf17 VARCHAR(8),
cf18 VARCHAR(8),
cf19 VARCHAR(8),
cf20 VARCHAR(8),
cf21 VARCHAR(8),
cf22 VARCHAR(8),
cf23 VARCHAR(8),
cf24 VARCHAR(8),
cf25 VARCHAR(8),
cf26 VARCHAR(8)
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'
LOCATION '/datasets/criteo/testdir';