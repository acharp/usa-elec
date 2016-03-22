USE DB_EDF;

DROP TABLE IF EXISTS sites_with_geo;

CREATE TABLE sites_with_geo (
site_id INT,
industry STRING,
sub_industry STRING,
square_feet INT,
latitute DOUBLE,
longitude DOUBLE,
timezone STRING,
offset STRING
)
COMMENT'This is the temp sites streaming data'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH '/tmp/all_sites.csv' 
INTO TABLE sites_with_geo;

CREATE INDEX site_idx_geo
ON TABLE sites_with_geo(site_id) 
AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' 
WITH DEFERRED REBUILD;
