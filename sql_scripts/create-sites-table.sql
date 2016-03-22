USE DB_EDF;

DROP TABLE IF EXISTS temp_sites;
DROP TABLE IF EXISTS sites;

CREATE TABLE temp_sites (
site_id INT,
industry STRING,
sub_industry STRING,
square_feet INT)
COMMENT'This is the temp sites streaming data'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH '/tmp/filtered_sites.csv' 
INTO TABLE temp_sites;

CREATE TABLE sites (
site_id INT,
sub_industry STRING,
square_feet INT)
COMMENT'This is the sites streaming data'
PARTITIONED BY (industry STRING) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY',' 
STORED AS ORC
TBLPROPERTIES("orc.compress"="SNAPPY");

INSERT OVERWRITE TABLE sites PARTITION (industry)
SELECT site_id, sub_industry, square_feet, industry
FROM temp_sites;

CREATE INDEX site_idx 
ON TABLE sites(site_id) 
AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' 
WITH DEFERRED REBUILD;

DROP TABLE temp_sites;
