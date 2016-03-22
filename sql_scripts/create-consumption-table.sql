USE DB_EDF;

DROP TABLE IF EXISTS temp_consumption;
DROP TABLE IF EXISTS consumption;

CREATE TABLE temp_consumption (
site_id INT,
timestp BIGINT,
temperature FLOAT,
consumption FLOAT)
COMMENT'This is the consumption and temperature datas'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH '/tmp/consumptiontable.csv' 
INTO TABLE temp_consumption;

CREATE TABLE consumption (
timestp BIGINT,
temperature FLOAT,
consumption FLOAT)
COMMENT'This is the consumption streaming data'
PARTITIONED BY (site_id INT) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY','
STORED AS ORC 
TBLPROPERTIES("orc.compress"="SNAPPY");

INSERT OVERWRITE TABLE consumption PARTITION (site_id)
SELECT timestp, temperature, consumption, site_id
FROM temp_consumption;

DROP TABLE temp_consumption;
