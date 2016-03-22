CREATE DATABASE DB_EDF;
USE DB_EDF;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.compress.output = true;  
SET mapred.output.compression.type = BLOCK;  
SET mapred.output.compression.codec = org.apache.hadoop.io.compress.SnappyCodec;