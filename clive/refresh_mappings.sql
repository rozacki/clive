-- generate only one data file
SET hive.tez.max.partition.factor=1;

use uc_clive;
-- Truncate table
truncate table mapping_text;

-- Load technical mapping clive working folder
load data local inpath "/home/uc/release/mi/processes/clive/production/*.csv" into table mapping_text;

-- Convert into ORC format
drop table mapping;
create table mapping stored as orc as select * from mapping_text;