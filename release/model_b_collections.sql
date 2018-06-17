use change_me;
!echo ------------------------;
!echo ------------------------ earningsData_SELF_REPORTED;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_earningsdata_earningsdata_self_reported
;

CREATE EXTERNAL TABLE src_accepted_data_earningsdata_earningsdata_self_reported(
`claimElement` STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING>
,`contractId`:STRING
,`declaredDateTime`:STRING>
,`invalidated` STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`acceptedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`d_oid`:STRING>
,`declarationId` STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/earningsData';

DROP TABLE IF EXISTS earningsdata_self_reported;

CREATE TABLE earningsdata_self_reported STORED AS ORC AS SELECT 
 
CASE WHEN SUBSTRING(`acceptedDateTime`.`d_date`, LENGTH(`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END as `accepteddatetime_d_date`, 
`claimElement`.`contractId` as `claimelement_contractid`, 
`claimElement`.`declaredDateTime` as `claimelement_declareddatetime`, 
CAST(`claimElement`.`effectiveDate`.`hasDate` as STRING) as `claimelement_effectivedate_hasdate`, 
`claimElement`.`effectiveDate`.`knownDate` as `claimelement_effectivedate_knowndate`, 
CAST(`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING) as `claimelement_paymenteffectivedate_hasdate`, 
`claimElement`.`paymentEffectiveDate`.`knownDate` as `claimelement_paymenteffectivedate_knowndate`, 
CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END as `createddatetime_d_date`, 
`declarationId` as `declarationid`, 
CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END as `invalidated_datetime_d_date`, 
`invalidated`.`reason` as `invalidated_reason`, 
`_id`.`d_oid` as `id` FROM src_accepted_data_earningsdata_earningsdata_self_reported
 src_accepted_data_earningsdata_earningsdata_self_reported 
 where `_removed`.undefined='SELF_REPORTED' or undefined='SELF_REPORTED';


!echo ------------------------;
!echo ------------------------ earningsData_SELF_EMPLOYED_EARNINGS_WITH_LATEST_MIF_DATA;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_earningsdata_earningsdata_self_employed_earnings_with_latest_mif_data
;

CREATE EXTERNAL TABLE src_accepted_data_earningsdata_earningsdata_self_employed_earnings_with_latest_mif_data(
`claimElement` STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING>
,`contractId`:STRING
,`declaredDateTime`:STRING>
,`invalidated` STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`acceptedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`d_oid`:STRING>
,`declarationId` STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/earningsData';

DROP TABLE IF EXISTS earningsdata_self_employed_earnings_with_latest_mif_data;

CREATE TABLE earningsdata_self_employed_earnings_with_latest_mif_data STORED AS ORC AS SELECT 
 
CASE WHEN SUBSTRING(`acceptedDateTime`.`d_date`, LENGTH(`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END as `accepteddatetime_d_date`, 
`claimElement`.`contractId` as `claimelement_contractid`, 
`claimElement`.`declaredDateTime` as `claimelement_declareddatetime`, 
CAST(`claimElement`.`effectiveDate`.`hasDate` as STRING) as `claimelement_effectivedate_hasdate`, 
`claimElement`.`effectiveDate`.`knownDate` as `claimelement_effectivedate_knowndate`, 
CAST(`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING) as `claimelement_paymenteffectivedate_hasdate`, 
`claimElement`.`paymentEffectiveDate`.`knownDate` as `claimelement_paymenteffectivedate_knowndate`, 
CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END as `createddatetime_d_date`, 
`declarationId` as `declarationid`, 
CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END as `invalidated_datetime_d_date`, 
`invalidated`.`reason` as `invalidated_reason`, 
`_id`.`d_oid` as `id` FROM src_accepted_data_earningsdata_earningsdata_self_employed_earnings_with_latest_mif_data
 src_accepted_data_earningsdata_earningsdata_self_employed_earnings_with_latest_mif_data 
 where `_removed`.undefined='SELF_EMPLOYED_EARNINGS_WITH_LATEST_MIF_DATA' or undefined='SELF_EMPLOYED_EARNINGS_WITH_LATEST_MIF_DATA';


!echo ------------------------;
!echo ------------------------ earningsData_SELF_EMPLOYED;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_earningsdata_earningsdata_self_employed
;

CREATE EXTERNAL TABLE src_accepted_data_earningsdata_earningsdata_self_employed(
`claimElement` STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING>
,`contractId`:STRING
,`declaredDateTime`:STRING>
,`invalidated` STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`acceptedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`d_oid`:STRING>
,`declarationId` STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/earningsData';

DROP TABLE IF EXISTS earningsdata_self_employed;

CREATE TABLE earningsdata_self_employed STORED AS ORC AS SELECT 
 
CASE WHEN SUBSTRING(`acceptedDateTime`.`d_date`, LENGTH(`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END as `accepteddatetime_d_date`, 
`claimElement`.`contractId` as `claimelement_contractid`, 
`claimElement`.`declaredDateTime` as `claimelement_declareddatetime`, 
CAST(`claimElement`.`effectiveDate`.`hasDate` as STRING) as `claimelement_effectivedate_hasdate`, 
`claimElement`.`effectiveDate`.`knownDate` as `claimelement_effectivedate_knowndate`, 
CAST(`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING) as `claimelement_paymenteffectivedate_hasdate`, 
`claimElement`.`paymentEffectiveDate`.`knownDate` as `claimelement_paymenteffectivedate_knowndate`, 
CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END as `createddatetime_d_date`, 
`declarationId` as `declarationid`, 
CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END as `invalidated_datetime_d_date`, 
`invalidated`.`reason` as `invalidated_reason`, 
`_id`.`d_oid` as `id` FROM src_accepted_data_earningsdata_earningsdata_self_employed
 src_accepted_data_earningsdata_earningsdata_self_employed 
 where `_removed`.undefined='SELF_EMPLOYED' or undefined='SELF_EMPLOYED';


!echo ------------------------;
!echo ------------------------ earningsData_CALCULATED_EARNINGS;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_earningsdata_earningsdata_calculated_earnings
;

CREATE EXTERNAL TABLE src_accepted_data_earningsdata_earningsdata_calculated_earnings(
`claimElement` STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING>
,`contractId`:STRING
,`declaredDateTime`:STRING>
,`invalidated` STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`acceptedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`d_oid`:STRING>
,`declarationId` STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/earningsData';

DROP TABLE IF EXISTS earningsdata_calculated_earnings;

CREATE TABLE earningsdata_calculated_earnings STORED AS ORC AS SELECT 
 
CASE WHEN SUBSTRING(`acceptedDateTime`.`d_date`, LENGTH(`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END as `accepteddatetime_d_date`, 
`claimElement`.`contractId` as `claimelement_contractid`, 
`claimElement`.`declaredDateTime` as `claimelement_declareddatetime`, 
CAST(`claimElement`.`effectiveDate`.`hasDate` as STRING) as `claimelement_effectivedate_hasdate`, 
`claimElement`.`effectiveDate`.`knownDate` as `claimelement_effectivedate_knowndate`, 
CAST(`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING) as `claimelement_paymenteffectivedate_hasdate`, 
`claimElement`.`paymentEffectiveDate`.`knownDate` as `claimelement_paymenteffectivedate_knowndate`, 
CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END as `createddatetime_d_date`, 
`declarationId` as `declarationid`, 
CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END as `invalidated_datetime_d_date`, 
`invalidated`.`reason` as `invalidated_reason`, 
`_id`.`d_oid` as `id` FROM src_accepted_data_earningsdata_earningsdata_calculated_earnings
 src_accepted_data_earningsdata_earningsdata_calculated_earnings 
 where `_removed`.undefined='CALCULATED_EARNINGS' or undefined='CALCULATED_EARNINGS';


!echo ------------------------;
!echo ------------------------ earningsData_RTE_FEED_TO_DO;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_earningsdata_earningsdata_rte_feed_to_do
;

CREATE EXTERNAL TABLE src_accepted_data_earningsdata_earningsdata_rte_feed_to_do(
`claimElement` STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING>
,`contractId`:STRING
,`declaredDateTime`:STRING>
,`invalidated` STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`acceptedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`d_oid`:STRING>
,`declarationId` STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/earningsData';

DROP TABLE IF EXISTS earningsdata_rte_feed_to_do;

CREATE TABLE earningsdata_rte_feed_to_do STORED AS ORC AS SELECT 
 
CASE WHEN SUBSTRING(`acceptedDateTime`.`d_date`, LENGTH(`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END as `accepteddatetime_d_date`, 
`claimElement`.`contractId` as `claimelement_contractid`, 
`claimElement`.`declaredDateTime` as `claimelement_declareddatetime`, 
CAST(`claimElement`.`effectiveDate`.`hasDate` as STRING) as `claimelement_effectivedate_hasdate`, 
`claimElement`.`effectiveDate`.`knownDate` as `claimelement_effectivedate_knowndate`, 
CAST(`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING) as `claimelement_paymenteffectivedate_hasdate`, 
`claimElement`.`paymentEffectiveDate`.`knownDate` as `claimelement_paymenteffectivedate_knowndate`, 
CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END as `createddatetime_d_date`, 
`declarationId` as `declarationid`, 
CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END as `invalidated_datetime_d_date`, 
`invalidated`.`reason` as `invalidated_reason`, 
`_id`.`d_oid` as `id` FROM src_accepted_data_earningsdata_earningsdata_rte_feed_to_do
 src_accepted_data_earningsdata_earningsdata_rte_feed_to_do 
 where `_removed`.undefined='RTE_FEED_TO_DO' or undefined='RTE_FEED_TO_DO';


