use change_me;
use change_me;
use change_me;
use change_me;
use change_me;
use change_me;
!echo ------------------------;
!echo ------------------------ overlappingBenefit;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_overlappingbenefit_overlappingbenefit
;

CREATE EXTERNAL TABLE src_accepted_data_overlappingbenefit_overlappingbenefit(
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
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/overlappingBenefit';

DROP TABLE IF EXISTS overlappingbenefit;

CREATE TABLE overlappingbenefit STORED AS ORC AS SELECT 
 
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
`_id`.`d_oid` as `id` FROM src_accepted_data_overlappingbenefit_overlappingbenefit
 src_accepted_data_overlappingbenefit_overlappingbenefit;


!echo ------------------------;
!echo ------------------------ gracePeriod;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_graceperiod_graceperiod
;

CREATE EXTERNAL TABLE src_accepted_data_graceperiod_graceperiod(
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
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/gracePeriod';

DROP TABLE IF EXISTS graceperiod;

CREATE TABLE graceperiod STORED AS ORC AS SELECT 
 
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
`_id`.`d_oid` as `id` FROM src_accepted_data_graceperiod_graceperiod
 src_accepted_data_graceperiod_graceperiod;


!echo ------------------------;
!echo ------------------------ capital;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_capital_capital
;

CREATE EXTERNAL TABLE src_accepted_data_capital_capital(
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
,`_id` STRUCT<`declarationId`:STRING>
,`declarationId` STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/capital';

DROP TABLE IF EXISTS capital;

CREATE TABLE capital STORED AS ORC AS SELECT 
 
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
`_id`.`declarationId` as `id` FROM src_accepted_data_capital_capital
 src_accepted_data_capital_capital;


!echo ------------------------;
!echo ------------------------ childrenCircumstances;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_childrencircumstances_childrencircumstances
;

CREATE EXTERNAL TABLE src_accepted_data_childrencircumstances_childrencircumstances(
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
,`_id` STRUCT<`declarationId`:STRING>
,`declarationId` STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/childrenCircumstances';

DROP TABLE IF EXISTS childrencircumstances;

CREATE TABLE childrencircumstances STORED AS ORC AS SELECT 
 
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
`_id`.`declarationId` as `id` FROM src_accepted_data_childrencircumstances_childrencircumstances
 src_accepted_data_childrencircumstances_childrencircumstances;


!echo ------------------------;
!echo ------------------------ housingEntitlement;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_housingentitlement_housingentitlement
;

CREATE EXTERNAL TABLE src_accepted_data_housingentitlement_housingentitlement(
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
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/housingEntitlement';

DROP TABLE IF EXISTS housingentitlement;

CREATE TABLE housingentitlement STORED AS ORC AS SELECT 
 
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
`_id`.`d_oid` as `id` FROM src_accepted_data_housingentitlement_housingentitlement
 src_accepted_data_housingentitlement_housingentitlement;


!echo ------------------------;
!echo ------------------------ healthAndDisabilityCircumstances;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_healthanddisabilitycircumstances_healthanddisabilitycircumstances
;

CREATE EXTERNAL TABLE src_accepted_data_healthanddisabilitycircumstances_healthanddisabilitycircumstances(
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
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/healthAndDisabilityCircumstances';

DROP TABLE IF EXISTS healthanddisabilitycircumstances;

CREATE TABLE healthanddisabilitycircumstances STORED AS ORC AS SELECT 
 
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
`_id`.`d_oid` as `id` FROM src_accepted_data_healthanddisabilitycircumstances_healthanddisabilitycircumstances
 src_accepted_data_healthanddisabilitycircumstances_healthanddisabilitycircumstances;


!echo ------------------------;
!echo ------------------------ employmentCircumstances;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_employmentcircumstances_employmentcircumstances
;

CREATE EXTERNAL TABLE src_accepted_data_employmentcircumstances_employmentcircumstances(
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
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/employmentCircumstances';

DROP TABLE IF EXISTS employmentcircumstances;

CREATE TABLE employmentcircumstances STORED AS ORC AS SELECT 
 
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
`_id`.`d_oid` as `id` FROM src_accepted_data_employmentcircumstances_employmentcircumstances
 src_accepted_data_employmentcircumstances_employmentcircumstances;


!echo ------------------------;
!echo ------------------------ guardiansAllowance;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_guardiansallowance_guardiansallowance
;

CREATE EXTERNAL TABLE src_accepted_data_guardiansallowance_guardiansallowance(
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
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/guardiansAllowance';

DROP TABLE IF EXISTS guardiansallowance;

CREATE TABLE guardiansallowance STORED AS ORC AS SELECT 
 
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
`_id`.`d_oid` as `id` FROM src_accepted_data_guardiansallowance_guardiansallowance
 src_accepted_data_guardiansallowance_guardiansallowance;


!echo ------------------------;
!echo ------------------------ healthCircumstances;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_healthcircumstances_healthcircumstances
;

CREATE EXTERNAL TABLE src_accepted_data_healthcircumstances_healthcircumstances(
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
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/healthCircumstances';

DROP TABLE IF EXISTS healthcircumstances;

CREATE TABLE healthcircumstances STORED AS ORC AS SELECT 
 
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
`_id`.`d_oid` as `id` FROM src_accepted_data_healthcircumstances_healthcircumstances
 src_accepted_data_healthcircumstances_healthcircumstances;


!echo ------------------------;
!echo ------------------------ personDetails;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_persondetails_persondetails
;

CREATE EXTERNAL TABLE src_accepted_data_persondetails_persondetails(
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
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/personDetails';

DROP TABLE IF EXISTS persondetails;

CREATE TABLE persondetails STORED AS ORC AS SELECT 
 
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
`_id`.`d_oid` as `id` FROM src_accepted_data_persondetails_persondetails
 src_accepted_data_persondetails_persondetails;


!echo ------------------------;
!echo ------------------------ bankDetails;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_bankdetails_bankdetails
;

CREATE EXTERNAL TABLE src_accepted_data_bankdetails_bankdetails(
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
,`_id` STRUCT<`declarationId`:STRING>
,`declarationId` STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/bankDetails';

DROP TABLE IF EXISTS bankdetails;

CREATE TABLE bankdetails STORED AS ORC AS SELECT 
 
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
`_id`.`declarationId` as `id` FROM src_accepted_data_bankdetails_bankdetails
 src_accepted_data_bankdetails_bankdetails;


!echo ------------------------;
!echo ------------------------ otherBenefit;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_otherbenefit_otherbenefit
;

CREATE EXTERNAL TABLE src_accepted_data_otherbenefit_otherbenefit(
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
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/otherBenefit';

DROP TABLE IF EXISTS otherbenefit;

CREATE TABLE otherbenefit STORED AS ORC AS SELECT 
 
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
`_id`.`d_oid` as `id` FROM src_accepted_data_otherbenefit_otherbenefit
 src_accepted_data_otherbenefit_otherbenefit;


!echo ------------------------;
!echo ------------------------ apa;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_apa_apa
;

CREATE EXTERNAL TABLE src_accepted_data_apa_apa(
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
,`_id` STRUCT<`declarationId`:STRING>
,`declarationId` STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/apa';

DROP TABLE IF EXISTS apa;

CREATE TABLE apa STORED AS ORC AS SELECT 
 
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
`_id`.`declarationId` as `id` FROM src_accepted_data_apa_apa
 src_accepted_data_apa_apa;


!echo ------------------------;
!echo ------------------------ calculateDeductions;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_calculatedeductions_calculatedeductions
;

CREATE EXTERNAL TABLE src_accepted_data_calculatedeductions_calculatedeductions(
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
,`_id` STRUCT<`assessmentPeriodId`:STRING>
,`declarationId` STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/calculateDeductions';

DROP TABLE IF EXISTS calculatedeductions;

CREATE TABLE calculatedeductions STORED AS ORC AS SELECT 
 
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
`_id`.`assessmentPeriodId` as `id` FROM src_accepted_data_calculatedeductions_calculatedeductions
 src_accepted_data_calculatedeductions_calculatedeductions;


!echo ------------------------;
!echo ------------------------ address;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_address_address
;

CREATE EXTERNAL TABLE src_accepted_data_address_address(
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
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/address';

DROP TABLE IF EXISTS address;

CREATE TABLE address STORED AS ORC AS SELECT 
 
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
`_id`.`d_oid` as `id` FROM src_accepted_data_address_address
 src_accepted_data_address_address;


!echo ------------------------;
!echo ------------------------ ineligiblePartner;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_ineligiblepartner_ineligiblepartner
;

CREATE EXTERNAL TABLE src_accepted_data_ineligiblepartner_ineligiblepartner(
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
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/ineligiblePartner';

DROP TABLE IF EXISTS ineligiblepartner;

CREATE TABLE ineligiblepartner STORED AS ORC AS SELECT 
 
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
`_id`.`d_oid` as `id` FROM src_accepted_data_ineligiblepartner_ineligiblepartner
 src_accepted_data_ineligiblepartner_ineligiblepartner;


!echo ------------------------;
!echo ------------------------ pregnancy;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_pregnancy_pregnancy
;

CREATE EXTERNAL TABLE src_accepted_data_pregnancy_pregnancy(
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
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/pregnancy';

DROP TABLE IF EXISTS pregnancy;

CREATE TABLE pregnancy STORED AS ORC AS SELECT 
 
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
`_id`.`d_oid` as `id` FROM src_accepted_data_pregnancy_pregnancy
 src_accepted_data_pregnancy_pregnancy;


!echo ------------------------;
!echo ------------------------ workAndEarningsCircumstances;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_workandearningscircumstances_workandearningscircumstances
;

CREATE EXTERNAL TABLE src_accepted_data_workandearningscircumstances_workandearningscircumstances(
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
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/workAndEarningsCircumstances';

DROP TABLE IF EXISTS workandearningscircumstances;

CREATE TABLE workandearningscircumstances STORED AS ORC AS SELECT 
 
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
`_id`.`d_oid` as `id` FROM src_accepted_data_workandearningscircumstances_workandearningscircumstances
 src_accepted_data_workandearningscircumstances_workandearningscircumstances;


!echo ------------------------;
!echo ------------------------ carersAllowance;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_carersallowance_carersallowance
;

CREATE EXTERNAL TABLE src_accepted_data_carersallowance_carersallowance(
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
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/carersAllowance';

DROP TABLE IF EXISTS carersallowance;

CREATE TABLE carersallowance STORED AS ORC AS SELECT 
 
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
`_id`.`d_oid` as `id` FROM src_accepted_data_carersallowance_carersallowance
 src_accepted_data_carersallowance_carersallowance;


!echo ------------------------;
!echo ------------------------ supportForMortgageInterest;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_supportformortgageinterest_supportformortgageinterest
;

CREATE EXTERNAL TABLE src_accepted_data_supportformortgageinterest_supportformortgageinterest(
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
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/supportForMortgageInterest';

DROP TABLE IF EXISTS supportformortgageinterest;

CREATE TABLE supportformortgageinterest STORED AS ORC AS SELECT 
 
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
`_id`.`d_oid` as `id` FROM src_accepted_data_supportformortgageinterest_supportformortgageinterest
 src_accepted_data_supportformortgageinterest_supportformortgageinterest;


!echo ------------------------;
!echo ------------------------ terminalIllness;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_terminalillness_terminalillness
;

CREATE EXTERNAL TABLE src_accepted_data_terminalillness_terminalillness(
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
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/terminalIllness';

DROP TABLE IF EXISTS terminalillness;

CREATE TABLE terminalillness STORED AS ORC AS SELECT 
 
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
`_id`.`d_oid` as `id` FROM src_accepted_data_terminalillness_terminalillness
 src_accepted_data_terminalillness_terminalillness;


!echo ------------------------;
!echo ------------------------ childElementEligibility;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_childelementeligibility_childelementeligibility
;

CREATE EXTERNAL TABLE src_accepted_data_childelementeligibility_childelementeligibility(
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
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/childElementEligibility';

DROP TABLE IF EXISTS childelementeligibility;

CREATE TABLE childelementeligibility STORED AS ORC AS SELECT 
 
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
`_id`.`d_oid` as `id` FROM src_accepted_data_childelementeligibility_childelementeligibility
 src_accepted_data_childelementeligibility_childelementeligibility;


!echo ------------------------;
!echo ------------------------ otherIncome;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_otherincome_otherincome
;

CREATE EXTERNAL TABLE src_accepted_data_otherincome_otherincome(
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
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/otherIncome';

DROP TABLE IF EXISTS otherincome;

CREATE TABLE otherincome STORED AS ORC AS SELECT 
 
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
`_id`.`d_oid` as `id` FROM src_accepted_data_otherincome_otherincome
 src_accepted_data_otherincome_otherincome;


!echo ------------------------;
!echo ------------------------ nationality;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_nationality_nationality
;

CREATE EXTERNAL TABLE src_accepted_data_nationality_nationality(
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
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/nationality';

DROP TABLE IF EXISTS nationality;

CREATE TABLE nationality STORED AS ORC AS SELECT 
 
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
`_id`.`d_oid` as `id` FROM src_accepted_data_nationality_nationality
 src_accepted_data_nationality_nationality;


!echo ------------------------;
!echo ------------------------ carerCircumstances;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_carercircumstances_carercircumstances
;

CREATE EXTERNAL TABLE src_accepted_data_carercircumstances_carercircumstances(
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
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/carerCircumstances';

DROP TABLE IF EXISTS carercircumstances;

CREATE TABLE carercircumstances STORED AS ORC AS SELECT 
 
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
`_id`.`d_oid` as `id` FROM src_accepted_data_carercircumstances_carercircumstances
 src_accepted_data_carercircumstances_carercircumstances;


!echo ------------------------;
!echo ------------------------ previousEarnings;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_previousearnings_previousearnings
;

CREATE EXTERNAL TABLE src_accepted_data_previousearnings_previousearnings(
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
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/previousEarnings';

DROP TABLE IF EXISTS previousearnings;

CREATE TABLE previousearnings STORED AS ORC AS SELECT 
 
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
`_id`.`d_oid` as `id` FROM src_accepted_data_previousearnings_previousearnings
 src_accepted_data_previousearnings_previousearnings;


!echo ------------------------;
!echo ------------------------ childcare;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_childcare_childcare
;

CREATE EXTERNAL TABLE src_accepted_data_childcare_childcare(
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
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/childcare';

DROP TABLE IF EXISTS childcare;

CREATE TABLE childcare STORED AS ORC AS SELECT 
 
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
`_id`.`d_oid` as `id` FROM src_accepted_data_childcare_childcare
 src_accepted_data_childcare_childcare;


!echo ------------------------;
!echo ------------------------ educationCircumstances;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_educationcircumstances_educationcircumstances
;

CREATE EXTERNAL TABLE src_accepted_data_educationcircumstances_educationcircumstances(
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
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/educationCircumstances';

DROP TABLE IF EXISTS educationcircumstances;

CREATE TABLE educationcircumstances STORED AS ORC AS SELECT 
 
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
`_id`.`d_oid` as `id` FROM src_accepted_data_educationcircumstances_educationcircumstances
 src_accepted_data_educationcircumstances_educationcircumstances;


!echo ------------------------;
!echo ------------------------ partnerQuestion;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_partnerquestion_partnerquestion
;

CREATE EXTERNAL TABLE src_accepted_data_partnerquestion_partnerquestion(
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
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/partnerQuestion';

DROP TABLE IF EXISTS partnerquestion;

CREATE TABLE partnerquestion STORED AS ORC AS SELECT 
 
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
`_id`.`d_oid` as `id` FROM src_accepted_data_partnerquestion_partnerquestion
 src_accepted_data_partnerquestion_partnerquestion;


!echo ------------------------;
!echo ------------------------ housingCircumstances;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_housingcircumstances_housingcircumstances
;

CREATE EXTERNAL TABLE src_accepted_data_housingcircumstances_housingcircumstances(
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
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/housingCircumstances';

DROP TABLE IF EXISTS housingcircumstances;

CREATE TABLE housingcircumstances STORED AS ORC AS SELECT 
 
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
`_id`.`d_oid` as `id` FROM src_accepted_data_housingcircumstances_housingcircumstances
 src_accepted_data_housingcircumstances_housingcircumstances;


!echo ------------------------;
!echo ------------------------ workCapabilityAssessmentDecision;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_workcapabilityassessmentdecision_workcapabilityassessmentdecision
;

CREATE EXTERNAL TABLE src_accepted_data_workcapabilityassessmentdecision_workcapabilityassessmentdecision(
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
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/workCapabilityAssessmentDecision';

DROP TABLE IF EXISTS workcapabilityassessmentdecision;

CREATE TABLE workcapabilityassessmentdecision STORED AS ORC AS SELECT 
 
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
`_id`.`d_oid` as `id` FROM src_accepted_data_workcapabilityassessmentdecision_workcapabilityassessmentdecision
 src_accepted_data_workcapabilityassessmentdecision_workcapabilityassessmentdecision;


