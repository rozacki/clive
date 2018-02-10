use change_me;
!echo ------------------------;
!echo ------------------------ overlappingBenefit;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_overlappingbenefit_overlappingbenefit
;

CREATE EXTERNAL TABLE src_accepted_data_overlappingbenefit_overlappingbenefit(
`claimElement` STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`benefitName`:STRING
,`claimantId`:STRING
,`endDate`:STRING
,`overlappingBenefitAmount`:STRING
,`startDate`:STRING
,`type`:STRING>
,`invalidated` STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`acceptedDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`d_oid`:STRING>
,`_version` STRING
,`declarationId` STRING
,`_removed` STRUCT<`claimElement`:STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`benefitName`:STRING
,`claimantId`:STRING
,`endDate`:STRING
,`overlappingBenefitAmount`:STRING
,`startDate`:STRING
,`type`:STRING>
,`invalidated`:STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`acceptedDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`d_oid`:STRING>
,`_version`:STRING
,`declarationId`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/overlappingBenefit';

DROP TABLE IF EXISTS overlappingBenefit;

CREATE TABLE overlappingbenefit STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`acceptedDateTime`.`d_date`, LENGTH(`_removed`.`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`acceptedDateTime`.`d_date`, LENGTH(`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as acceptedDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`contractId`, `claimElement`.`contractId`) as claimElement_contractId, 
COALESCE(`_removed`.`claimElement`.`declaredDateTime`, `claimElement`.`declaredDateTime`) as claimElement_declaredDateTime, 
COALESCE(CAST(`_removed`.`claimElement`.`effectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`effectiveDate`.`hasDate` as STRING)) as claimElement_effectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`knownDate`, `claimElement`.`effectiveDate`.`knownDate`) as claimElement_effectiveDate_knownDate, 
COALESCE(CAST(`_removed`.`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING)) as claimElement_paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`knownDate`, `claimElement`.`paymentEffectiveDate`.`knownDate`) as claimElement_paymentEffectiveDate_knownDate, 
COALESCE(`_removed`.`claimElement`.`benefitName`, `claimElement`.`benefitName`) as claimElement_benefitName, 
COALESCE(`_removed`.`claimElement`.`claimantId`, `claimElement`.`claimantId`) as claimElement_claimantId, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`type`, `claimElement`.`effectiveDate`.`type`) as claimElement_effectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`date`, `claimElement`.`effectiveDate`.`date`) as claimElement_effectiveDate_date, 
COALESCE(`_removed`.`claimElement`.`endDate`, `claimElement`.`endDate`) as claimElement_endDate, 
COALESCE(`_removed`.`claimElement`.`overlappingBenefitAmount`, `claimElement`.`overlappingBenefitAmount`) as claimElement_overlappingBenefitAmount, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`type`, `claimElement`.`paymentEffectiveDate`.`type`) as claimElement_paymentEffectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`date`, `claimElement`.`paymentEffectiveDate`.`date`) as claimElement_paymentEffectiveDate_date, 
COALESCE(`_removed`.`claimElement`.`startDate`, `claimElement`.`startDate`) as claimElement_startDate, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`_removed`.`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as claimElement_declaredDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`type`, `claimElement`.`type`) as claimElement_type, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`invalidated`.`dateTime`.`d_date`, LENGTH(`_removed`.`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as invalidated_dateTime_d_date, 
COALESCE(`_removed`.`invalidated`.`reason`, `invalidated`.`reason`) as invalidated_reason, 
COALESCE(`_removed`.`_id`.`d_oid`, `_id`.`d_oid`) as id FROM src_accepted_data_overlappingbenefit_overlappingbenefit
 src_accepted_data_overlappingbenefit_overlappingbenefit;


!echo ------------------------;
!echo ------------------------ gracePeriod;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_graceperiod_graceperiod
;

CREATE EXTERNAL TABLE src_accepted_data_graceperiod_graceperiod(
`claimElement` STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`claimantId`:STRING
,`endDate`:STRING
,`type`:STRING>
,`invalidated` STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`acceptedDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`d_oid`:STRING>
,`_version` STRING
,`declarationId` STRING
,`_removed` STRUCT<`claimElement`:STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`claimantId`:STRING
,`endDate`:STRING
,`type`:STRING>
,`invalidated`:STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`acceptedDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`d_oid`:STRING>
,`_version`:STRING
,`declarationId`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/gracePeriod';

DROP TABLE IF EXISTS gracePeriod;

CREATE TABLE graceperiod STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`acceptedDateTime`.`d_date`, LENGTH(`_removed`.`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`acceptedDateTime`.`d_date`, LENGTH(`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as acceptedDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`contractId`, `claimElement`.`contractId`) as claimElement_contractId, 
COALESCE(`_removed`.`claimElement`.`declaredDateTime`, `claimElement`.`declaredDateTime`) as claimElement_declaredDateTime, 
COALESCE(CAST(`_removed`.`claimElement`.`effectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`effectiveDate`.`hasDate` as STRING)) as claimElement_effectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`knownDate`, `claimElement`.`effectiveDate`.`knownDate`) as claimElement_effectiveDate_knownDate, 
COALESCE(CAST(`_removed`.`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING)) as claimElement_paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`knownDate`, `claimElement`.`paymentEffectiveDate`.`knownDate`) as claimElement_paymentEffectiveDate_knownDate, 
COALESCE(`_removed`.`claimElement`.`claimantId`, `claimElement`.`claimantId`) as claimElement_claimantId, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`type`, `claimElement`.`effectiveDate`.`type`) as claimElement_effectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`date`, `claimElement`.`effectiveDate`.`date`) as claimElement_effectiveDate_date, 
COALESCE(`_removed`.`claimElement`.`endDate`, `claimElement`.`endDate`) as claimElement_endDate, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`type`, `claimElement`.`paymentEffectiveDate`.`type`) as claimElement_paymentEffectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`date`, `claimElement`.`paymentEffectiveDate`.`date`) as claimElement_paymentEffectiveDate_date, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`_removed`.`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as claimElement_declaredDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`type`, `claimElement`.`type`) as claimElement_type, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`invalidated`.`dateTime`.`d_date`, LENGTH(`_removed`.`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as invalidated_dateTime_d_date, 
COALESCE(`_removed`.`invalidated`.`reason`, `invalidated`.`reason`) as invalidated_reason, 
COALESCE(`_removed`.`_id`.`d_oid`, `_id`.`d_oid`) as id FROM src_accepted_data_graceperiod_graceperiod
 src_accepted_data_graceperiod_graceperiod;


!echo ------------------------;
!echo ------------------------ capital;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_capital_capital
;

CREATE EXTERNAL TABLE src_accepted_data_capital_capital(
`claimElement` STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`capitalAmount`:STRING
,`type`:STRING>
,`invalidated` STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`acceptedDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`declarationId`:STRING>
,`_version` STRING
,`declarationId` STRING
,`_removed` STRUCT<`claimElement`:STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`capitalAmount`:STRING
,`type`:STRING>
,`invalidated`:STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`acceptedDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`declarationId`:STRING>
,`_version`:STRING
,`declarationId`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/capital';

DROP TABLE IF EXISTS capital;

CREATE TABLE capital STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`acceptedDateTime`.`d_date`, LENGTH(`_removed`.`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`acceptedDateTime`.`d_date`, LENGTH(`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as acceptedDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`contractId`, `claimElement`.`contractId`) as claimElement_contractId, 
COALESCE(`_removed`.`claimElement`.`declaredDateTime`, `claimElement`.`declaredDateTime`) as claimElement_declaredDateTime, 
COALESCE(CAST(`_removed`.`claimElement`.`effectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`effectiveDate`.`hasDate` as STRING)) as claimElement_effectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`knownDate`, `claimElement`.`effectiveDate`.`knownDate`) as claimElement_effectiveDate_knownDate, 
COALESCE(CAST(`_removed`.`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING)) as claimElement_paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`knownDate`, `claimElement`.`paymentEffectiveDate`.`knownDate`) as claimElement_paymentEffectiveDate_knownDate, 
COALESCE(`_removed`.`claimElement`.`capitalAmount`, `claimElement`.`capitalAmount`) as claimElement_capitalAmount, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`type`, `claimElement`.`effectiveDate`.`type`) as claimElement_effectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`date`, `claimElement`.`effectiveDate`.`date`) as claimElement_effectiveDate_date, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`type`, `claimElement`.`paymentEffectiveDate`.`type`) as claimElement_paymentEffectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`date`, `claimElement`.`paymentEffectiveDate`.`date`) as claimElement_paymentEffectiveDate_date, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`_removed`.`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as claimElement_declaredDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`type`, `claimElement`.`type`) as claimElement_type, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`invalidated`.`dateTime`.`d_date`, LENGTH(`_removed`.`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as invalidated_dateTime_d_date, 
COALESCE(`_removed`.`invalidated`.`reason`, `invalidated`.`reason`) as invalidated_reason, 
COALESCE(`_removed`.`_id`.`declarationId`, `_id`.`declarationId`) as id FROM src_accepted_data_capital_capital
 src_accepted_data_capital_capital;


!echo ------------------------;
!echo ------------------------ childrenCircumstances;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_childrencircumstances_childrencircumstances
;

CREATE EXTERNAL TABLE src_accepted_data_childrencircumstances_childrencircumstances(
`claimElement` STRUCT<`children`:ARRAY<STRUCT<`disabilityDetails`:STRUCT<`dlaCareComponent`:STRING
,`pipDailyLivingComponent`:STRING
,`receivingDisabilityLivingAllowance`:BOOLEAN
,`receivingPersonalIndependencePayment`:BOOLEAN
,`registeredBlind`:BOOLEAN>
,`nonDependantDetails`:STRUCT<`awayOnArmedForcesOperations`:BOOLEAN
,`personIsClaimantsChild`:BOOLEAN
,`primaryCarer`:STRING
,`receivingArmedForcesIndependencePayment`:BOOLEAN
,`receivingAttendanceAllowance`:BOOLEAN
,`receivingCarersAllowance`:BOOLEAN
,`receivingDisabilityLivingAllowance`:BOOLEAN
,`receivingPersonalIndependencePayment`:BOOLEAN
,`receivingStatePensionCredit`:BOOLEAN>
,`temporaryAbsence`:STRUCT<`abroadForMoreThanOneMonth`:BOOLEAN
,`inPrison`:BOOLEAN
,`inTheCareOfLocalAuthority`:BOOLEAN>
,`adoptedOrUnderGuardianship`:BOOLEAN
,`dateOfBirth`:STRING
,`declarationDate`:STRING
,`fullName`:STRING
,`gender`:STRING
,`id`:STRING
,`inQualifyingFullTimeEducation`:BOOLEAN
,`removalDate`:STRING
>>
,`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`hasExceptionToTwoChildPolicy`:BOOLEAN
,`primaryCarerId`:STRING
,`type`:STRING>
,`invalidated` STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`acceptedDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`declarationId`:STRING>
,`_version` STRING
,`declarationId` STRING
,`_removed` STRUCT<`claimElement`:STRUCT<`children`:ARRAY<STRUCT<`disabilityDetails`:STRUCT<`dlaCareComponent`:STRING
,`pipDailyLivingComponent`:STRING
,`receivingDisabilityLivingAllowance`:BOOLEAN
,`receivingPersonalIndependencePayment`:BOOLEAN
,`registeredBlind`:BOOLEAN>
,`nonDependantDetails`:STRUCT<`awayOnArmedForcesOperations`:BOOLEAN
,`personIsClaimantsChild`:BOOLEAN
,`primaryCarer`:STRING
,`receivingArmedForcesIndependencePayment`:BOOLEAN
,`receivingAttendanceAllowance`:BOOLEAN
,`receivingCarersAllowance`:BOOLEAN
,`receivingDisabilityLivingAllowance`:BOOLEAN
,`receivingPersonalIndependencePayment`:BOOLEAN
,`receivingStatePensionCredit`:BOOLEAN>
,`temporaryAbsence`:STRUCT<`abroadForMoreThanOneMonth`:BOOLEAN
,`inPrison`:BOOLEAN
,`inTheCareOfLocalAuthority`:BOOLEAN>
,`adoptedOrUnderGuardianship`:BOOLEAN
,`dateOfBirth`:STRING
,`declarationDate`:STRING
,`fullName`:STRING
,`gender`:STRING
,`id`:STRING
,`inQualifyingFullTimeEducation`:BOOLEAN
,`removalDate`:STRING
>>
,`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`hasExceptionToTwoChildPolicy`:BOOLEAN
,`primaryCarerId`:STRING
,`type`:STRING>
,`invalidated`:STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`acceptedDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`declarationId`:STRING>
,`_version`:STRING
,`declarationId`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/childrenCircumstances';

DROP TABLE IF EXISTS childrenCircumstances;

CREATE TABLE childrencircumstances STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`acceptedDateTime`.`d_date`, LENGTH(`_removed`.`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`acceptedDateTime`.`d_date`, LENGTH(`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as acceptedDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`contractId`, `claimElement`.`contractId`) as claimElement_contractId, 
COALESCE(`_removed`.`claimElement`.`declaredDateTime`, `claimElement`.`declaredDateTime`) as claimElement_declaredDateTime, 
COALESCE(CAST(`_removed`.`claimElement`.`effectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`effectiveDate`.`hasDate` as STRING)) as claimElement_effectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`knownDate`, `claimElement`.`effectiveDate`.`knownDate`) as claimElement_effectiveDate_knownDate, 
COALESCE(CAST(`_removed`.`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING)) as claimElement_paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`knownDate`, `claimElement`.`paymentEffectiveDate`.`knownDate`) as claimElement_paymentEffectiveDate_knownDate, 
COALESCE(CAST(`removed_exploded_claimelement_children`.`adoptedOrUnderGuardianship` as STRING), CAST(`exploded_claimelement_children`.`adoptedOrUnderGuardianship` as STRING)) as claimElement_children_adoptedOrUnderGuardianship, 
COALESCE(`removed_exploded_claimelement_children`.`dateOfBirth`, `exploded_claimelement_children`.`dateOfBirth`) as claimElement_children_dateOfBirth, 
COALESCE(`removed_exploded_claimelement_children`.`declarationDate`, `exploded_claimelement_children`.`declarationDate`) as claimElement_children_declarationDate, 
COALESCE(`removed_exploded_claimelement_children`.`disabilityDetails`.`dlaCareComponent`, `exploded_claimelement_children`.`disabilityDetails`.`dlaCareComponent`) as claimElement_children_disabilityDetails_dlaCareComponent, 
COALESCE(`removed_exploded_claimelement_children`.`disabilityDetails`.`pipDailyLivingComponent`, `exploded_claimelement_children`.`disabilityDetails`.`pipDailyLivingComponent`) as claimElement_children_disabilityDetails_pipDailyLivingComponent, 
COALESCE(CAST(`removed_exploded_claimelement_children`.`disabilityDetails`.`receivingDisabilityLivingAllowance` as STRING), CAST(`exploded_claimelement_children`.`disabilityDetails`.`receivingDisabilityLivingAllowance` as STRING)) as claimElement_children_disabilityDetails_receivingDisabilityLivingAllowance, 
COALESCE(CAST(`removed_exploded_claimelement_children`.`disabilityDetails`.`receivingPersonalIndependencePayment` as STRING), CAST(`exploded_claimelement_children`.`disabilityDetails`.`receivingPersonalIndependencePayment` as STRING)) as claimElement_children_disabilityDetails_receivingPersonalIndependencePayment, 
COALESCE(CAST(`removed_exploded_claimelement_children`.`disabilityDetails`.`registeredBlind` as STRING), CAST(`exploded_claimelement_children`.`disabilityDetails`.`registeredBlind` as STRING)) as claimElement_children_disabilityDetails_registeredBlind, 
COALESCE(`removed_exploded_claimelement_children`.`fullName`, `exploded_claimelement_children`.`fullName`) as claimElement_children_fullName, 
COALESCE(`removed_exploded_claimelement_children`.`gender`, `exploded_claimelement_children`.`gender`) as claimElement_children_gender, 
COALESCE(`removed_exploded_claimelement_children`.`id`, `exploded_claimelement_children`.`id`) as claimElement_children_id, 
COALESCE(CAST(`removed_exploded_claimelement_children`.`inQualifyingFullTimeEducation` as STRING), CAST(`exploded_claimelement_children`.`inQualifyingFullTimeEducation` as STRING)) as claimElement_children_inQualifyingFullTimeEducation, 
COALESCE(CAST(`removed_exploded_claimelement_children`.`nonDependantDetails`.`awayOnArmedForcesOperations` as STRING), CAST(`exploded_claimelement_children`.`nonDependantDetails`.`awayOnArmedForcesOperations` as STRING)) as claimElement_children_nonDependantDetails_awayOnArmedForcesOperations, 
COALESCE(CAST(`removed_exploded_claimelement_children`.`nonDependantDetails`.`personIsClaimantsChild` as STRING), CAST(`exploded_claimelement_children`.`nonDependantDetails`.`personIsClaimantsChild` as STRING)) as claimElement_children_nonDependantDetails_personIsClaimantsChild, 
COALESCE(`removed_exploded_claimelement_children`.`nonDependantDetails`.`primaryCarer`, `exploded_claimelement_children`.`nonDependantDetails`.`primaryCarer`) as claimElement_children_nonDependantDetails_primaryCarer, 
COALESCE(CAST(`removed_exploded_claimelement_children`.`nonDependantDetails`.`receivingArmedForcesIndependencePayment` as STRING), CAST(`exploded_claimelement_children`.`nonDependantDetails`.`receivingArmedForcesIndependencePayment` as STRING)) as claimElement_children_nonDependantDetails_receivingArmedForcesIndependencePayment, 
COALESCE(CAST(`removed_exploded_claimelement_children`.`nonDependantDetails`.`receivingAttendanceAllowance` as STRING), CAST(`exploded_claimelement_children`.`nonDependantDetails`.`receivingAttendanceAllowance` as STRING)) as claimElement_children_nonDependantDetails_receivingAttendanceAllowance, 
COALESCE(CAST(`removed_exploded_claimelement_children`.`nonDependantDetails`.`receivingCarersAllowance` as STRING), CAST(`exploded_claimelement_children`.`nonDependantDetails`.`receivingCarersAllowance` as STRING)) as claimElement_children_nonDependantDetails_receivingCarersAllowance, 
COALESCE(CAST(`removed_exploded_claimelement_children`.`nonDependantDetails`.`receivingDisabilityLivingAllowance` as STRING), CAST(`exploded_claimelement_children`.`nonDependantDetails`.`receivingDisabilityLivingAllowance` as STRING)) as claimElement_children_nonDependantDetails_receivingDisabilityLivingAllowance, 
COALESCE(CAST(`removed_exploded_claimelement_children`.`nonDependantDetails`.`receivingPersonalIndependencePayment` as STRING), CAST(`exploded_claimelement_children`.`nonDependantDetails`.`receivingPersonalIndependencePayment` as STRING)) as claimElement_children_nonDependantDetails_receivingPersonalIndependencePayment, 
COALESCE(CAST(`removed_exploded_claimelement_children`.`nonDependantDetails`.`receivingStatePensionCredit` as STRING), CAST(`exploded_claimelement_children`.`nonDependantDetails`.`receivingStatePensionCredit` as STRING)) as claimElement_children_nonDependantDetails_receivingStatePensionCredit, 
COALESCE(`removed_exploded_claimelement_children`.`removalDate`, `exploded_claimelement_children`.`removalDate`) as claimElement_children_removalDate, 
COALESCE(CAST(`removed_exploded_claimelement_children`.`temporaryAbsence`.`abroadForMoreThanOneMonth` as STRING), CAST(`exploded_claimelement_children`.`temporaryAbsence`.`abroadForMoreThanOneMonth` as STRING)) as claimElement_children_temporaryAbsence_abroadForMoreThanOneMonth, 
COALESCE(CAST(`removed_exploded_claimelement_children`.`temporaryAbsence`.`inPrison` as STRING), CAST(`exploded_claimelement_children`.`temporaryAbsence`.`inPrison` as STRING)) as claimElement_children_temporaryAbsence_inPrison, 
COALESCE(CAST(`removed_exploded_claimelement_children`.`temporaryAbsence`.`inTheCareOfLocalAuthority` as STRING), CAST(`exploded_claimelement_children`.`temporaryAbsence`.`inTheCareOfLocalAuthority` as STRING)) as claimElement_children_temporaryAbsence_inTheCareOfLocalAuthority, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`_removed`.`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as claimElement_declaredDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`type`, `claimElement`.`effectiveDate`.`type`) as claimElement_effectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`date`, `claimElement`.`effectiveDate`.`date`) as claimElement_effectiveDate_date, 
COALESCE(CAST(`_removed`.`claimElement`.`hasExceptionToTwoChildPolicy` as STRING), CAST(`claimElement`.`hasExceptionToTwoChildPolicy` as STRING)) as claimElement_hasExceptionToTwoChildPolicy, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`type`, `claimElement`.`paymentEffectiveDate`.`type`) as claimElement_paymentEffectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`date`, `claimElement`.`paymentEffectiveDate`.`date`) as claimElement_paymentEffectiveDate_date, 
COALESCE(`_removed`.`claimElement`.`primaryCarerId`, `claimElement`.`primaryCarerId`) as claimElement_primaryCarerId, 
COALESCE(`_removed`.`claimElement`.`type`, `claimElement`.`type`) as claimElement_type, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`invalidated`.`dateTime`.`d_date`, LENGTH(`_removed`.`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as invalidated_dateTime_d_date, 
COALESCE(`_removed`.`invalidated`.`reason`, `invalidated`.`reason`) as invalidated_reason, 
COALESCE(`_removed`.`_id`.`declarationId`, `_id`.`declarationId`) as id FROM src_accepted_data_childrencircumstances_childrencircumstances
 LATERAL VIEW OUTER EXPLODE(`claimElement`.`children`) view_exploded_claimelement_children AS exploded_claimelement_children 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`claimElement`.`children`) view_removed_exploded_claimelement_children AS removed_exploded_claimelement_children 
;


!echo ------------------------;
!echo ------------------------ housingEntitlement;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_housingentitlement_housingentitlement
;

CREATE EXTERNAL TABLE src_accepted_data_housingentitlement_housingentitlement(
`claimElement` STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`entitled`:BOOLEAN
,`type`:STRING>
,`invalidated` STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`acceptedDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`d_oid`:STRING>
,`_version` STRING
,`declarationId` STRING
,`_removed` STRUCT<`claimElement`:STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`entitled`:BOOLEAN
,`type`:STRING>
,`invalidated`:STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`acceptedDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`d_oid`:STRING>
,`_version`:STRING
,`declarationId`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/housingEntitlement';

DROP TABLE IF EXISTS housingEntitlement;

CREATE TABLE housingentitlement STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`acceptedDateTime`.`d_date`, LENGTH(`_removed`.`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`acceptedDateTime`.`d_date`, LENGTH(`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as acceptedDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`contractId`, `claimElement`.`contractId`) as claimElement_contractId, 
COALESCE(`_removed`.`claimElement`.`declaredDateTime`, `claimElement`.`declaredDateTime`) as claimElement_declaredDateTime, 
COALESCE(CAST(`_removed`.`claimElement`.`effectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`effectiveDate`.`hasDate` as STRING)) as claimElement_effectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`knownDate`, `claimElement`.`effectiveDate`.`knownDate`) as claimElement_effectiveDate_knownDate, 
COALESCE(CAST(`_removed`.`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING)) as claimElement_paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`knownDate`, `claimElement`.`paymentEffectiveDate`.`knownDate`) as claimElement_paymentEffectiveDate_knownDate, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`_removed`.`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as claimElement_declaredDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`type`, `claimElement`.`effectiveDate`.`type`) as claimElement_effectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`date`, `claimElement`.`effectiveDate`.`date`) as claimElement_effectiveDate_date, 
COALESCE(CAST(`_removed`.`claimElement`.`entitled` as STRING), CAST(`claimElement`.`entitled` as STRING)) as claimElement_entitled, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`type`, `claimElement`.`paymentEffectiveDate`.`type`) as claimElement_paymentEffectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`date`, `claimElement`.`paymentEffectiveDate`.`date`) as claimElement_paymentEffectiveDate_date, 
COALESCE(`_removed`.`claimElement`.`type`, `claimElement`.`type`) as claimElement_type, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`invalidated`.`dateTime`.`d_date`, LENGTH(`_removed`.`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as invalidated_dateTime_d_date, 
COALESCE(`_removed`.`invalidated`.`reason`, `invalidated`.`reason`) as invalidated_reason, 
COALESCE(`_removed`.`_id`.`d_oid`, `_id`.`d_oid`) as id FROM src_accepted_data_housingentitlement_housingentitlement
 src_accepted_data_housingentitlement_housingentitlement;


!echo ------------------------;
!echo ------------------------ healthAndDisabilityCircumstances;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_healthanddisabilitycircumstances_healthanddisabilitycircumstances
;

CREATE EXTERNAL TABLE src_accepted_data_healthanddisabilitycircumstances_healthanddisabilitycircumstances(
`claimElement` STRUCT<`medicalTreatments`:ARRAY<STRUCT<`startDate`:STRING
,`treatment`:STRING
>>
,`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`abilityToWork`:STRUCT<`hasAFitNote`:BOOLEAN
,`restrictedAbilityToWork`:BOOLEAN>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`hospitalStays`:STRUCT<`admissionDate`:STRING
,`dischargeDate`:STRING
,`hospitalStay`:BOOLEAN>
,`pregnancy`:STRUCT<`dueDate`:STRING
,`illnessPutsPregnancyAtRisk`:STRING>
,`supportAtWork`:STRUCT<`hasGuideDog`:BOOLEAN
,`hasWheelchair`:BOOLEAN
,`needsHearingLoop`:BOOLEAN
,`otherSupport`:STRING>
,`terminalIllness`:STRUCT<`prognosis`:STRING
,`terminallyIll`:BOOLEAN>
,`healthConditions`:ARRAY<STRING>
,`contractId`:STRING
,`agentToCallTerminallyIllClaimant`:BOOLEAN
,`claimantId`:STRING
,`copiedFromDeclarationId`:STRING
,`declarationId`:STRING
,`periodOfSicknessId`:STRING
,`processId`:STRING
,`reportingIllness`:BOOLEAN
,`type`:STRING>
,`invalidated` STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`acceptedDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`d_oid`:STRING>
,`_version` STRING
,`declarationId` STRING
,`_removed` STRUCT<`claimElement`:STRUCT<`medicalTreatments`:ARRAY<STRUCT<`startDate`:STRING
,`treatment`:STRING
>>
,`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`abilityToWork`:STRUCT<`hasAFitNote`:BOOLEAN
,`restrictedAbilityToWork`:BOOLEAN>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`hospitalStays`:STRUCT<`admissionDate`:STRING
,`dischargeDate`:STRING
,`hospitalStay`:BOOLEAN>
,`pregnancy`:STRUCT<`dueDate`:STRING
,`illnessPutsPregnancyAtRisk`:STRING>
,`supportAtWork`:STRUCT<`hasGuideDog`:BOOLEAN
,`hasWheelchair`:BOOLEAN
,`needsHearingLoop`:BOOLEAN
,`otherSupport`:STRING>
,`terminalIllness`:STRUCT<`prognosis`:STRING
,`terminallyIll`:BOOLEAN>
,`healthConditions`:ARRAY<STRING>
,`contractId`:STRING
,`agentToCallTerminallyIllClaimant`:BOOLEAN
,`claimantId`:STRING
,`copiedFromDeclarationId`:STRING
,`declarationId`:STRING
,`periodOfSicknessId`:STRING
,`processId`:STRING
,`reportingIllness`:BOOLEAN
,`type`:STRING>
,`invalidated`:STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`acceptedDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`d_oid`:STRING>
,`_version`:STRING
,`declarationId`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/healthAndDisabilityCircumstances';

DROP TABLE IF EXISTS healthAndDisabilityCircumstances;

CREATE TABLE healthanddisabilitycircumstances STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`acceptedDateTime`.`d_date`, LENGTH(`_removed`.`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`acceptedDateTime`.`d_date`, LENGTH(`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as acceptedDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`contractId`, `claimElement`.`contractId`) as claimElement_contractId, 
COALESCE(`_removed`.`claimElement`.`declaredDateTime`, `claimElement`.`declaredDateTime`) as claimElement_declaredDateTime, 
COALESCE(CAST(`_removed`.`claimElement`.`effectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`effectiveDate`.`hasDate` as STRING)) as claimElement_effectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`knownDate`, `claimElement`.`effectiveDate`.`knownDate`) as claimElement_effectiveDate_knownDate, 
COALESCE(CAST(`_removed`.`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING)) as claimElement_paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`knownDate`, `claimElement`.`paymentEffectiveDate`.`knownDate`) as claimElement_paymentEffectiveDate_knownDate, 
COALESCE(CAST(`_removed`.`claimElement`.`abilityToWork`.`hasAFitNote` as STRING), CAST(`claimElement`.`abilityToWork`.`hasAFitNote` as STRING)) as claimElement_abilityToWork_hasAFitNote, 
COALESCE(CAST(`_removed`.`claimElement`.`abilityToWork`.`restrictedAbilityToWork` as STRING), CAST(`claimElement`.`abilityToWork`.`restrictedAbilityToWork` as STRING)) as claimElement_abilityToWork_restrictedAbilityToWork, 
COALESCE(CAST(`_removed`.`claimElement`.`agentToCallTerminallyIllClaimant` as STRING), CAST(`claimElement`.`agentToCallTerminallyIllClaimant` as STRING)) as claimElement_agentToCallTerminallyIllClaimant, 
COALESCE(`_removed`.`claimElement`.`claimantId`, `claimElement`.`claimantId`) as claimElement_claimantId, 
COALESCE(`_removed`.`claimElement`.`copiedFromDeclarationId`, `claimElement`.`copiedFromDeclarationId`) as claimElement_copiedFromDeclarationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`claimElement`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`claimElement`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`claimElement`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`claimElement`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`claimElement`.`createdDateTime`.`d_date`, LENGTH(`claimElement`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`claimElement`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`claimElement`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`claimElement`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`claimElement`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as claimElement_createdDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`declarationId`, `claimElement`.`declarationId`) as claimElement_declarationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`_removed`.`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as claimElement_declaredDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`type`, `claimElement`.`effectiveDate`.`type`) as claimElement_effectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`date`, `claimElement`.`effectiveDate`.`date`) as claimElement_effectiveDate_date, 
COALESCE(`removed_exploded_claimelement_healthconditions`, `exploded_claimelement_healthconditions`) as claimElement_healthConditions, 
COALESCE(`_removed`.`claimElement`.`hospitalStays`.`admissionDate`, `claimElement`.`hospitalStays`.`admissionDate`) as claimElement_hospitalStays_admissionDate, 
COALESCE(`_removed`.`claimElement`.`hospitalStays`.`dischargeDate`, `claimElement`.`hospitalStays`.`dischargeDate`) as claimElement_hospitalStays_dischargeDate, 
COALESCE(CAST(`_removed`.`claimElement`.`hospitalStays`.`hospitalStay` as STRING), CAST(`claimElement`.`hospitalStays`.`hospitalStay` as STRING)) as claimElement_hospitalStays_hospitalStay, 
COALESCE(`removed_exploded_claimelement_medicaltreatments`.`startDate`, `exploded_claimelement_medicaltreatments`.`startDate`) as claimElement_medicalTreatments_startDate, 
COALESCE(`removed_exploded_claimelement_medicaltreatments`.`treatment`, `exploded_claimelement_medicaltreatments`.`treatment`) as claimElement_medicalTreatments_treatment, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`type`, `claimElement`.`paymentEffectiveDate`.`type`) as claimElement_paymentEffectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`date`, `claimElement`.`paymentEffectiveDate`.`date`) as claimElement_paymentEffectiveDate_date, 
COALESCE(`_removed`.`claimElement`.`periodOfSicknessId`, `claimElement`.`periodOfSicknessId`) as claimElement_periodOfSicknessId, 
COALESCE(`_removed`.`claimElement`.`pregnancy`.`dueDate`, `claimElement`.`pregnancy`.`dueDate`) as claimElement_pregnancy_dueDate, 
COALESCE(`_removed`.`claimElement`.`pregnancy`.`illnessPutsPregnancyAtRisk`, `claimElement`.`pregnancy`.`illnessPutsPregnancyAtRisk`) as claimElement_pregnancy_illnessPutsPregnancyAtRisk, 
COALESCE(`_removed`.`claimElement`.`processId`, `claimElement`.`processId`) as claimElement_processId, 
COALESCE(CAST(`_removed`.`claimElement`.`reportingIllness` as STRING), CAST(`claimElement`.`reportingIllness` as STRING)) as claimElement_reportingIllness, 
COALESCE(CAST(`_removed`.`claimElement`.`supportAtWork`.`hasGuideDog` as STRING), CAST(`claimElement`.`supportAtWork`.`hasGuideDog` as STRING)) as claimElement_supportAtWork_hasGuideDog, 
COALESCE(CAST(`_removed`.`claimElement`.`supportAtWork`.`hasWheelchair` as STRING), CAST(`claimElement`.`supportAtWork`.`hasWheelchair` as STRING)) as claimElement_supportAtWork_hasWheelchair, 
COALESCE(CAST(`_removed`.`claimElement`.`supportAtWork`.`needsHearingLoop` as STRING), CAST(`claimElement`.`supportAtWork`.`needsHearingLoop` as STRING)) as claimElement_supportAtWork_needsHearingLoop, 
COALESCE(`_removed`.`claimElement`.`supportAtWork`.`otherSupport`, `claimElement`.`supportAtWork`.`otherSupport`) as claimElement_supportAtWork_otherSupport, 
COALESCE(`_removed`.`claimElement`.`terminalIllness`.`prognosis`, `claimElement`.`terminalIllness`.`prognosis`) as claimElement_terminalIllness_prognosis, 
COALESCE(CAST(`_removed`.`claimElement`.`terminalIllness`.`terminallyIll` as STRING), CAST(`claimElement`.`terminalIllness`.`terminallyIll` as STRING)) as claimElement_terminalIllness_terminallyIll, 
COALESCE(`_removed`.`claimElement`.`type`, `claimElement`.`type`) as claimElement_type, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`invalidated`.`dateTime`.`d_date`, LENGTH(`_removed`.`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as invalidated_dateTime_d_date, 
COALESCE(`_removed`.`invalidated`.`reason`, `invalidated`.`reason`) as invalidated_reason, 
COALESCE(`_removed`.`_id`.`d_oid`, `_id`.`d_oid`) as id FROM src_accepted_data_healthanddisabilitycircumstances_healthanddisabilitycircumstances
 LATERAL VIEW OUTER EXPLODE(`claimElement`.`healthConditions`) view_exploded_claimelement_healthconditions AS exploded_claimelement_healthconditions 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`claimElement`.`healthConditions`) view_removed_exploded_claimelement_healthconditions AS removed_exploded_claimelement_healthconditions 
 LATERAL VIEW OUTER EXPLODE(`claimElement`.`medicalTreatments`) view_exploded_claimelement_medicaltreatments AS exploded_claimelement_medicaltreatments 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`claimElement`.`medicalTreatments`) view_removed_exploded_claimelement_medicaltreatments AS removed_exploded_claimelement_medicaltreatments 
;


!echo ------------------------;
!echo ------------------------ employmentCircumstances;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_employmentcircumstances_employmentcircumstances
;

CREATE EXTERNAL TABLE src_accepted_data_employmentcircumstances_employmentcircumstances(
`claimElement` STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`claimantId`:STRING
,`gainfullySelfEmployed`:BOOLEAN
,`mifReviewDate`:STRING
,`mifReviewReason`:STRING
,`minimumIncomeFloor`:STRING
,`minimumIncomeFloorDate`:STRING
,`mustReportSelfEmploymentEarnings`:BOOLEAN
,`type`:STRING>
,`invalidated` STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`acceptedDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`d_oid`:STRING>
,`_version` STRING
,`declarationId` STRING
,`_removed` STRUCT<`claimElement`:STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`claimantId`:STRING
,`gainfullySelfEmployed`:BOOLEAN
,`mifReviewDate`:STRING
,`mifReviewReason`:STRING
,`minimumIncomeFloor`:STRING
,`minimumIncomeFloorDate`:STRING
,`mustReportSelfEmploymentEarnings`:BOOLEAN
,`type`:STRING>
,`invalidated`:STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`acceptedDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`d_oid`:STRING>
,`_version`:STRING
,`declarationId`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/employmentCircumstances';

DROP TABLE IF EXISTS employmentCircumstances;

CREATE TABLE employmentcircumstances STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`acceptedDateTime`.`d_date`, LENGTH(`_removed`.`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`acceptedDateTime`.`d_date`, LENGTH(`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as acceptedDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`contractId`, `claimElement`.`contractId`) as claimElement_contractId, 
COALESCE(`_removed`.`claimElement`.`declaredDateTime`, `claimElement`.`declaredDateTime`) as claimElement_declaredDateTime, 
COALESCE(CAST(`_removed`.`claimElement`.`effectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`effectiveDate`.`hasDate` as STRING)) as claimElement_effectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`knownDate`, `claimElement`.`effectiveDate`.`knownDate`) as claimElement_effectiveDate_knownDate, 
COALESCE(CAST(`_removed`.`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING)) as claimElement_paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`knownDate`, `claimElement`.`paymentEffectiveDate`.`knownDate`) as claimElement_paymentEffectiveDate_knownDate, 
COALESCE(`_removed`.`claimElement`.`claimantId`, `claimElement`.`claimantId`) as claimElement_claimantId, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`type`, `claimElement`.`effectiveDate`.`type`) as claimElement_effectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`date`, `claimElement`.`effectiveDate`.`date`) as claimElement_effectiveDate_date, 
COALESCE(CAST(`_removed`.`claimElement`.`gainfullySelfEmployed` as STRING), CAST(`claimElement`.`gainfullySelfEmployed` as STRING)) as claimElement_gainfullySelfEmployed, 
COALESCE(`_removed`.`claimElement`.`mifReviewDate`, `claimElement`.`mifReviewDate`) as claimElement_mifReviewDate, 
COALESCE(`_removed`.`claimElement`.`mifReviewReason`, `claimElement`.`mifReviewReason`) as claimElement_mifReviewReason, 
COALESCE(`_removed`.`claimElement`.`minimumIncomeFloor`, `claimElement`.`minimumIncomeFloor`) as claimElement_minimumIncomeFloor, 
COALESCE(`_removed`.`claimElement`.`minimumIncomeFloorDate`, `claimElement`.`minimumIncomeFloorDate`) as claimElement_minimumIncomeFloorDate, 
COALESCE(CAST(`_removed`.`claimElement`.`mustReportSelfEmploymentEarnings` as STRING), CAST(`claimElement`.`mustReportSelfEmploymentEarnings` as STRING)) as claimElement_mustReportSelfEmploymentEarnings, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`type`, `claimElement`.`paymentEffectiveDate`.`type`) as claimElement_paymentEffectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`date`, `claimElement`.`paymentEffectiveDate`.`date`) as claimElement_paymentEffectiveDate_date, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`_removed`.`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as claimElement_declaredDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`type`, `claimElement`.`type`) as claimElement_type, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`invalidated`.`dateTime`.`d_date`, LENGTH(`_removed`.`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as invalidated_dateTime_d_date, 
COALESCE(`_removed`.`invalidated`.`reason`, `invalidated`.`reason`) as invalidated_reason, 
COALESCE(`_removed`.`_id`.`d_oid`, `_id`.`d_oid`) as id FROM src_accepted_data_employmentcircumstances_employmentcircumstances
 src_accepted_data_employmentcircumstances_employmentcircumstances;


!echo ------------------------;
!echo ------------------------ guardiansAllowance;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_guardiansallowance_guardiansallowance
;

CREATE EXTERNAL TABLE src_accepted_data_guardiansallowance_guardiansallowance(
`claimElement` STRUCT<`namesAndDates`:ARRAY<STRUCT<`endDate`:STRING
,`name`:STRING
,`startDate`:STRING
>>
,`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`claimantId`:STRING
,`receivingGuardiansAllowance`:BOOLEAN
,`type`:STRING>
,`invalidated` STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`acceptedDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`d_oid`:STRING>
,`_version` STRING
,`declarationId` STRING
,`_removed` STRUCT<`claimElement`:STRUCT<`namesAndDates`:ARRAY<STRUCT<`endDate`:STRING
,`name`:STRING
,`startDate`:STRING
>>
,`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`claimantId`:STRING
,`receivingGuardiansAllowance`:BOOLEAN
,`type`:STRING>
,`invalidated`:STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`acceptedDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`d_oid`:STRING>
,`_version`:STRING
,`declarationId`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/guardiansAllowance';

DROP TABLE IF EXISTS guardiansAllowance;

CREATE TABLE guardiansallowance STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`acceptedDateTime`.`d_date`, LENGTH(`_removed`.`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`acceptedDateTime`.`d_date`, LENGTH(`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as acceptedDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`contractId`, `claimElement`.`contractId`) as claimElement_contractId, 
COALESCE(`_removed`.`claimElement`.`declaredDateTime`, `claimElement`.`declaredDateTime`) as claimElement_declaredDateTime, 
COALESCE(CAST(`_removed`.`claimElement`.`effectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`effectiveDate`.`hasDate` as STRING)) as claimElement_effectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`knownDate`, `claimElement`.`effectiveDate`.`knownDate`) as claimElement_effectiveDate_knownDate, 
COALESCE(CAST(`_removed`.`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING)) as claimElement_paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`knownDate`, `claimElement`.`paymentEffectiveDate`.`knownDate`) as claimElement_paymentEffectiveDate_knownDate, 
COALESCE(`_removed`.`claimElement`.`claimantId`, `claimElement`.`claimantId`) as claimElement_claimantId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`_removed`.`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as claimElement_declaredDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`type`, `claimElement`.`effectiveDate`.`type`) as claimElement_effectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`date`, `claimElement`.`effectiveDate`.`date`) as claimElement_effectiveDate_date, 
COALESCE(`removed_exploded_claimelement_namesanddates`.`endDate`, `exploded_claimelement_namesanddates`.`endDate`) as claimElement_namesAndDates_endDate, 
COALESCE(`removed_exploded_claimelement_namesanddates`.`name`, `exploded_claimelement_namesanddates`.`name`) as claimElement_namesAndDates_name, 
COALESCE(`removed_exploded_claimelement_namesanddates`.`startDate`, `exploded_claimelement_namesanddates`.`startDate`) as claimElement_namesAndDates_startDate, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`type`, `claimElement`.`paymentEffectiveDate`.`type`) as claimElement_paymentEffectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`date`, `claimElement`.`paymentEffectiveDate`.`date`) as claimElement_paymentEffectiveDate_date, 
COALESCE(CAST(`_removed`.`claimElement`.`receivingGuardiansAllowance` as STRING), CAST(`claimElement`.`receivingGuardiansAllowance` as STRING)) as claimElement_receivingGuardiansAllowance, 
COALESCE(`_removed`.`claimElement`.`type`, `claimElement`.`type`) as claimElement_type, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`invalidated`.`dateTime`.`d_date`, LENGTH(`_removed`.`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as invalidated_dateTime_d_date, 
COALESCE(`_removed`.`invalidated`.`reason`, `invalidated`.`reason`) as invalidated_reason, 
COALESCE(`_removed`.`_id`.`d_oid`, `_id`.`d_oid`) as id FROM src_accepted_data_guardiansallowance_guardiansallowance
 LATERAL VIEW OUTER EXPLODE(`claimElement`.`namesAndDates`) view_exploded_claimelement_namesanddates AS exploded_claimelement_namesanddates 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`claimElement`.`namesAndDates`) view_removed_exploded_claimelement_namesanddates AS removed_exploded_claimelement_namesanddates 
;


!echo ------------------------;
!echo ------------------------ healthCircumstances;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_healthcircumstances_healthcircumstances
;

CREATE EXTERNAL TABLE src_accepted_data_healthcircumstances_healthcircumstances(
`claimElement` STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`armedForcesIndependencePayment`:BOOLEAN
,`attendanceAllowance`:BOOLEAN
,`claimantId`:STRING
,`constantAttendanceAllowance`:BOOLEAN
,`dlaAllowance`:BOOLEAN
,`healthCondition`:BOOLEAN
,`piPayment`:BOOLEAN
,`type`:STRING>
,`invalidated` STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`acceptedDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`d_oid`:STRING>
,`_version` STRING
,`declarationId` STRING
,`_removed` STRUCT<`claimElement`:STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`armedForcesIndependencePayment`:BOOLEAN
,`attendanceAllowance`:BOOLEAN
,`claimantId`:STRING
,`constantAttendanceAllowance`:BOOLEAN
,`dlaAllowance`:BOOLEAN
,`healthCondition`:BOOLEAN
,`piPayment`:BOOLEAN
,`type`:STRING>
,`invalidated`:STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`acceptedDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`d_oid`:STRING>
,`_version`:STRING
,`declarationId`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/healthCircumstances';

DROP TABLE IF EXISTS healthCircumstances;

CREATE TABLE healthcircumstances STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`acceptedDateTime`.`d_date`, LENGTH(`_removed`.`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`acceptedDateTime`.`d_date`, LENGTH(`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as acceptedDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`contractId`, `claimElement`.`contractId`) as claimElement_contractId, 
COALESCE(`_removed`.`claimElement`.`declaredDateTime`, `claimElement`.`declaredDateTime`) as claimElement_declaredDateTime, 
COALESCE(CAST(`_removed`.`claimElement`.`effectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`effectiveDate`.`hasDate` as STRING)) as claimElement_effectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`knownDate`, `claimElement`.`effectiveDate`.`knownDate`) as claimElement_effectiveDate_knownDate, 
COALESCE(CAST(`_removed`.`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING)) as claimElement_paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`knownDate`, `claimElement`.`paymentEffectiveDate`.`knownDate`) as claimElement_paymentEffectiveDate_knownDate, 
COALESCE(CAST(`_removed`.`claimElement`.`armedForcesIndependencePayment` as STRING), CAST(`claimElement`.`armedForcesIndependencePayment` as STRING)) as claimElement_armedForcesIndependencePayment, 
COALESCE(CAST(`_removed`.`claimElement`.`attendanceAllowance` as STRING), CAST(`claimElement`.`attendanceAllowance` as STRING)) as claimElement_attendanceAllowance, 
COALESCE(`_removed`.`claimElement`.`claimantId`, `claimElement`.`claimantId`) as claimElement_claimantId, 
COALESCE(CAST(`_removed`.`claimElement`.`constantAttendanceAllowance` as STRING), CAST(`claimElement`.`constantAttendanceAllowance` as STRING)) as claimElement_constantAttendanceAllowance, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`_removed`.`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as claimElement_declaredDateTime_d_date, 
COALESCE(CAST(`_removed`.`claimElement`.`dlaAllowance` as STRING), CAST(`claimElement`.`dlaAllowance` as STRING)) as claimElement_dlaAllowance, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`type`, `claimElement`.`effectiveDate`.`type`) as claimElement_effectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`date`, `claimElement`.`effectiveDate`.`date`) as claimElement_effectiveDate_date, 
COALESCE(CAST(`_removed`.`claimElement`.`healthCondition` as STRING), CAST(`claimElement`.`healthCondition` as STRING)) as claimElement_healthCondition, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`type`, `claimElement`.`paymentEffectiveDate`.`type`) as claimElement_paymentEffectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`date`, `claimElement`.`paymentEffectiveDate`.`date`) as claimElement_paymentEffectiveDate_date, 
COALESCE(CAST(`_removed`.`claimElement`.`piPayment` as STRING), CAST(`claimElement`.`piPayment` as STRING)) as claimElement_piPayment, 
COALESCE(`_removed`.`claimElement`.`type`, `claimElement`.`type`) as claimElement_type, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`invalidated`.`dateTime`.`d_date`, LENGTH(`_removed`.`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as invalidated_dateTime_d_date, 
COALESCE(`_removed`.`invalidated`.`reason`, `invalidated`.`reason`) as invalidated_reason, 
COALESCE(`_removed`.`_id`.`d_oid`, `_id`.`d_oid`) as id FROM src_accepted_data_healthcircumstances_healthcircumstances
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
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`acceptedDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`d_oid`:STRING>
,`_version` STRING
,`declarationId` STRING
,`_removed` STRUCT<`claimElement`:STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING>
,`contractId`:STRING
,`declaredDateTime`:STRING>
,`invalidated`:STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`acceptedDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`d_oid`:STRING>
,`_version`:STRING
,`declarationId`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/personDetails';

DROP TABLE IF EXISTS personDetails;

CREATE TABLE persondetails STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`acceptedDateTime`.`d_date`, LENGTH(`_removed`.`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`acceptedDateTime`.`d_date`, LENGTH(`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as acceptedDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`contractId`, `claimElement`.`contractId`) as claimElement_contractId, 
COALESCE(`_removed`.`claimElement`.`declaredDateTime`, `claimElement`.`declaredDateTime`) as claimElement_declaredDateTime, 
COALESCE(CAST(`_removed`.`claimElement`.`effectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`effectiveDate`.`hasDate` as STRING)) as claimElement_effectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`knownDate`, `claimElement`.`effectiveDate`.`knownDate`) as claimElement_effectiveDate_knownDate, 
COALESCE(CAST(`_removed`.`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING)) as claimElement_paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`knownDate`, `claimElement`.`paymentEffectiveDate`.`knownDate`) as claimElement_paymentEffectiveDate_knownDate, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`invalidated`.`dateTime`.`d_date`, LENGTH(`_removed`.`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as invalidated_dateTime_d_date, 
COALESCE(`_removed`.`invalidated`.`reason`, `invalidated`.`reason`) as invalidated_reason, 
COALESCE(`_removed`.`_id`.`d_oid`, `_id`.`d_oid`) as id FROM src_accepted_data_persondetails_persondetails
 src_accepted_data_persondetails_persondetails;


!echo ------------------------;
!echo ------------------------ bankDetails;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_bankdetails_bankdetails
;

CREATE EXTERNAL TABLE src_accepted_data_bankdetails_bankdetails(
`claimElement` STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`ignoreRollNumber`:BOOLEAN
,`type`:STRING>
,`invalidated` STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`acceptedDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`declarationId`:STRING>
,`_version` STRING
,`declarationId` STRING
,`_removed` STRUCT<`claimElement`:STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`ignoreRollNumber`:BOOLEAN
,`type`:STRING>
,`invalidated`:STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`acceptedDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`declarationId`:STRING>
,`_version`:STRING
,`declarationId`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/bankDetails';

DROP TABLE IF EXISTS bankDetails;

CREATE TABLE bankdetails STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`acceptedDateTime`.`d_date`, LENGTH(`_removed`.`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`acceptedDateTime`.`d_date`, LENGTH(`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as acceptedDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`contractId`, `claimElement`.`contractId`) as claimElement_contractId, 
COALESCE(`_removed`.`claimElement`.`declaredDateTime`, `claimElement`.`declaredDateTime`) as claimElement_declaredDateTime, 
COALESCE(CAST(`_removed`.`claimElement`.`effectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`effectiveDate`.`hasDate` as STRING)) as claimElement_effectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`knownDate`, `claimElement`.`effectiveDate`.`knownDate`) as claimElement_effectiveDate_knownDate, 
COALESCE(CAST(`_removed`.`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING)) as claimElement_paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`knownDate`, `claimElement`.`paymentEffectiveDate`.`knownDate`) as claimElement_paymentEffectiveDate_knownDate, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`_removed`.`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as claimElement_declaredDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`type`, `claimElement`.`effectiveDate`.`type`) as claimElement_effectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`date`, `claimElement`.`effectiveDate`.`date`) as claimElement_effectiveDate_date, 
COALESCE(CAST(`_removed`.`claimElement`.`ignoreRollNumber` as STRING), CAST(`claimElement`.`ignoreRollNumber` as STRING)) as claimElement_ignoreRollNumber, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`type`, `claimElement`.`paymentEffectiveDate`.`type`) as claimElement_paymentEffectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`date`, `claimElement`.`paymentEffectiveDate`.`date`) as claimElement_paymentEffectiveDate_date, 
COALESCE(`_removed`.`claimElement`.`type`, `claimElement`.`type`) as claimElement_type, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`invalidated`.`dateTime`.`d_date`, LENGTH(`_removed`.`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as invalidated_dateTime_d_date, 
COALESCE(`_removed`.`invalidated`.`reason`, `invalidated`.`reason`) as invalidated_reason, 
COALESCE(`_removed`.`_id`.`declarationId`, `_id`.`declarationId`) as id FROM src_accepted_data_bankdetails_bankdetails
 src_accepted_data_bankdetails_bankdetails;


!echo ------------------------;
!echo ------------------------ otherBenefit;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_otherbenefit_otherbenefit
;

CREATE EXTERNAL TABLE src_accepted_data_otherbenefit_otherbenefit(
`claimElement` STRUCT<`children`:ARRAY<STRUCT<`id`:STRING
,`name`:STRING
>>
,`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`removedDateTime`:STRUCT<`d_date`:STRING>
,`supersededDateTime`:STRUCT<`d_date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`amount`:STRING
,`claimantId`:STRING
,`endDate`:STRING
,`frequency`:STRING
,`originalEndDate`:STRING
,`otherBenefitAwardType`:STRING
,`otherBenefitClaimId`:STRING
,`otherBenefitClaimVersion`:STRING
,`otherBenefitSourceType`:STRING
,`startDate`:STRING
,`childName`:STRING
,`type`:STRING>
,`invalidated` STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`acceptedDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`d_oid`:STRING>
,`_version` STRING
,`declarationId` STRING
,`_removed` STRUCT<`claimElement`:STRUCT<`children`:ARRAY<STRUCT<`id`:STRING
,`name`:STRING
>>
,`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`removedDateTime`:STRUCT<`d_date`:STRING>
,`supersededDateTime`:STRUCT<`d_date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`amount`:STRING
,`claimantId`:STRING
,`endDate`:STRING
,`frequency`:STRING
,`originalEndDate`:STRING
,`otherBenefitAwardType`:STRING
,`otherBenefitClaimId`:STRING
,`otherBenefitClaimVersion`:STRING
,`otherBenefitSourceType`:STRING
,`startDate`:STRING
,`childName`:STRING
,`type`:STRING>
,`invalidated`:STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`acceptedDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`d_oid`:STRING>
,`_version`:STRING
,`declarationId`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/otherBenefit';

DROP TABLE IF EXISTS otherBenefit;

CREATE TABLE otherbenefit STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`acceptedDateTime`.`d_date`, LENGTH(`_removed`.`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`acceptedDateTime`.`d_date`, LENGTH(`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as acceptedDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`contractId`, `claimElement`.`contractId`) as claimElement_contractId, 
COALESCE(`_removed`.`claimElement`.`declaredDateTime`, `claimElement`.`declaredDateTime`) as claimElement_declaredDateTime, 
COALESCE(CAST(`_removed`.`claimElement`.`effectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`effectiveDate`.`hasDate` as STRING)) as claimElement_effectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`knownDate`, `claimElement`.`effectiveDate`.`knownDate`) as claimElement_effectiveDate_knownDate, 
COALESCE(CAST(`_removed`.`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING)) as claimElement_paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`knownDate`, `claimElement`.`paymentEffectiveDate`.`knownDate`) as claimElement_paymentEffectiveDate_knownDate, 
COALESCE(`_removed`.`claimElement`.`amount`, `claimElement`.`amount`) as claimElement_amount, 
COALESCE(`removed_exploded_claimelement_children`.`id`, `exploded_claimelement_children`.`id`) as claimElement_children_id, 
COALESCE(`removed_exploded_claimelement_children`.`name`, `exploded_claimelement_children`.`name`) as claimElement_children_name, 
COALESCE(`_removed`.`claimElement`.`claimantId`, `claimElement`.`claimantId`) as claimElement_claimantId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`claimElement`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`claimElement`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`claimElement`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`claimElement`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`claimElement`.`createdDateTime`.`d_date`, LENGTH(`claimElement`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`claimElement`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`claimElement`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`claimElement`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`claimElement`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as claimElement_createdDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`type`, `claimElement`.`effectiveDate`.`type`) as claimElement_effectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`date`, `claimElement`.`effectiveDate`.`date`) as claimElement_effectiveDate_date, 
COALESCE(`_removed`.`claimElement`.`endDate`, `claimElement`.`endDate`) as claimElement_endDate, 
COALESCE(`_removed`.`claimElement`.`frequency`, `claimElement`.`frequency`) as claimElement_frequency, 
COALESCE(`_removed`.`claimElement`.`originalEndDate`, `claimElement`.`originalEndDate`) as claimElement_originalEndDate, 
COALESCE(`_removed`.`claimElement`.`otherBenefitAwardType`, `claimElement`.`otherBenefitAwardType`) as claimElement_otherBenefitAwardType, 
COALESCE(`_removed`.`claimElement`.`otherBenefitClaimId`, `claimElement`.`otherBenefitClaimId`) as claimElement_otherBenefitClaimId, 
COALESCE(`_removed`.`claimElement`.`otherBenefitClaimVersion`, `claimElement`.`otherBenefitClaimVersion`) as claimElement_otherBenefitClaimVersion, 
COALESCE(`_removed`.`claimElement`.`otherBenefitSourceType`, `claimElement`.`otherBenefitSourceType`) as claimElement_otherBenefitSourceType, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`type`, `claimElement`.`paymentEffectiveDate`.`type`) as claimElement_paymentEffectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`date`, `claimElement`.`paymentEffectiveDate`.`date`) as claimElement_paymentEffectiveDate_date, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`claimElement`.`removedDateTime`.`d_date`, LENGTH(`_removed`.`claimElement`.`removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`claimElement`.`removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`claimElement`.`removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`claimElement`.`removedDateTime`.`d_date`, LENGTH(`claimElement`.`removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`claimElement`.`removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`claimElement`.`removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`claimElement`.`removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`claimElement`.`removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as claimElement_removedDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`startDate`, `claimElement`.`startDate`) as claimElement_startDate, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`claimElement`.`supersededDateTime`.`d_date`, LENGTH(`_removed`.`claimElement`.`supersededDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`supersededDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`supersededDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`supersededDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`supersededDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`claimElement`.`supersededDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`claimElement`.`supersededDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`claimElement`.`supersededDateTime`.`d_date`, LENGTH(`claimElement`.`supersededDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`claimElement`.`supersededDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`supersededDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`claimElement`.`supersededDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`supersededDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`claimElement`.`supersededDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`claimElement`.`supersededDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as claimElement_supersededDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`childName`, `claimElement`.`childName`) as claimElement_childName, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`_removed`.`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as claimElement_declaredDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`type`, `claimElement`.`type`) as claimElement_type, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`invalidated`.`dateTime`.`d_date`, LENGTH(`_removed`.`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as invalidated_dateTime_d_date, 
COALESCE(`_removed`.`invalidated`.`reason`, `invalidated`.`reason`) as invalidated_reason, 
COALESCE(`_removed`.`_id`.`d_oid`, `_id`.`d_oid`) as id FROM src_accepted_data_otherbenefit_otherbenefit
 LATERAL VIEW OUTER EXPLODE(`claimElement`.`children`) view_exploded_claimelement_children AS exploded_claimelement_children 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`claimElement`.`children`) view_removed_exploded_claimelement_children AS removed_exploded_claimelement_children 
;


!echo ------------------------;
!echo ------------------------ apa;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_apa_apa
;

CREATE EXTERNAL TABLE src_accepted_data_apa_apa(
`claimElement` STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`directLandlordPaymentDetails`:STRUCT<`creditorReferenceNumber`:STRING
,`landlordApaEmailAddress`:STRING
,`relatedHousingCircumstances`:STRING
,`tenancyId`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`apaTypes`:ARRAY<STRING>
,`contractId`:STRING
,`alternativeFrequency`:STRING
,`apaAgreed`:BOOLEAN
,`type`:STRING>
,`invalidated` STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`acceptedDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`declarationId`:STRING>
,`_version` STRING
,`declarationId` STRING
,`_removed` STRUCT<`claimElement`:STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`directLandlordPaymentDetails`:STRUCT<`creditorReferenceNumber`:STRING
,`landlordApaEmailAddress`:STRING
,`relatedHousingCircumstances`:STRING
,`tenancyId`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`apaTypes`:ARRAY<STRING>
,`contractId`:STRING
,`alternativeFrequency`:STRING
,`apaAgreed`:BOOLEAN
,`type`:STRING>
,`invalidated`:STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`acceptedDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`declarationId`:STRING>
,`_version`:STRING
,`declarationId`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/apa';

DROP TABLE IF EXISTS apa;

CREATE TABLE apa STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`acceptedDateTime`.`d_date`, LENGTH(`_removed`.`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`acceptedDateTime`.`d_date`, LENGTH(`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as acceptedDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`contractId`, `claimElement`.`contractId`) as claimElement_contractId, 
COALESCE(`_removed`.`claimElement`.`declaredDateTime`, `claimElement`.`declaredDateTime`) as claimElement_declaredDateTime, 
COALESCE(CAST(`_removed`.`claimElement`.`effectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`effectiveDate`.`hasDate` as STRING)) as claimElement_effectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`knownDate`, `claimElement`.`effectiveDate`.`knownDate`) as claimElement_effectiveDate_knownDate, 
COALESCE(CAST(`_removed`.`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING)) as claimElement_paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`knownDate`, `claimElement`.`paymentEffectiveDate`.`knownDate`) as claimElement_paymentEffectiveDate_knownDate, 
COALESCE(`_removed`.`claimElement`.`alternativeFrequency`, `claimElement`.`alternativeFrequency`) as claimElement_alternativeFrequency, 
COALESCE(CAST(`_removed`.`claimElement`.`apaAgreed` as STRING), CAST(`claimElement`.`apaAgreed` as STRING)) as claimElement_apaAgreed, 
COALESCE(`removed_exploded_claimelement_apatypes`, `exploded_claimelement_apatypes`) as claimElement_apaTypes, 
COALESCE(`_removed`.`claimElement`.`directLandlordPaymentDetails`.`creditorReferenceNumber`, `claimElement`.`directLandlordPaymentDetails`.`creditorReferenceNumber`) as claimElement_directLandlordPaymentDetails_creditorReferenceNumber, 
COALESCE(`_removed`.`claimElement`.`directLandlordPaymentDetails`.`landlordApaEmailAddress`, `claimElement`.`directLandlordPaymentDetails`.`landlordApaEmailAddress`) as claimElement_directLandlordPaymentDetails_landlordApaEmailAddress, 
COALESCE(`_removed`.`claimElement`.`directLandlordPaymentDetails`.`relatedHousingCircumstances`, `claimElement`.`directLandlordPaymentDetails`.`relatedHousingCircumstances`) as claimElement_directLandlordPaymentDetails_relatedHousingCircumstances, 
COALESCE(`_removed`.`claimElement`.`directLandlordPaymentDetails`.`tenancyId`, `claimElement`.`directLandlordPaymentDetails`.`tenancyId`) as claimElement_directLandlordPaymentDetails_tenancyId, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`type`, `claimElement`.`effectiveDate`.`type`) as claimElement_effectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`date`, `claimElement`.`effectiveDate`.`date`) as claimElement_effectiveDate_date, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`type`, `claimElement`.`paymentEffectiveDate`.`type`) as claimElement_paymentEffectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`date`, `claimElement`.`paymentEffectiveDate`.`date`) as claimElement_paymentEffectiveDate_date, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`_removed`.`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as claimElement_declaredDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`type`, `claimElement`.`type`) as claimElement_type, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`invalidated`.`dateTime`.`d_date`, LENGTH(`_removed`.`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as invalidated_dateTime_d_date, 
COALESCE(`_removed`.`invalidated`.`reason`, `invalidated`.`reason`) as invalidated_reason, 
COALESCE(`_removed`.`_id`.`declarationId`, `_id`.`declarationId`) as id FROM src_accepted_data_apa_apa
 LATERAL VIEW OUTER EXPLODE(`claimElement`.`apaTypes`) view_exploded_claimelement_apatypes AS exploded_claimelement_apatypes 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`claimElement`.`apaTypes`) view_removed_exploded_claimelement_apatypes AS removed_exploded_claimelement_apatypes 
;


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
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`acceptedDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`assessmentPeriodId`:STRING>
,`_version` STRING
,`declarationId` STRING
,`_removed` STRUCT<`claimElement`:STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING>
,`contractId`:STRING
,`declaredDateTime`:STRING>
,`invalidated`:STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`acceptedDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`assessmentPeriodId`:STRING>
,`_version`:STRING
,`declarationId`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/calculateDeductions';

DROP TABLE IF EXISTS calculateDeductions;

CREATE TABLE calculatedeductions STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`acceptedDateTime`.`d_date`, LENGTH(`_removed`.`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`acceptedDateTime`.`d_date`, LENGTH(`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as acceptedDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`contractId`, `claimElement`.`contractId`) as claimElement_contractId, 
COALESCE(`_removed`.`claimElement`.`declaredDateTime`, `claimElement`.`declaredDateTime`) as claimElement_declaredDateTime, 
COALESCE(CAST(`_removed`.`claimElement`.`effectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`effectiveDate`.`hasDate` as STRING)) as claimElement_effectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`knownDate`, `claimElement`.`effectiveDate`.`knownDate`) as claimElement_effectiveDate_knownDate, 
COALESCE(CAST(`_removed`.`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING)) as claimElement_paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`knownDate`, `claimElement`.`paymentEffectiveDate`.`knownDate`) as claimElement_paymentEffectiveDate_knownDate, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`invalidated`.`dateTime`.`d_date`, LENGTH(`_removed`.`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as invalidated_dateTime_d_date, 
COALESCE(`_removed`.`invalidated`.`reason`, `invalidated`.`reason`) as invalidated_reason, 
COALESCE(`_removed`.`_id`.`assessmentPeriodId`, `_id`.`assessmentPeriodId`) as id FROM src_accepted_data_calculatedeductions_calculatedeductions
 src_accepted_data_calculatedeductions_calculatedeductions;


!echo ------------------------;
!echo ------------------------ address;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_address_address
;

CREATE EXTERNAL TABLE src_accepted_data_address_address(
`claimElement` STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`postcode`:STRING
,`region`:STRING
,`type`:STRING>
,`invalidated` STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`acceptedDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`d_oid`:STRING>
,`_version` STRING
,`declarationId` STRING
,`_removed` STRUCT<`claimElement`:STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`postcode`:STRING
,`region`:STRING
,`type`:STRING>
,`invalidated`:STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`acceptedDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`d_oid`:STRING>
,`_version`:STRING
,`declarationId`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/address';

DROP TABLE IF EXISTS address;

CREATE TABLE address STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`acceptedDateTime`.`d_date`, LENGTH(`_removed`.`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`acceptedDateTime`.`d_date`, LENGTH(`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as acceptedDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`contractId`, `claimElement`.`contractId`) as claimElement_contractId, 
COALESCE(`_removed`.`claimElement`.`declaredDateTime`, `claimElement`.`declaredDateTime`) as claimElement_declaredDateTime, 
COALESCE(CAST(`_removed`.`claimElement`.`effectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`effectiveDate`.`hasDate` as STRING)) as claimElement_effectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`knownDate`, `claimElement`.`effectiveDate`.`knownDate`) as claimElement_effectiveDate_knownDate, 
COALESCE(CAST(`_removed`.`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING)) as claimElement_paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`knownDate`, `claimElement`.`paymentEffectiveDate`.`knownDate`) as claimElement_paymentEffectiveDate_knownDate, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`_removed`.`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as claimElement_declaredDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`type`, `claimElement`.`effectiveDate`.`type`) as claimElement_effectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`date`, `claimElement`.`effectiveDate`.`date`) as claimElement_effectiveDate_date, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`type`, `claimElement`.`paymentEffectiveDate`.`type`) as claimElement_paymentEffectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`date`, `claimElement`.`paymentEffectiveDate`.`date`) as claimElement_paymentEffectiveDate_date, 
COALESCE(`_removed`.`claimElement`.`postcode`, `claimElement`.`postcode`) as claimElement_postcode, 
COALESCE(`_removed`.`claimElement`.`region`, `claimElement`.`region`) as claimElement_region, 
COALESCE(`_removed`.`claimElement`.`type`, `claimElement`.`type`) as claimElement_type, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`invalidated`.`dateTime`.`d_date`, LENGTH(`_removed`.`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as invalidated_dateTime_d_date, 
COALESCE(`_removed`.`invalidated`.`reason`, `invalidated`.`reason`) as invalidated_reason, 
COALESCE(`_removed`.`_id`.`d_oid`, `_id`.`d_oid`) as id FROM src_accepted_data_address_address
 src_accepted_data_address_address;


!echo ------------------------;
!echo ------------------------ ineligiblePartner;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_ineligiblepartner_ineligiblepartner
;

CREATE EXTERNAL TABLE src_accepted_data_ineligiblepartner_ineligiblepartner(
`claimElement` STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`ineligiblePartner`:STRING
,`ineligiblePartnerHasNino`:BOOLEAN
,`type`:STRING>
,`invalidated` STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`acceptedDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`d_oid`:STRING>
,`_version` STRING
,`declarationId` STRING
,`_removed` STRUCT<`claimElement`:STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`ineligiblePartner`:STRING
,`ineligiblePartnerHasNino`:BOOLEAN
,`type`:STRING>
,`invalidated`:STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`acceptedDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`d_oid`:STRING>
,`_version`:STRING
,`declarationId`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/ineligiblePartner';

DROP TABLE IF EXISTS ineligiblePartner;

CREATE TABLE ineligiblepartner STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`acceptedDateTime`.`d_date`, LENGTH(`_removed`.`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`acceptedDateTime`.`d_date`, LENGTH(`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as acceptedDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`contractId`, `claimElement`.`contractId`) as claimElement_contractId, 
COALESCE(`_removed`.`claimElement`.`declaredDateTime`, `claimElement`.`declaredDateTime`) as claimElement_declaredDateTime, 
COALESCE(CAST(`_removed`.`claimElement`.`effectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`effectiveDate`.`hasDate` as STRING)) as claimElement_effectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`knownDate`, `claimElement`.`effectiveDate`.`knownDate`) as claimElement_effectiveDate_knownDate, 
COALESCE(CAST(`_removed`.`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING)) as claimElement_paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`knownDate`, `claimElement`.`paymentEffectiveDate`.`knownDate`) as claimElement_paymentEffectiveDate_knownDate, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`type`, `claimElement`.`effectiveDate`.`type`) as claimElement_effectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`date`, `claimElement`.`effectiveDate`.`date`) as claimElement_effectiveDate_date, 
COALESCE(`_removed`.`claimElement`.`ineligiblePartner`, `claimElement`.`ineligiblePartner`) as claimElement_ineligiblePartner, 
COALESCE(CAST(`_removed`.`claimElement`.`ineligiblePartnerHasNino` as STRING), CAST(`claimElement`.`ineligiblePartnerHasNino` as STRING)) as claimElement_ineligiblePartnerHasNino, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`type`, `claimElement`.`paymentEffectiveDate`.`type`) as claimElement_paymentEffectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`date`, `claimElement`.`paymentEffectiveDate`.`date`) as claimElement_paymentEffectiveDate_date, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`_removed`.`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as claimElement_declaredDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`type`, `claimElement`.`type`) as claimElement_type, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`invalidated`.`dateTime`.`d_date`, LENGTH(`_removed`.`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as invalidated_dateTime_d_date, 
COALESCE(`_removed`.`invalidated`.`reason`, `invalidated`.`reason`) as invalidated_reason, 
COALESCE(`_removed`.`_id`.`d_oid`, `_id`.`d_oid`) as id FROM src_accepted_data_ineligiblepartner_ineligiblepartner
 src_accepted_data_ineligiblepartner_ineligiblepartner;


!echo ------------------------;
!echo ------------------------ pregnancy;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_pregnancy_pregnancy
;

CREATE EXTERNAL TABLE src_accepted_data_pregnancy_pregnancy(
`claimElement` STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`claimantId`:STRING
,`dueDate`:STRING
,`type`:STRING>
,`invalidated` STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`acceptedDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`d_oid`:STRING>
,`_version` STRING
,`declarationId` STRING
,`_removed` STRUCT<`claimElement`:STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`claimantId`:STRING
,`dueDate`:STRING
,`type`:STRING>
,`invalidated`:STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`acceptedDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`d_oid`:STRING>
,`_version`:STRING
,`declarationId`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/pregnancy';

DROP TABLE IF EXISTS pregnancy;

CREATE TABLE pregnancy STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`acceptedDateTime`.`d_date`, LENGTH(`_removed`.`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`acceptedDateTime`.`d_date`, LENGTH(`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as acceptedDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`contractId`, `claimElement`.`contractId`) as claimElement_contractId, 
COALESCE(`_removed`.`claimElement`.`declaredDateTime`, `claimElement`.`declaredDateTime`) as claimElement_declaredDateTime, 
COALESCE(CAST(`_removed`.`claimElement`.`effectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`effectiveDate`.`hasDate` as STRING)) as claimElement_effectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`knownDate`, `claimElement`.`effectiveDate`.`knownDate`) as claimElement_effectiveDate_knownDate, 
COALESCE(CAST(`_removed`.`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING)) as claimElement_paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`knownDate`, `claimElement`.`paymentEffectiveDate`.`knownDate`) as claimElement_paymentEffectiveDate_knownDate, 
COALESCE(`_removed`.`claimElement`.`claimantId`, `claimElement`.`claimantId`) as claimElement_claimantId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`_removed`.`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as claimElement_declaredDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`dueDate`, `claimElement`.`dueDate`) as claimElement_dueDate, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`type`, `claimElement`.`effectiveDate`.`type`) as claimElement_effectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`date`, `claimElement`.`effectiveDate`.`date`) as claimElement_effectiveDate_date, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`type`, `claimElement`.`paymentEffectiveDate`.`type`) as claimElement_paymentEffectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`date`, `claimElement`.`paymentEffectiveDate`.`date`) as claimElement_paymentEffectiveDate_date, 
COALESCE(`_removed`.`claimElement`.`type`, `claimElement`.`type`) as claimElement_type, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`invalidated`.`dateTime`.`d_date`, LENGTH(`_removed`.`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as invalidated_dateTime_d_date, 
COALESCE(`_removed`.`invalidated`.`reason`, `invalidated`.`reason`) as invalidated_reason, 
COALESCE(`_removed`.`_id`.`d_oid`, `_id`.`d_oid`) as id FROM src_accepted_data_pregnancy_pregnancy
 src_accepted_data_pregnancy_pregnancy;


!echo ------------------------;
!echo ------------------------ workAndEarningsCircumstances;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_workandearningscircumstances_workandearningscircumstances
;

CREATE EXTERNAL TABLE src_accepted_data_workandearningscircumstances_workandearningscircumstances(
`claimElement` STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`childTaxCredit`:STRUCT<`dateStopped`:STRING
,`receivedInLast5Weeks`:BOOLEAN>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`earnings`:STRUCT<`amount`:STRING
,`hoursPerWeek`:STRING
,`paymentFrequency`:STRING>
,`employerAndMaternityAllowance`:STRUCT<`maternityAllowance`:BOOLEAN
,`maternityAllowanceAmount`:STRING
,`maternityAllowancePaymentFrequency`:STRING
,`otherEmployerPaymentFrequency`:STRING
,`otherPayFromEmployer`:BOOLEAN
,`otherPayFromEmployerAmount`:STRING>
,`futureEmployment`:STRUCT<`expectedEarnings`:STRING
,`hoursPerWeek`:STRING
,`paymentFrequency`:STRING
,`startDate`:STRING
,`startingEmployment`:BOOLEAN>
,`workHistory`:STRUCT<`dateStoppedWorking`:STRING
,`stoppedWorkingInLast9Months`:BOOLEAN>
,`workingTaxCredit`:STRUCT<`dateStopped`:STRING
,`receivedInLast5Weeks`:BOOLEAN>
,`contractId`:STRING
,`citizenId`:STRING
,`employmentStatus`:STRING
,`expectedPreviousEarnings`:STRING
,`startingSelfEmployment`:BOOLEAN
,`working`:BOOLEAN
,`type`:STRING>
,`invalidated` STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`acceptedDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`d_oid`:STRING>
,`_version` STRING
,`declarationId` STRING
,`_removed` STRUCT<`claimElement`:STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`childTaxCredit`:STRUCT<`dateStopped`:STRING
,`receivedInLast5Weeks`:BOOLEAN>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`earnings`:STRUCT<`amount`:STRING
,`hoursPerWeek`:STRING
,`paymentFrequency`:STRING>
,`employerAndMaternityAllowance`:STRUCT<`maternityAllowance`:BOOLEAN
,`maternityAllowanceAmount`:STRING
,`maternityAllowancePaymentFrequency`:STRING
,`otherEmployerPaymentFrequency`:STRING
,`otherPayFromEmployer`:BOOLEAN
,`otherPayFromEmployerAmount`:STRING>
,`futureEmployment`:STRUCT<`expectedEarnings`:STRING
,`hoursPerWeek`:STRING
,`paymentFrequency`:STRING
,`startDate`:STRING
,`startingEmployment`:BOOLEAN>
,`workHistory`:STRUCT<`dateStoppedWorking`:STRING
,`stoppedWorkingInLast9Months`:BOOLEAN>
,`workingTaxCredit`:STRUCT<`dateStopped`:STRING
,`receivedInLast5Weeks`:BOOLEAN>
,`contractId`:STRING
,`citizenId`:STRING
,`employmentStatus`:STRING
,`expectedPreviousEarnings`:STRING
,`startingSelfEmployment`:BOOLEAN
,`working`:BOOLEAN
,`type`:STRING>
,`invalidated`:STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`acceptedDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`d_oid`:STRING>
,`_version`:STRING
,`declarationId`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/workAndEarningsCircumstances';

DROP TABLE IF EXISTS workAndEarningsCircumstances;

CREATE TABLE workandearningscircumstances STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`acceptedDateTime`.`d_date`, LENGTH(`_removed`.`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`acceptedDateTime`.`d_date`, LENGTH(`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as acceptedDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`contractId`, `claimElement`.`contractId`) as claimElement_contractId, 
COALESCE(`_removed`.`claimElement`.`declaredDateTime`, `claimElement`.`declaredDateTime`) as claimElement_declaredDateTime, 
COALESCE(CAST(`_removed`.`claimElement`.`effectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`effectiveDate`.`hasDate` as STRING)) as claimElement_effectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`knownDate`, `claimElement`.`effectiveDate`.`knownDate`) as claimElement_effectiveDate_knownDate, 
COALESCE(CAST(`_removed`.`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING)) as claimElement_paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`knownDate`, `claimElement`.`paymentEffectiveDate`.`knownDate`) as claimElement_paymentEffectiveDate_knownDate, 
COALESCE(`_removed`.`claimElement`.`childTaxCredit`.`dateStopped`, `claimElement`.`childTaxCredit`.`dateStopped`) as claimElement_childTaxCredit_dateStopped, 
COALESCE(CAST(`_removed`.`claimElement`.`childTaxCredit`.`receivedInLast5Weeks` as STRING), CAST(`claimElement`.`childTaxCredit`.`receivedInLast5Weeks` as STRING)) as claimElement_childTaxCredit_receivedInLast5Weeks, 
COALESCE(`_removed`.`claimElement`.`citizenId`, `claimElement`.`citizenId`) as claimElement_citizenId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`_removed`.`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as claimElement_declaredDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`earnings`.`amount`, `claimElement`.`earnings`.`amount`) as claimElement_earnings_amount, 
COALESCE(`_removed`.`claimElement`.`earnings`.`hoursPerWeek`, `claimElement`.`earnings`.`hoursPerWeek`) as claimElement_earnings_hoursPerWeek, 
COALESCE(`_removed`.`claimElement`.`earnings`.`paymentFrequency`, `claimElement`.`earnings`.`paymentFrequency`) as claimElement_earnings_paymentFrequency, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`type`, `claimElement`.`effectiveDate`.`type`) as claimElement_effectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`date`, `claimElement`.`effectiveDate`.`date`) as claimElement_effectiveDate_date, 
COALESCE(CAST(`_removed`.`claimElement`.`employerAndMaternityAllowance`.`maternityAllowance` as STRING), CAST(`claimElement`.`employerAndMaternityAllowance`.`maternityAllowance` as STRING)) as claimElement_employerAndMaternityAllowance_maternityAllowance, 
COALESCE(`_removed`.`claimElement`.`employerAndMaternityAllowance`.`maternityAllowanceAmount`, `claimElement`.`employerAndMaternityAllowance`.`maternityAllowanceAmount`) as claimElement_employerAndMaternityAllowance_maternityAllowanceAmount, 
COALESCE(`_removed`.`claimElement`.`employerAndMaternityAllowance`.`maternityAllowancePaymentFrequency`, `claimElement`.`employerAndMaternityAllowance`.`maternityAllowancePaymentFrequency`) as claimElement_employerAndMaternityAllowance_maternityAllowancePaymentFrequency, 
COALESCE(`_removed`.`claimElement`.`employerAndMaternityAllowance`.`otherEmployerPaymentFrequency`, `claimElement`.`employerAndMaternityAllowance`.`otherEmployerPaymentFrequency`) as claimElement_employerAndMaternityAllowance_otherEmployerPaymentFrequency, 
COALESCE(CAST(`_removed`.`claimElement`.`employerAndMaternityAllowance`.`otherPayFromEmployer` as STRING), CAST(`claimElement`.`employerAndMaternityAllowance`.`otherPayFromEmployer` as STRING)) as claimElement_employerAndMaternityAllowance_otherPayFromEmployer, 
COALESCE(`_removed`.`claimElement`.`employerAndMaternityAllowance`.`otherPayFromEmployerAmount`, `claimElement`.`employerAndMaternityAllowance`.`otherPayFromEmployerAmount`) as claimElement_employerAndMaternityAllowance_otherPayFromEmployerAmount, 
COALESCE(`_removed`.`claimElement`.`employmentStatus`, `claimElement`.`employmentStatus`) as claimElement_employmentStatus, 
COALESCE(`_removed`.`claimElement`.`expectedPreviousEarnings`, `claimElement`.`expectedPreviousEarnings`) as claimElement_expectedPreviousEarnings, 
COALESCE(`_removed`.`claimElement`.`futureEmployment`.`expectedEarnings`, `claimElement`.`futureEmployment`.`expectedEarnings`) as claimElement_futureEmployment_expectedEarnings, 
COALESCE(`_removed`.`claimElement`.`futureEmployment`.`hoursPerWeek`, `claimElement`.`futureEmployment`.`hoursPerWeek`) as claimElement_futureEmployment_hoursPerWeek, 
COALESCE(`_removed`.`claimElement`.`futureEmployment`.`paymentFrequency`, `claimElement`.`futureEmployment`.`paymentFrequency`) as claimElement_futureEmployment_paymentFrequency, 
COALESCE(`_removed`.`claimElement`.`futureEmployment`.`startDate`, `claimElement`.`futureEmployment`.`startDate`) as claimElement_futureEmployment_startDate, 
COALESCE(CAST(`_removed`.`claimElement`.`futureEmployment`.`startingEmployment` as STRING), CAST(`claimElement`.`futureEmployment`.`startingEmployment` as STRING)) as claimElement_futureEmployment_startingEmployment, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`type`, `claimElement`.`paymentEffectiveDate`.`type`) as claimElement_paymentEffectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`date`, `claimElement`.`paymentEffectiveDate`.`date`) as claimElement_paymentEffectiveDate_date, 
COALESCE(CAST(`_removed`.`claimElement`.`startingSelfEmployment` as STRING), CAST(`claimElement`.`startingSelfEmployment` as STRING)) as claimElement_startingSelfEmployment, 
COALESCE(`_removed`.`claimElement`.`workHistory`.`dateStoppedWorking`, `claimElement`.`workHistory`.`dateStoppedWorking`) as claimElement_workHistory_dateStoppedWorking, 
COALESCE(CAST(`_removed`.`claimElement`.`workHistory`.`stoppedWorkingInLast9Months` as STRING), CAST(`claimElement`.`workHistory`.`stoppedWorkingInLast9Months` as STRING)) as claimElement_workHistory_stoppedWorkingInLast9Months, 
COALESCE(CAST(`_removed`.`claimElement`.`working` as STRING), CAST(`claimElement`.`working` as STRING)) as claimElement_working, 
COALESCE(`_removed`.`claimElement`.`workingTaxCredit`.`dateStopped`, `claimElement`.`workingTaxCredit`.`dateStopped`) as claimElement_workingTaxCredit_dateStopped, 
COALESCE(CAST(`_removed`.`claimElement`.`workingTaxCredit`.`receivedInLast5Weeks` as STRING), CAST(`claimElement`.`workingTaxCredit`.`receivedInLast5Weeks` as STRING)) as claimElement_workingTaxCredit_receivedInLast5Weeks, 
COALESCE(`_removed`.`claimElement`.`type`, `claimElement`.`type`) as claimElement_type, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`invalidated`.`dateTime`.`d_date`, LENGTH(`_removed`.`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as invalidated_dateTime_d_date, 
COALESCE(`_removed`.`invalidated`.`reason`, `invalidated`.`reason`) as invalidated_reason, 
COALESCE(`_removed`.`_id`.`d_oid`, `_id`.`d_oid`) as id FROM src_accepted_data_workandearningscircumstances_workandearningscircumstances
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
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`acceptedDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`d_oid`:STRING>
,`_version` STRING
,`declarationId` STRING
,`_removed` STRUCT<`claimElement`:STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING>
,`contractId`:STRING
,`declaredDateTime`:STRING>
,`invalidated`:STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`acceptedDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`d_oid`:STRING>
,`_version`:STRING
,`declarationId`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/carersAllowance';

DROP TABLE IF EXISTS carersAllowance;

CREATE TABLE carersallowance STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`acceptedDateTime`.`d_date`, LENGTH(`_removed`.`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`acceptedDateTime`.`d_date`, LENGTH(`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as acceptedDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`contractId`, `claimElement`.`contractId`) as claimElement_contractId, 
COALESCE(`_removed`.`claimElement`.`declaredDateTime`, `claimElement`.`declaredDateTime`) as claimElement_declaredDateTime, 
COALESCE(CAST(`_removed`.`claimElement`.`effectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`effectiveDate`.`hasDate` as STRING)) as claimElement_effectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`knownDate`, `claimElement`.`effectiveDate`.`knownDate`) as claimElement_effectiveDate_knownDate, 
COALESCE(CAST(`_removed`.`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING)) as claimElement_paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`knownDate`, `claimElement`.`paymentEffectiveDate`.`knownDate`) as claimElement_paymentEffectiveDate_knownDate, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`invalidated`.`dateTime`.`d_date`, LENGTH(`_removed`.`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as invalidated_dateTime_d_date, 
COALESCE(`_removed`.`invalidated`.`reason`, `invalidated`.`reason`) as invalidated_reason, 
COALESCE(`_removed`.`_id`.`d_oid`, `_id`.`d_oid`) as id FROM src_accepted_data_carersallowance_carersallowance
 src_accepted_data_carersallowance_carersallowance;


!echo ------------------------;
!echo ------------------------ supportForMortgageInterest;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_supportformortgageinterest_supportformortgageinterest
;

CREATE EXTERNAL TABLE src_accepted_data_supportformortgageinterest_supportformortgageinterest(
`claimElement` STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`numberOfMortgagesAndEligibleLoans`:STRING
,`supportAmount`:STRING
,`type`:STRING>
,`invalidated` STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`acceptedDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`d_oid`:STRING>
,`_version` STRING
,`declarationId` STRING
,`_removed` STRUCT<`claimElement`:STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`numberOfMortgagesAndEligibleLoans`:STRING
,`supportAmount`:STRING
,`type`:STRING>
,`invalidated`:STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`acceptedDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`d_oid`:STRING>
,`_version`:STRING
,`declarationId`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/supportForMortgageInterest';

DROP TABLE IF EXISTS supportForMortgageInterest;

CREATE TABLE supportformortgageinterest STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`acceptedDateTime`.`d_date`, LENGTH(`_removed`.`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`acceptedDateTime`.`d_date`, LENGTH(`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as acceptedDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`contractId`, `claimElement`.`contractId`) as claimElement_contractId, 
COALESCE(`_removed`.`claimElement`.`declaredDateTime`, `claimElement`.`declaredDateTime`) as claimElement_declaredDateTime, 
COALESCE(CAST(`_removed`.`claimElement`.`effectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`effectiveDate`.`hasDate` as STRING)) as claimElement_effectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`knownDate`, `claimElement`.`effectiveDate`.`knownDate`) as claimElement_effectiveDate_knownDate, 
COALESCE(CAST(`_removed`.`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING)) as claimElement_paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`knownDate`, `claimElement`.`paymentEffectiveDate`.`knownDate`) as claimElement_paymentEffectiveDate_knownDate, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`type`, `claimElement`.`effectiveDate`.`type`) as claimElement_effectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`date`, `claimElement`.`effectiveDate`.`date`) as claimElement_effectiveDate_date, 
COALESCE(`_removed`.`claimElement`.`numberOfMortgagesAndEligibleLoans`, `claimElement`.`numberOfMortgagesAndEligibleLoans`) as claimElement_numberOfMortgagesAndEligibleLoans, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`type`, `claimElement`.`paymentEffectiveDate`.`type`) as claimElement_paymentEffectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`date`, `claimElement`.`paymentEffectiveDate`.`date`) as claimElement_paymentEffectiveDate_date, 
COALESCE(`_removed`.`claimElement`.`supportAmount`, `claimElement`.`supportAmount`) as claimElement_supportAmount, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`_removed`.`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as claimElement_declaredDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`type`, `claimElement`.`type`) as claimElement_type, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`invalidated`.`dateTime`.`d_date`, LENGTH(`_removed`.`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as invalidated_dateTime_d_date, 
COALESCE(`_removed`.`invalidated`.`reason`, `invalidated`.`reason`) as invalidated_reason, 
COALESCE(`_removed`.`_id`.`d_oid`, `_id`.`d_oid`) as id FROM src_accepted_data_supportformortgageinterest_supportformortgageinterest
 src_accepted_data_supportformortgageinterest_supportformortgageinterest;


!echo ------------------------;
!echo ------------------------ terminalIllness;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_terminalillness_terminalillness
;

CREATE EXTERNAL TABLE src_accepted_data_terminalillness_terminalillness(
`claimElement` STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`agentPrognosis`:STRING
,`claimantId`:STRING
,`evidenceAccepted`:BOOLEAN
,`type`:STRING>
,`invalidated` STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`acceptedDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`d_oid`:STRING>
,`_version` STRING
,`declarationId` STRING
,`_removed` STRUCT<`claimElement`:STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`agentPrognosis`:STRING
,`claimantId`:STRING
,`evidenceAccepted`:BOOLEAN
,`type`:STRING>
,`invalidated`:STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`acceptedDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`d_oid`:STRING>
,`_version`:STRING
,`declarationId`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/terminalIllness';

DROP TABLE IF EXISTS terminalIllness;

CREATE TABLE terminalillness STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`acceptedDateTime`.`d_date`, LENGTH(`_removed`.`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`acceptedDateTime`.`d_date`, LENGTH(`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as acceptedDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`contractId`, `claimElement`.`contractId`) as claimElement_contractId, 
COALESCE(`_removed`.`claimElement`.`declaredDateTime`, `claimElement`.`declaredDateTime`) as claimElement_declaredDateTime, 
COALESCE(CAST(`_removed`.`claimElement`.`effectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`effectiveDate`.`hasDate` as STRING)) as claimElement_effectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`knownDate`, `claimElement`.`effectiveDate`.`knownDate`) as claimElement_effectiveDate_knownDate, 
COALESCE(CAST(`_removed`.`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING)) as claimElement_paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`knownDate`, `claimElement`.`paymentEffectiveDate`.`knownDate`) as claimElement_paymentEffectiveDate_knownDate, 
COALESCE(`_removed`.`claimElement`.`agentPrognosis`, `claimElement`.`agentPrognosis`) as claimElement_agentPrognosis, 
COALESCE(`_removed`.`claimElement`.`claimantId`, `claimElement`.`claimantId`) as claimElement_claimantId, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`type`, `claimElement`.`effectiveDate`.`type`) as claimElement_effectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`date`, `claimElement`.`effectiveDate`.`date`) as claimElement_effectiveDate_date, 
COALESCE(CAST(`_removed`.`claimElement`.`evidenceAccepted` as STRING), CAST(`claimElement`.`evidenceAccepted` as STRING)) as claimElement_evidenceAccepted, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`type`, `claimElement`.`paymentEffectiveDate`.`type`) as claimElement_paymentEffectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`date`, `claimElement`.`paymentEffectiveDate`.`date`) as claimElement_paymentEffectiveDate_date, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`_removed`.`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as claimElement_declaredDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`type`, `claimElement`.`type`) as claimElement_type, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`invalidated`.`dateTime`.`d_date`, LENGTH(`_removed`.`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as invalidated_dateTime_d_date, 
COALESCE(`_removed`.`invalidated`.`reason`, `invalidated`.`reason`) as invalidated_reason, 
COALESCE(`_removed`.`_id`.`d_oid`, `_id`.`d_oid`) as id FROM src_accepted_data_terminalillness_terminalillness
 src_accepted_data_terminalillness_terminalillness;


!echo ------------------------;
!echo ------------------------ childElementEligibility;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_childelementeligibility_childelementeligibility
;

CREATE EXTERNAL TABLE src_accepted_data_childelementeligibility_childelementeligibility(
`claimElement` STRUCT<`childElementEligibilityDecisions`:ARRAY<STRUCT<`childId`:STRING
,`exceptionType`:STRING
,`fullName`:STRING
,`placementDate`:STRING
,`childDateOfBirth`:STRING
>>
,`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`type`:STRING>
,`invalidated` STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`acceptedDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`d_oid`:STRING>
,`_version` STRING
,`declarationId` STRING
,`_removed` STRUCT<`claimElement`:STRUCT<`childElementEligibilityDecisions`:ARRAY<STRUCT<`childId`:STRING
,`exceptionType`:STRING
,`fullName`:STRING
,`placementDate`:STRING
,`childDateOfBirth`:STRING
>>
,`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`type`:STRING>
,`invalidated`:STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`acceptedDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`d_oid`:STRING>
,`_version`:STRING
,`declarationId`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/childElementEligibility';

DROP TABLE IF EXISTS childElementEligibility;

CREATE TABLE childelementeligibility STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`acceptedDateTime`.`d_date`, LENGTH(`_removed`.`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`acceptedDateTime`.`d_date`, LENGTH(`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as acceptedDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`contractId`, `claimElement`.`contractId`) as claimElement_contractId, 
COALESCE(`_removed`.`claimElement`.`declaredDateTime`, `claimElement`.`declaredDateTime`) as claimElement_declaredDateTime, 
COALESCE(CAST(`_removed`.`claimElement`.`effectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`effectiveDate`.`hasDate` as STRING)) as claimElement_effectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`knownDate`, `claimElement`.`effectiveDate`.`knownDate`) as claimElement_effectiveDate_knownDate, 
COALESCE(CAST(`_removed`.`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING)) as claimElement_paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`knownDate`, `claimElement`.`paymentEffectiveDate`.`knownDate`) as claimElement_paymentEffectiveDate_knownDate, 
COALESCE(`removed_exploded_claimelement_childelementeligibilitydecisions`.`childId`, `exploded_claimelement_childelementeligibilitydecisions`.`childId`) as claimElement_childElementEligibilityDecisions_childId, 
COALESCE(`removed_exploded_claimelement_childelementeligibilitydecisions`.`exceptionType`, `exploded_claimelement_childelementeligibilitydecisions`.`exceptionType`) as claimElement_childElementEligibilityDecisions_exceptionType, 
COALESCE(`removed_exploded_claimelement_childelementeligibilitydecisions`.`fullName`, `exploded_claimelement_childelementeligibilitydecisions`.`fullName`) as claimElement_childElementEligibilityDecisions_fullName, 
COALESCE(`removed_exploded_claimelement_childelementeligibilitydecisions`.`placementDate`, `exploded_claimelement_childelementeligibilitydecisions`.`placementDate`) as claimElement_childElementEligibilityDecisions_placementDate, 
COALESCE(`removed_exploded_claimelement_childelementeligibilitydecisions`.`childDateOfBirth`, `exploded_claimelement_childelementeligibilitydecisions`.`childDateOfBirth`) as claimElement_childElementEligibilityDecisions_childDateOfBirth, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`type`, `claimElement`.`effectiveDate`.`type`) as claimElement_effectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`date`, `claimElement`.`effectiveDate`.`date`) as claimElement_effectiveDate_date, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`type`, `claimElement`.`paymentEffectiveDate`.`type`) as claimElement_paymentEffectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`date`, `claimElement`.`paymentEffectiveDate`.`date`) as claimElement_paymentEffectiveDate_date, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`_removed`.`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as claimElement_declaredDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`type`, `claimElement`.`type`) as claimElement_type, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`invalidated`.`dateTime`.`d_date`, LENGTH(`_removed`.`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as invalidated_dateTime_d_date, 
COALESCE(`_removed`.`invalidated`.`reason`, `invalidated`.`reason`) as invalidated_reason, 
COALESCE(`_removed`.`_id`.`d_oid`, `_id`.`d_oid`) as id FROM src_accepted_data_childelementeligibility_childelementeligibility
 LATERAL VIEW OUTER EXPLODE(`claimElement`.`childElementEligibilityDecisions`) view_exploded_claimelement_childelementeligibilitydecisions AS exploded_claimelement_childelementeligibilitydecisions 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`claimElement`.`childElementEligibilityDecisions`) view_removed_exploded_claimelement_childelementeligibilitydecisions AS removed_exploded_claimelement_childelementeligibilitydecisions 
;


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
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`acceptedDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`d_oid`:STRING>
,`_version` STRING
,`declarationId` STRING
,`_removed` STRUCT<`claimElement`:STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING>
,`contractId`:STRING
,`declaredDateTime`:STRING>
,`invalidated`:STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`acceptedDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`d_oid`:STRING>
,`_version`:STRING
,`declarationId`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/otherIncome';

DROP TABLE IF EXISTS otherIncome;

CREATE TABLE otherincome STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`acceptedDateTime`.`d_date`, LENGTH(`_removed`.`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`acceptedDateTime`.`d_date`, LENGTH(`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as acceptedDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`contractId`, `claimElement`.`contractId`) as claimElement_contractId, 
COALESCE(`_removed`.`claimElement`.`declaredDateTime`, `claimElement`.`declaredDateTime`) as claimElement_declaredDateTime, 
COALESCE(CAST(`_removed`.`claimElement`.`effectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`effectiveDate`.`hasDate` as STRING)) as claimElement_effectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`knownDate`, `claimElement`.`effectiveDate`.`knownDate`) as claimElement_effectiveDate_knownDate, 
COALESCE(CAST(`_removed`.`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING)) as claimElement_paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`knownDate`, `claimElement`.`paymentEffectiveDate`.`knownDate`) as claimElement_paymentEffectiveDate_knownDate, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`invalidated`.`dateTime`.`d_date`, LENGTH(`_removed`.`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as invalidated_dateTime_d_date, 
COALESCE(`_removed`.`invalidated`.`reason`, `invalidated`.`reason`) as invalidated_reason, 
COALESCE(`_removed`.`_id`.`d_oid`, `_id`.`d_oid`) as id FROM src_accepted_data_otherincome_otherincome
 src_accepted_data_otherincome_otherincome;


!echo ------------------------;
!echo ------------------------ nationality;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_nationality_nationality
;

CREATE EXTERNAL TABLE src_accepted_data_nationality_nationality(
`claimElement` STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`citizenId`:STRING
,`returnAfterAbsence`:BOOLEAN
,`ukNational`:BOOLEAN
,`claimantId`:STRING
,`type`:STRING>
,`invalidated` STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`acceptedDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`d_oid`:STRING>
,`_version` STRING
,`declarationId` STRING
,`_removed` STRUCT<`claimElement`:STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`citizenId`:STRING
,`returnAfterAbsence`:BOOLEAN
,`ukNational`:BOOLEAN
,`claimantId`:STRING
,`type`:STRING>
,`invalidated`:STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`acceptedDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`d_oid`:STRING>
,`_version`:STRING
,`declarationId`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/nationality';

DROP TABLE IF EXISTS nationality;

CREATE TABLE nationality STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`acceptedDateTime`.`d_date`, LENGTH(`_removed`.`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`acceptedDateTime`.`d_date`, LENGTH(`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as acceptedDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`contractId`, `claimElement`.`contractId`) as claimElement_contractId, 
COALESCE(`_removed`.`claimElement`.`declaredDateTime`, `claimElement`.`declaredDateTime`) as claimElement_declaredDateTime, 
COALESCE(CAST(`_removed`.`claimElement`.`effectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`effectiveDate`.`hasDate` as STRING)) as claimElement_effectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`knownDate`, `claimElement`.`effectiveDate`.`knownDate`) as claimElement_effectiveDate_knownDate, 
COALESCE(CAST(`_removed`.`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING)) as claimElement_paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`knownDate`, `claimElement`.`paymentEffectiveDate`.`knownDate`) as claimElement_paymentEffectiveDate_knownDate, 
COALESCE(`_removed`.`claimElement`.`citizenId`, `claimElement`.`citizenId`) as claimElement_citizenId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`_removed`.`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as claimElement_declaredDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`type`, `claimElement`.`effectiveDate`.`type`) as claimElement_effectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`date`, `claimElement`.`effectiveDate`.`date`) as claimElement_effectiveDate_date, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`type`, `claimElement`.`paymentEffectiveDate`.`type`) as claimElement_paymentEffectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`date`, `claimElement`.`paymentEffectiveDate`.`date`) as claimElement_paymentEffectiveDate_date, 
COALESCE(CAST(`_removed`.`claimElement`.`returnAfterAbsence` as STRING), CAST(`claimElement`.`returnAfterAbsence` as STRING)) as claimElement_returnAfterAbsence, 
COALESCE(CAST(`_removed`.`claimElement`.`ukNational` as STRING), CAST(`claimElement`.`ukNational` as STRING)) as claimElement_ukNational, 
COALESCE(`_removed`.`claimElement`.`claimantId`, `claimElement`.`claimantId`) as claimElement_claimantId, 
COALESCE(`_removed`.`claimElement`.`type`, `claimElement`.`type`) as claimElement_type, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`invalidated`.`dateTime`.`d_date`, LENGTH(`_removed`.`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as invalidated_dateTime_d_date, 
COALESCE(`_removed`.`invalidated`.`reason`, `invalidated`.`reason`) as invalidated_reason, 
COALESCE(`_removed`.`_id`.`d_oid`, `_id`.`d_oid`) as id FROM src_accepted_data_nationality_nationality
 src_accepted_data_nationality_nationality;


!echo ------------------------;
!echo ------------------------ carerCircumstances;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_carercircumstances_carercircumstances
;

CREATE EXTERNAL TABLE src_accepted_data_carercircumstances_carercircumstances(
`claimElement` STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`careeCircumstances`:STRUCT<`addressNumber`:STRING
,`dateOfBirth`:STRING
,`firstName`:STRING
,`lastName`:STRING
,`middleName`:STRING
,`postcode`:STRING
,`relationship`:STRING
,`townCity`:STRING>
,`caringCircumstances`:STRUCT<`caringForSomeone`:BOOLEAN
,`personReceivesBenefit`:BOOLEAN>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`otherCaringResponsibilities`:STRUCT<`caringForSomeoneElse`:BOOLEAN
,`personReceivesBenefit`:BOOLEAN>
,`stoppedCaringCircumstances`:STRUCT<`dueToDeath`:BOOLEAN
,`stoppedDate`:STRING>
,`timeOffCircumstances`:STRUCT<`endDate`:STRING
,`startDate`:STRING>
,`contractId`:STRING
,`claimantId`:STRING
,`moreThan35Hours`:BOOLEAN
,`type`:STRING>
,`invalidated` STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`acceptedDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`d_oid`:STRING>
,`_version` STRING
,`declarationId` STRING
,`_removed` STRUCT<`claimElement`:STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`careeCircumstances`:STRUCT<`addressNumber`:STRING
,`dateOfBirth`:STRING
,`firstName`:STRING
,`lastName`:STRING
,`middleName`:STRING
,`postcode`:STRING
,`relationship`:STRING
,`townCity`:STRING>
,`caringCircumstances`:STRUCT<`caringForSomeone`:BOOLEAN
,`personReceivesBenefit`:BOOLEAN>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`otherCaringResponsibilities`:STRUCT<`caringForSomeoneElse`:BOOLEAN
,`personReceivesBenefit`:BOOLEAN>
,`stoppedCaringCircumstances`:STRUCT<`dueToDeath`:BOOLEAN
,`stoppedDate`:STRING>
,`timeOffCircumstances`:STRUCT<`endDate`:STRING
,`startDate`:STRING>
,`contractId`:STRING
,`claimantId`:STRING
,`moreThan35Hours`:BOOLEAN
,`type`:STRING>
,`invalidated`:STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`acceptedDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`d_oid`:STRING>
,`_version`:STRING
,`declarationId`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/carerCircumstances';

DROP TABLE IF EXISTS carerCircumstances;

CREATE TABLE carercircumstances STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`acceptedDateTime`.`d_date`, LENGTH(`_removed`.`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`acceptedDateTime`.`d_date`, LENGTH(`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as acceptedDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`contractId`, `claimElement`.`contractId`) as claimElement_contractId, 
COALESCE(`_removed`.`claimElement`.`declaredDateTime`, `claimElement`.`declaredDateTime`) as claimElement_declaredDateTime, 
COALESCE(CAST(`_removed`.`claimElement`.`effectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`effectiveDate`.`hasDate` as STRING)) as claimElement_effectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`knownDate`, `claimElement`.`effectiveDate`.`knownDate`) as claimElement_effectiveDate_knownDate, 
COALESCE(CAST(`_removed`.`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING)) as claimElement_paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`knownDate`, `claimElement`.`paymentEffectiveDate`.`knownDate`) as claimElement_paymentEffectiveDate_knownDate, 
COALESCE(`_removed`.`claimElement`.`careeCircumstances`.`addressNumber`, `claimElement`.`careeCircumstances`.`addressNumber`) as claimElement_careeCircumstances_addressNumber, 
COALESCE(`_removed`.`claimElement`.`careeCircumstances`.`dateOfBirth`, `claimElement`.`careeCircumstances`.`dateOfBirth`) as claimElement_careeCircumstances_dateOfBirth, 
COALESCE(`_removed`.`claimElement`.`careeCircumstances`.`firstName`, `claimElement`.`careeCircumstances`.`firstName`) as claimElement_careeCircumstances_firstName, 
COALESCE(`_removed`.`claimElement`.`careeCircumstances`.`lastName`, `claimElement`.`careeCircumstances`.`lastName`) as claimElement_careeCircumstances_lastName, 
COALESCE(`_removed`.`claimElement`.`careeCircumstances`.`middleName`, `claimElement`.`careeCircumstances`.`middleName`) as claimElement_careeCircumstances_middleName, 
COALESCE(`_removed`.`claimElement`.`careeCircumstances`.`postcode`, `claimElement`.`careeCircumstances`.`postcode`) as claimElement_careeCircumstances_postcode, 
COALESCE(`_removed`.`claimElement`.`careeCircumstances`.`relationship`, `claimElement`.`careeCircumstances`.`relationship`) as claimElement_careeCircumstances_relationship, 
COALESCE(`_removed`.`claimElement`.`careeCircumstances`.`townCity`, `claimElement`.`careeCircumstances`.`townCity`) as claimElement_careeCircumstances_townCity, 
COALESCE(CAST(`_removed`.`claimElement`.`caringCircumstances`.`caringForSomeone` as STRING), CAST(`claimElement`.`caringCircumstances`.`caringForSomeone` as STRING)) as claimElement_caringCircumstances_caringForSomeone, 
COALESCE(CAST(`_removed`.`claimElement`.`caringCircumstances`.`personReceivesBenefit` as STRING), CAST(`claimElement`.`caringCircumstances`.`personReceivesBenefit` as STRING)) as claimElement_caringCircumstances_personReceivesBenefit, 
COALESCE(`_removed`.`claimElement`.`claimantId`, `claimElement`.`claimantId`) as claimElement_claimantId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`_removed`.`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as claimElement_declaredDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`type`, `claimElement`.`effectiveDate`.`type`) as claimElement_effectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`date`, `claimElement`.`effectiveDate`.`date`) as claimElement_effectiveDate_date, 
COALESCE(CAST(`_removed`.`claimElement`.`moreThan35Hours` as STRING), CAST(`claimElement`.`moreThan35Hours` as STRING)) as claimElement_moreThan35Hours, 
COALESCE(CAST(`_removed`.`claimElement`.`otherCaringResponsibilities`.`caringForSomeoneElse` as STRING), CAST(`claimElement`.`otherCaringResponsibilities`.`caringForSomeoneElse` as STRING)) as claimElement_otherCaringResponsibilities_caringForSomeoneElse, 
COALESCE(CAST(`_removed`.`claimElement`.`otherCaringResponsibilities`.`personReceivesBenefit` as STRING), CAST(`claimElement`.`otherCaringResponsibilities`.`personReceivesBenefit` as STRING)) as claimElement_otherCaringResponsibilities_personReceivesBenefit, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`type`, `claimElement`.`paymentEffectiveDate`.`type`) as claimElement_paymentEffectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`date`, `claimElement`.`paymentEffectiveDate`.`date`) as claimElement_paymentEffectiveDate_date, 
COALESCE(CAST(`_removed`.`claimElement`.`stoppedCaringCircumstances`.`dueToDeath` as STRING), CAST(`claimElement`.`stoppedCaringCircumstances`.`dueToDeath` as STRING)) as claimElement_stoppedCaringCircumstances_dueToDeath, 
COALESCE(`_removed`.`claimElement`.`stoppedCaringCircumstances`.`stoppedDate`, `claimElement`.`stoppedCaringCircumstances`.`stoppedDate`) as claimElement_stoppedCaringCircumstances_stoppedDate, 
COALESCE(`_removed`.`claimElement`.`timeOffCircumstances`.`endDate`, `claimElement`.`timeOffCircumstances`.`endDate`) as claimElement_timeOffCircumstances_endDate, 
COALESCE(`_removed`.`claimElement`.`timeOffCircumstances`.`startDate`, `claimElement`.`timeOffCircumstances`.`startDate`) as claimElement_timeOffCircumstances_startDate, 
COALESCE(`_removed`.`claimElement`.`type`, `claimElement`.`type`) as claimElement_type, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`invalidated`.`dateTime`.`d_date`, LENGTH(`_removed`.`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as invalidated_dateTime_d_date, 
COALESCE(`_removed`.`invalidated`.`reason`, `invalidated`.`reason`) as invalidated_reason, 
COALESCE(`_removed`.`_id`.`d_oid`, `_id`.`d_oid`) as id FROM src_accepted_data_carercircumstances_carercircumstances
 src_accepted_data_carercircumstances_carercircumstances;


!echo ------------------------;
!echo ------------------------ previousEarnings;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_previousearnings_previousearnings
;

CREATE EXTERNAL TABLE src_accepted_data_previousearnings_previousearnings(
`claimElement` STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`autoDeclaredDueToExpiry`:BOOLEAN
,`claimantId`:STRING
,`dateStoppedWorking`:STRING
,`dropInEarningsApId`:STRING
,`type`:STRING>
,`invalidated` STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`acceptedDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`d_oid`:STRING>
,`_version` STRING
,`declarationId` STRING
,`_removed` STRUCT<`claimElement`:STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`autoDeclaredDueToExpiry`:BOOLEAN
,`claimantId`:STRING
,`dateStoppedWorking`:STRING
,`dropInEarningsApId`:STRING
,`type`:STRING>
,`invalidated`:STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`acceptedDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`d_oid`:STRING>
,`_version`:STRING
,`declarationId`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/previousEarnings';

DROP TABLE IF EXISTS previousEarnings;

CREATE TABLE previousearnings STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`acceptedDateTime`.`d_date`, LENGTH(`_removed`.`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`acceptedDateTime`.`d_date`, LENGTH(`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as acceptedDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`contractId`, `claimElement`.`contractId`) as claimElement_contractId, 
COALESCE(`_removed`.`claimElement`.`declaredDateTime`, `claimElement`.`declaredDateTime`) as claimElement_declaredDateTime, 
COALESCE(CAST(`_removed`.`claimElement`.`effectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`effectiveDate`.`hasDate` as STRING)) as claimElement_effectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`knownDate`, `claimElement`.`effectiveDate`.`knownDate`) as claimElement_effectiveDate_knownDate, 
COALESCE(CAST(`_removed`.`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING)) as claimElement_paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`knownDate`, `claimElement`.`paymentEffectiveDate`.`knownDate`) as claimElement_paymentEffectiveDate_knownDate, 
COALESCE(CAST(`_removed`.`claimElement`.`autoDeclaredDueToExpiry` as STRING), CAST(`claimElement`.`autoDeclaredDueToExpiry` as STRING)) as claimElement_autoDeclaredDueToExpiry, 
COALESCE(`_removed`.`claimElement`.`claimantId`, `claimElement`.`claimantId`) as claimElement_claimantId, 
COALESCE(`_removed`.`claimElement`.`dateStoppedWorking`, `claimElement`.`dateStoppedWorking`) as claimElement_dateStoppedWorking, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`_removed`.`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as claimElement_declaredDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`dropInEarningsApId`, `claimElement`.`dropInEarningsApId`) as claimElement_dropInEarningsApId, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`type`, `claimElement`.`effectiveDate`.`type`) as claimElement_effectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`date`, `claimElement`.`effectiveDate`.`date`) as claimElement_effectiveDate_date, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`type`, `claimElement`.`paymentEffectiveDate`.`type`) as claimElement_paymentEffectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`date`, `claimElement`.`paymentEffectiveDate`.`date`) as claimElement_paymentEffectiveDate_date, 
COALESCE(`_removed`.`claimElement`.`type`, `claimElement`.`type`) as claimElement_type, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`invalidated`.`dateTime`.`d_date`, LENGTH(`_removed`.`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as invalidated_dateTime_d_date, 
COALESCE(`_removed`.`invalidated`.`reason`, `invalidated`.`reason`) as invalidated_reason, 
COALESCE(`_removed`.`_id`.`d_oid`, `_id`.`d_oid`) as id FROM src_accepted_data_previousearnings_previousearnings
 src_accepted_data_previousearnings_previousearnings;


!echo ------------------------;
!echo ------------------------ childcare;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_childcare_childcare
;

CREATE EXTERNAL TABLE src_accepted_data_childcare_childcare(
`claimElement` STRUCT<`declaredChildcareProviders`:ARRAY<STRUCT<`childcareCosts`:ARRAY<STRUCT<`children`:ARRAY<STRUCT<`ageInYears`:STRING
,`childId`:STRING
,`firstName`:STRING
,`gender`:STRING
,`lastName`:STRING
>>
,`amountPaid`:STRING
,`dateDeclared`:STRING
,`datePaid`:STRING
,`dateVerified`:STRING
,`id`:STRING
,`periodEnd`:STRING
,`periodStart`:STRING
,`providerId`:STRING
>>
,`childcareProvider`:STRUCT<`address1`:STRING
,`address2`:STRING
,`id`:STRING
,`phoneNumber`:STRING
,`postcode`:STRING
,`providerName`:STRING
,`registrationNumber`:STRING
,`town`:STRING>
>>
,`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`hasChildcareCosts`:BOOLEAN
,`type`:STRING>
,`invalidated` STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`acceptedDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`d_oid`:STRING>
,`_version` STRING
,`declarationId` STRING
,`_removed` STRUCT<`claimElement`:STRUCT<`declaredChildcareProviders`:ARRAY<STRUCT<`childcareCosts`:ARRAY<STRUCT<`children`:ARRAY<STRUCT<`ageInYears`:STRING
,`childId`:STRING
,`firstName`:STRING
,`gender`:STRING
,`lastName`:STRING
>>
,`amountPaid`:STRING
,`dateDeclared`:STRING
,`datePaid`:STRING
,`dateVerified`:STRING
,`id`:STRING
,`periodEnd`:STRING
,`periodStart`:STRING
,`providerId`:STRING
>>
,`childcareProvider`:STRUCT<`address1`:STRING
,`address2`:STRING
,`id`:STRING
,`phoneNumber`:STRING
,`postcode`:STRING
,`providerName`:STRING
,`registrationNumber`:STRING
,`town`:STRING>
>>
,`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`hasChildcareCosts`:BOOLEAN
,`type`:STRING>
,`invalidated`:STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`acceptedDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`d_oid`:STRING>
,`_version`:STRING
,`declarationId`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/childcare';

DROP TABLE IF EXISTS childcare;

CREATE TABLE childcare STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`acceptedDateTime`.`d_date`, LENGTH(`_removed`.`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`acceptedDateTime`.`d_date`, LENGTH(`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as acceptedDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`contractId`, `claimElement`.`contractId`) as claimElement_contractId, 
COALESCE(`_removed`.`claimElement`.`declaredDateTime`, `claimElement`.`declaredDateTime`) as claimElement_declaredDateTime, 
COALESCE(CAST(`_removed`.`claimElement`.`effectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`effectiveDate`.`hasDate` as STRING)) as claimElement_effectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`knownDate`, `claimElement`.`effectiveDate`.`knownDate`) as claimElement_effectiveDate_knownDate, 
COALESCE(CAST(`_removed`.`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING)) as claimElement_paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`knownDate`, `claimElement`.`paymentEffectiveDate`.`knownDate`) as claimElement_paymentEffectiveDate_knownDate, 
COALESCE(`removed_exploded_claimelement_declaredchildcareproviders_exploded_childcarecosts`.`amountPaid`, `exploded_claimelement_declaredchildcareproviders_exploded_childcarecosts`.`amountPaid`) as claimElement_declaredChildcareProviders_childcareCosts_amountPaid, 
COALESCE(`removed_exploded_claimelement_declaredchildcareproviders_exploded_childcarecosts_exploded_children`.`ageInYears`, `exploded_claimelement_declaredchildcareproviders_exploded_childcarecosts_exploded_children`.`ageInYears`) as claimElement_declaredChildcareProviders_childcareCosts_children_ageInYears, 
COALESCE(`removed_exploded_claimelement_declaredchildcareproviders_exploded_childcarecosts_exploded_children`.`childId`, `exploded_claimelement_declaredchildcareproviders_exploded_childcarecosts_exploded_children`.`childId`) as claimElement_declaredChildcareProviders_childcareCosts_children_childId, 
COALESCE(`removed_exploded_claimelement_declaredchildcareproviders_exploded_childcarecosts_exploded_children`.`firstName`, `exploded_claimelement_declaredchildcareproviders_exploded_childcarecosts_exploded_children`.`firstName`) as claimElement_declaredChildcareProviders_childcareCosts_children_firstName, 
COALESCE(`removed_exploded_claimelement_declaredchildcareproviders_exploded_childcarecosts_exploded_children`.`gender`, `exploded_claimelement_declaredchildcareproviders_exploded_childcarecosts_exploded_children`.`gender`) as claimElement_declaredChildcareProviders_childcareCosts_children_gender, 
COALESCE(`removed_exploded_claimelement_declaredchildcareproviders_exploded_childcarecosts_exploded_children`.`lastName`, `exploded_claimelement_declaredchildcareproviders_exploded_childcarecosts_exploded_children`.`lastName`) as claimElement_declaredChildcareProviders_childcareCosts_children_lastName, 
COALESCE(`removed_exploded_claimelement_declaredchildcareproviders_exploded_childcarecosts`.`dateDeclared`, `exploded_claimelement_declaredchildcareproviders_exploded_childcarecosts`.`dateDeclared`) as claimElement_declaredChildcareProviders_childcareCosts_dateDeclared, 
COALESCE(`removed_exploded_claimelement_declaredchildcareproviders_exploded_childcarecosts`.`datePaid`, `exploded_claimelement_declaredchildcareproviders_exploded_childcarecosts`.`datePaid`) as claimElement_declaredChildcareProviders_childcareCosts_datePaid, 
COALESCE(`removed_exploded_claimelement_declaredchildcareproviders_exploded_childcarecosts`.`dateVerified`, `exploded_claimelement_declaredchildcareproviders_exploded_childcarecosts`.`dateVerified`) as claimElement_declaredChildcareProviders_childcareCosts_dateVerified, 
COALESCE(`removed_exploded_claimelement_declaredchildcareproviders_exploded_childcarecosts`.`id`, `exploded_claimelement_declaredchildcareproviders_exploded_childcarecosts`.`id`) as claimElement_declaredChildcareProviders_childcareCosts_id, 
COALESCE(`removed_exploded_claimelement_declaredchildcareproviders_exploded_childcarecosts`.`periodEnd`, `exploded_claimelement_declaredchildcareproviders_exploded_childcarecosts`.`periodEnd`) as claimElement_declaredChildcareProviders_childcareCosts_periodEnd, 
COALESCE(`removed_exploded_claimelement_declaredchildcareproviders_exploded_childcarecosts`.`periodStart`, `exploded_claimelement_declaredchildcareproviders_exploded_childcarecosts`.`periodStart`) as claimElement_declaredChildcareProviders_childcareCosts_periodStart, 
COALESCE(`removed_exploded_claimelement_declaredchildcareproviders_exploded_childcarecosts`.`providerId`, `exploded_claimelement_declaredchildcareproviders_exploded_childcarecosts`.`providerId`) as claimElement_declaredChildcareProviders_childcareCosts_providerId, 
COALESCE(`removed_exploded_claimelement_declaredchildcareproviders`.`childcareProvider`.`address1`, `exploded_claimelement_declaredchildcareproviders`.`childcareProvider`.`address1`) as claimElement_declaredChildcareProviders_childcareProvider_address1, 
COALESCE(`removed_exploded_claimelement_declaredchildcareproviders`.`childcareProvider`.`address2`, `exploded_claimelement_declaredchildcareproviders`.`childcareProvider`.`address2`) as claimElement_declaredChildcareProviders_childcareProvider_address2, 
COALESCE(`removed_exploded_claimelement_declaredchildcareproviders`.`childcareProvider`.`id`, `exploded_claimelement_declaredchildcareproviders`.`childcareProvider`.`id`) as claimElement_declaredChildcareProviders_childcareProvider_id, 
COALESCE(`removed_exploded_claimelement_declaredchildcareproviders`.`childcareProvider`.`phoneNumber`, `exploded_claimelement_declaredchildcareproviders`.`childcareProvider`.`phoneNumber`) as claimElement_declaredChildcareProviders_childcareProvider_phoneNumber, 
COALESCE(`removed_exploded_claimelement_declaredchildcareproviders`.`childcareProvider`.`postcode`, `exploded_claimelement_declaredchildcareproviders`.`childcareProvider`.`postcode`) as claimElement_declaredChildcareProviders_childcareProvider_postcode, 
COALESCE(`removed_exploded_claimelement_declaredchildcareproviders`.`childcareProvider`.`providerName`, `exploded_claimelement_declaredchildcareproviders`.`childcareProvider`.`providerName`) as claimElement_declaredChildcareProviders_childcareProvider_providerName, 
COALESCE(`removed_exploded_claimelement_declaredchildcareproviders`.`childcareProvider`.`registrationNumber`, `exploded_claimelement_declaredchildcareproviders`.`childcareProvider`.`registrationNumber`) as claimElement_declaredChildcareProviders_childcareProvider_registrationNumber, 
COALESCE(`removed_exploded_claimelement_declaredchildcareproviders`.`childcareProvider`.`town`, `exploded_claimelement_declaredchildcareproviders`.`childcareProvider`.`town`) as claimElement_declaredChildcareProviders_childcareProvider_town, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`_removed`.`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as claimElement_declaredDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`type`, `claimElement`.`effectiveDate`.`type`) as claimElement_effectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`date`, `claimElement`.`effectiveDate`.`date`) as claimElement_effectiveDate_date, 
COALESCE(CAST(`_removed`.`claimElement`.`hasChildcareCosts` as STRING), CAST(`claimElement`.`hasChildcareCosts` as STRING)) as claimElement_hasChildcareCosts, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`type`, `claimElement`.`paymentEffectiveDate`.`type`) as claimElement_paymentEffectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`date`, `claimElement`.`paymentEffectiveDate`.`date`) as claimElement_paymentEffectiveDate_date, 
COALESCE(`_removed`.`claimElement`.`type`, `claimElement`.`type`) as claimElement_type, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`invalidated`.`dateTime`.`d_date`, LENGTH(`_removed`.`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as invalidated_dateTime_d_date, 
COALESCE(`_removed`.`invalidated`.`reason`, `invalidated`.`reason`) as invalidated_reason, 
COALESCE(`_removed`.`_id`.`d_oid`, `_id`.`d_oid`) as id FROM src_accepted_data_childcare_childcare
 LATERAL VIEW OUTER EXPLODE(`claimElement`.`declaredChildcareProviders`) view_exploded_claimelement_declaredchildcareproviders AS exploded_claimelement_declaredchildcareproviders 
 LATERAL VIEW OUTER EXPLODE(`exploded_claimelement_declaredchildcareproviders`.`childcareCosts`) view_exploded_claimelement_declaredchildcareproviders_exploded_childcarecosts AS exploded_claimelement_declaredchildcareproviders_exploded_childcarecosts 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`claimElement`.`declaredChildcareProviders`) view_removed_exploded_claimelement_declaredchildcareproviders AS removed_exploded_claimelement_declaredchildcareproviders 
 LATERAL VIEW OUTER EXPLODE(`removed_exploded_claimelement_declaredchildcareproviders`.`childcareCosts`) view_removed_exploded_claimelement_declaredchildcareproviders_exploded_childcarecosts AS removed_exploded_claimelement_declaredchildcareproviders_exploded_childcarecosts 
 LATERAL VIEW OUTER EXPLODE(`exploded_claimelement_declaredchildcareproviders_exploded_childcarecosts`.`children`) view_exploded_claimelement_declaredchildcareproviders_exploded_childcarecosts_exploded_children AS exploded_claimelement_declaredchildcareproviders_exploded_childcarecosts_exploded_children 
 LATERAL VIEW OUTER EXPLODE(`removed_exploded_claimelement_declaredchildcareproviders_exploded_childcarecosts`.`children`) view_removed_exploded_claimelement_declaredchildcareproviders_exploded_childcarecosts_exploded_children AS removed_exploded_claimelement_declaredchildcareproviders_exploded_childcarecosts_exploded_children 
;


!echo ------------------------;
!echo ------------------------ educationCircumstances;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_educationcircumstances_educationcircumstances
;

CREATE EXTERNAL TABLE src_accepted_data_educationcircumstances_educationcircumstances(
`claimElement` STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`claimantId`:STRING
,`fullTimeEducation`:BOOLEAN
,`type`:STRING>
,`invalidated` STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`acceptedDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`d_oid`:STRING>
,`_version` STRING
,`declarationId` STRING
,`_removed` STRUCT<`claimElement`:STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`claimantId`:STRING
,`fullTimeEducation`:BOOLEAN
,`type`:STRING>
,`invalidated`:STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`acceptedDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`d_oid`:STRING>
,`_version`:STRING
,`declarationId`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/educationCircumstances';

DROP TABLE IF EXISTS educationCircumstances;

CREATE TABLE educationcircumstances STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`acceptedDateTime`.`d_date`, LENGTH(`_removed`.`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`acceptedDateTime`.`d_date`, LENGTH(`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as acceptedDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`contractId`, `claimElement`.`contractId`) as claimElement_contractId, 
COALESCE(`_removed`.`claimElement`.`declaredDateTime`, `claimElement`.`declaredDateTime`) as claimElement_declaredDateTime, 
COALESCE(CAST(`_removed`.`claimElement`.`effectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`effectiveDate`.`hasDate` as STRING)) as claimElement_effectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`knownDate`, `claimElement`.`effectiveDate`.`knownDate`) as claimElement_effectiveDate_knownDate, 
COALESCE(CAST(`_removed`.`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING)) as claimElement_paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`knownDate`, `claimElement`.`paymentEffectiveDate`.`knownDate`) as claimElement_paymentEffectiveDate_knownDate, 
COALESCE(`_removed`.`claimElement`.`claimantId`, `claimElement`.`claimantId`) as claimElement_claimantId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`_removed`.`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as claimElement_declaredDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`type`, `claimElement`.`effectiveDate`.`type`) as claimElement_effectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`date`, `claimElement`.`effectiveDate`.`date`) as claimElement_effectiveDate_date, 
COALESCE(CAST(`_removed`.`claimElement`.`fullTimeEducation` as STRING), CAST(`claimElement`.`fullTimeEducation` as STRING)) as claimElement_fullTimeEducation, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`type`, `claimElement`.`paymentEffectiveDate`.`type`) as claimElement_paymentEffectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`date`, `claimElement`.`paymentEffectiveDate`.`date`) as claimElement_paymentEffectiveDate_date, 
COALESCE(`_removed`.`claimElement`.`type`, `claimElement`.`type`) as claimElement_type, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`invalidated`.`dateTime`.`d_date`, LENGTH(`_removed`.`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as invalidated_dateTime_d_date, 
COALESCE(`_removed`.`invalidated`.`reason`, `invalidated`.`reason`) as invalidated_reason, 
COALESCE(`_removed`.`_id`.`d_oid`, `_id`.`d_oid`) as id FROM src_accepted_data_educationcircumstances_educationcircumstances
 src_accepted_data_educationcircumstances_educationcircumstances;


!echo ------------------------;
!echo ------------------------ partnerQuestion;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_partnerquestion_partnerquestion
;

CREATE EXTERNAL TABLE src_accepted_data_partnerquestion_partnerquestion(
`claimElement` STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`claimType`:STRING
,`claimantId`:STRING
,`type`:STRING>
,`invalidated` STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`acceptedDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`d_oid`:STRING>
,`_version` STRING
,`declarationId` STRING
,`_removed` STRUCT<`claimElement`:STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`claimType`:STRING
,`claimantId`:STRING
,`type`:STRING>
,`invalidated`:STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`acceptedDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`d_oid`:STRING>
,`_version`:STRING
,`declarationId`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/partnerQuestion';

DROP TABLE IF EXISTS partnerQuestion;

CREATE TABLE partnerquestion STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`acceptedDateTime`.`d_date`, LENGTH(`_removed`.`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`acceptedDateTime`.`d_date`, LENGTH(`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as acceptedDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`contractId`, `claimElement`.`contractId`) as claimElement_contractId, 
COALESCE(`_removed`.`claimElement`.`declaredDateTime`, `claimElement`.`declaredDateTime`) as claimElement_declaredDateTime, 
COALESCE(CAST(`_removed`.`claimElement`.`effectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`effectiveDate`.`hasDate` as STRING)) as claimElement_effectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`knownDate`, `claimElement`.`effectiveDate`.`knownDate`) as claimElement_effectiveDate_knownDate, 
COALESCE(CAST(`_removed`.`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING)) as claimElement_paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`knownDate`, `claimElement`.`paymentEffectiveDate`.`knownDate`) as claimElement_paymentEffectiveDate_knownDate, 
COALESCE(`_removed`.`claimElement`.`claimType`, `claimElement`.`claimType`) as claimElement_claimType, 
COALESCE(`_removed`.`claimElement`.`claimantId`, `claimElement`.`claimantId`) as claimElement_claimantId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`_removed`.`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as claimElement_declaredDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`type`, `claimElement`.`effectiveDate`.`type`) as claimElement_effectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`date`, `claimElement`.`effectiveDate`.`date`) as claimElement_effectiveDate_date, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`type`, `claimElement`.`paymentEffectiveDate`.`type`) as claimElement_paymentEffectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`date`, `claimElement`.`paymentEffectiveDate`.`date`) as claimElement_paymentEffectiveDate_date, 
COALESCE(`_removed`.`claimElement`.`type`, `claimElement`.`type`) as claimElement_type, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`invalidated`.`dateTime`.`d_date`, LENGTH(`_removed`.`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as invalidated_dateTime_d_date, 
COALESCE(`_removed`.`invalidated`.`reason`, `invalidated`.`reason`) as invalidated_reason, 
COALESCE(`_removed`.`_id`.`d_oid`, `_id`.`d_oid`) as id FROM src_accepted_data_partnerquestion_partnerquestion
 src_accepted_data_partnerquestion_partnerquestion;


!echo ------------------------;
!echo ------------------------ housingCircumstances;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_housingcircumstances_housingcircumstances
;

CREATE EXTERNAL TABLE src_accepted_data_housingcircumstances_housingcircumstances(
`claimElement` STRUCT<`privateRentedPropertyDeclarationDetails`:STRUCT<`jointTenancy`:STRUCT<`rent`:STRUCT<`amount`:STRING
,`paymentFrequency`:STRING
,`type`:STRING>
,`serviceCharges`:STRUCT<`amount`:STRING
,`paymentFrequency`:STRING
,`type`:STRING>
,`numberOfTenants`:STRING>
,`rentDetails`:STRUCT<`regularPayment`:STRUCT<`amount`:STRING
,`paymentFrequency`:STRING
,`type`:STRING>
,`startDate`:STRING>
,`additionalRentedHousingInformation`:STRUCT<`exemptFromTheSharedRoomRate`:BOOLEAN
,`nonCommercialRelationshipWithLandlord`:BOOLEAN
,`supportedExemptAccommodation`:BOOLEAN>
,`bedroomAndTenancy`:STRUCT<`bothYouAndPartnerNamedOnTenancyOrRentalAgreement`:BOOLEAN
,`jointTenancyWithSomeoneOtherThanPartner`:BOOLEAN
,`numberOfBedrooms`:STRING>
,`landlordDetails`:STRUCT<`addressNumber`:STRING
,`email`:STRING
,`name`:STRING
,`payRentTo`:STRING
,`phoneNumber`:STRING
,`postcode`:STRING
,`townCity`:STRING>
,`landlordRelationshipDetails`:STRUCT<`anyoneCloseRelative`:BOOLEAN
,`anyoneFinancialInterest`:BOOLEAN
,`liveAtSameProperty`:BOOLEAN>>
,`socialAccommodationDeclarationDetails`:STRUCT<`jointTenancy`:STRUCT<`rent`:STRUCT<`amount`:STRING
,`paymentFrequency`:STRING
,`type`:STRING>
,`serviceCharges`:STRUCT<`amount`:STRING
,`paymentFrequency`:STRING
,`type`:STRING>
,`numberOfTenants`:STRING>
,`rentDetails`:STRUCT<`regularPayment`:STRUCT<`amount`:STRING
,`paymentFrequency`:STRING
,`type`:STRING>
,`startDate`:STRING>
,`socialHousing`:STRUCT<`serviceCharges`:STRUCT<`amount`:STRING
,`paymentFrequency`:STRING
,`type`:STRING>
,`rentFreeWeeks`:STRING>
,`additionalRentedHousingInformation`:STRUCT<`exemptFromTheSharedRoomRate`:BOOLEAN
,`nonCommercialRelationshipWithLandlord`:BOOLEAN
,`supportedExemptAccommodation`:BOOLEAN>
,`bedroomAndTenancy`:STRUCT<`bothYouAndPartnerNamedOnTenancyOrRentalAgreement`:BOOLEAN
,`jointTenancyWithSomeoneOtherThanPartner`:BOOLEAN
,`numberOfBedrooms`:STRING>
,`landlordDetails`:STRUCT<`addressNumber`:STRING
,`email`:STRING
,`name`:STRING
,`payRentTo`:STRING
,`phoneNumber`:STRING
,`postcode`:STRING
,`townCity`:STRING>>
,`ownPropertyDeclarationDetails`:STRUCT<`mortgageDetailsList`:ARRAY<STRUCT<`amount`:STRING
,`forDisabilityAdaptation`:BOOLEAN
,`lender`:STRING
,`lenderCode`:STRING
,`loanReferenceNumber`:STRING
,`mortgageDetailsId`:STRING
>>
,`serviceChargeDetails`:STRUCT<`amount`:STRING
,`frequency`:STRING>
,`haveMortgage`:BOOLEAN
,`payServiceCharges`:BOOLEAN>
,`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`councilTax`:STRUCT<`appliedToCouncilTaxReduction`:STRING
,`councilTaxInClaimantName`:BOOLEAN>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`noApplicableHousingCostsDeclarationDetails`:STRUCT<`noApplicableHousingCostsType`:STRING>
,`otherHousingDeclarationDetails`:STRUCT<`furtherDetails`:STRING
,`otherAccommodationType`:STRING>
,`contractId`:STRING
,`livingDetails`:STRING
,`elementAwardAffectedByNonDependents`:BOOLEAN
,`numberOfBedroomsSignificant`:BOOLEAN
,`type`:STRING>
,`invalidated` STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`acceptedDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`d_oid`:STRING>
,`_version` STRING
,`declarationId` STRING
,`_removed` STRUCT<`claimElement`:STRUCT<`privateRentedPropertyDeclarationDetails`:STRUCT<`jointTenancy`:STRUCT<`rent`:STRUCT<`amount`:STRING
,`paymentFrequency`:STRING
,`type`:STRING>
,`serviceCharges`:STRUCT<`amount`:STRING
,`paymentFrequency`:STRING
,`type`:STRING>
,`numberOfTenants`:STRING>
,`rentDetails`:STRUCT<`regularPayment`:STRUCT<`amount`:STRING
,`paymentFrequency`:STRING
,`type`:STRING>
,`startDate`:STRING>
,`additionalRentedHousingInformation`:STRUCT<`exemptFromTheSharedRoomRate`:BOOLEAN
,`nonCommercialRelationshipWithLandlord`:BOOLEAN
,`supportedExemptAccommodation`:BOOLEAN>
,`bedroomAndTenancy`:STRUCT<`bothYouAndPartnerNamedOnTenancyOrRentalAgreement`:BOOLEAN
,`jointTenancyWithSomeoneOtherThanPartner`:BOOLEAN
,`numberOfBedrooms`:STRING>
,`landlordDetails`:STRUCT<`addressNumber`:STRING
,`email`:STRING
,`name`:STRING
,`payRentTo`:STRING
,`phoneNumber`:STRING
,`postcode`:STRING
,`townCity`:STRING>
,`landlordRelationshipDetails`:STRUCT<`anyoneCloseRelative`:BOOLEAN
,`anyoneFinancialInterest`:BOOLEAN
,`liveAtSameProperty`:BOOLEAN>>
,`socialAccommodationDeclarationDetails`:STRUCT<`jointTenancy`:STRUCT<`rent`:STRUCT<`amount`:STRING
,`paymentFrequency`:STRING
,`type`:STRING>
,`serviceCharges`:STRUCT<`amount`:STRING
,`paymentFrequency`:STRING
,`type`:STRING>
,`numberOfTenants`:STRING>
,`rentDetails`:STRUCT<`regularPayment`:STRUCT<`amount`:STRING
,`paymentFrequency`:STRING
,`type`:STRING>
,`startDate`:STRING>
,`socialHousing`:STRUCT<`serviceCharges`:STRUCT<`amount`:STRING
,`paymentFrequency`:STRING
,`type`:STRING>
,`rentFreeWeeks`:STRING>
,`additionalRentedHousingInformation`:STRUCT<`exemptFromTheSharedRoomRate`:BOOLEAN
,`nonCommercialRelationshipWithLandlord`:BOOLEAN
,`supportedExemptAccommodation`:BOOLEAN>
,`bedroomAndTenancy`:STRUCT<`bothYouAndPartnerNamedOnTenancyOrRentalAgreement`:BOOLEAN
,`jointTenancyWithSomeoneOtherThanPartner`:BOOLEAN
,`numberOfBedrooms`:STRING>
,`landlordDetails`:STRUCT<`addressNumber`:STRING
,`email`:STRING
,`name`:STRING
,`payRentTo`:STRING
,`phoneNumber`:STRING
,`postcode`:STRING
,`townCity`:STRING>>
,`ownPropertyDeclarationDetails`:STRUCT<`mortgageDetailsList`:ARRAY<STRUCT<`amount`:STRING
,`forDisabilityAdaptation`:BOOLEAN
,`lender`:STRING
,`lenderCode`:STRING
,`loanReferenceNumber`:STRING
,`mortgageDetailsId`:STRING
>>
,`serviceChargeDetails`:STRUCT<`amount`:STRING
,`frequency`:STRING>
,`haveMortgage`:BOOLEAN
,`payServiceCharges`:BOOLEAN>
,`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`councilTax`:STRUCT<`appliedToCouncilTaxReduction`:STRING
,`councilTaxInClaimantName`:BOOLEAN>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`noApplicableHousingCostsDeclarationDetails`:STRUCT<`noApplicableHousingCostsType`:STRING>
,`otherHousingDeclarationDetails`:STRUCT<`furtherDetails`:STRING
,`otherAccommodationType`:STRING>
,`contractId`:STRING
,`livingDetails`:STRING
,`elementAwardAffectedByNonDependents`:BOOLEAN
,`numberOfBedroomsSignificant`:BOOLEAN
,`type`:STRING>
,`invalidated`:STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`acceptedDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`d_oid`:STRING>
,`_version`:STRING
,`declarationId`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/housingCircumstances';

DROP TABLE IF EXISTS housingCircumstances;

CREATE TABLE housingcircumstances STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`acceptedDateTime`.`d_date`, LENGTH(`_removed`.`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`acceptedDateTime`.`d_date`, LENGTH(`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as acceptedDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`contractId`, `claimElement`.`contractId`) as claimElement_contractId, 
COALESCE(`_removed`.`claimElement`.`declaredDateTime`, `claimElement`.`declaredDateTime`) as claimElement_declaredDateTime, 
COALESCE(CAST(`_removed`.`claimElement`.`effectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`effectiveDate`.`hasDate` as STRING)) as claimElement_effectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`knownDate`, `claimElement`.`effectiveDate`.`knownDate`) as claimElement_effectiveDate_knownDate, 
COALESCE(CAST(`_removed`.`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING)) as claimElement_paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`knownDate`, `claimElement`.`paymentEffectiveDate`.`knownDate`) as claimElement_paymentEffectiveDate_knownDate, 
COALESCE(`_removed`.`claimElement`.`councilTax`.`appliedToCouncilTaxReduction`, `claimElement`.`councilTax`.`appliedToCouncilTaxReduction`) as claimElement_councilTax_appliedToCouncilTaxReduction, 
COALESCE(CAST(`_removed`.`claimElement`.`councilTax`.`councilTaxInClaimantName` as STRING), CAST(`claimElement`.`councilTax`.`councilTaxInClaimantName` as STRING)) as claimElement_councilTax_councilTaxInClaimantName, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`_removed`.`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as claimElement_declaredDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`type`, `claimElement`.`effectiveDate`.`type`) as claimElement_effectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`date`, `claimElement`.`effectiveDate`.`date`) as claimElement_effectiveDate_date, 
COALESCE(`_removed`.`claimElement`.`livingDetails`, `claimElement`.`livingDetails`) as claimElement_livingDetails, 
COALESCE(`_removed`.`claimElement`.`noApplicableHousingCostsDeclarationDetails`.`noApplicableHousingCostsType`, `claimElement`.`noApplicableHousingCostsDeclarationDetails`.`noApplicableHousingCostsType`) as claimElement_noApplicableHousingCostsDeclarationDetails_noApplicableHousingCostsType, 
COALESCE(`_removed`.`claimElement`.`otherHousingDeclarationDetails`.`furtherDetails`, `claimElement`.`otherHousingDeclarationDetails`.`furtherDetails`) as claimElement_otherHousingDeclarationDetails_furtherDetails, 
COALESCE(`_removed`.`claimElement`.`otherHousingDeclarationDetails`.`otherAccommodationType`, `claimElement`.`otherHousingDeclarationDetails`.`otherAccommodationType`) as claimElement_otherHousingDeclarationDetails_otherAccommodationType, 
COALESCE(CAST(`_removed`.`claimElement`.`ownPropertyDeclarationDetails`.`haveMortgage` as STRING), CAST(`claimElement`.`ownPropertyDeclarationDetails`.`haveMortgage` as STRING)) as claimElement_ownPropertyDeclarationDetails_haveMortgage, 
COALESCE(CAST(`_removed`.`claimElement`.`ownPropertyDeclarationDetails`.`payServiceCharges` as STRING), CAST(`claimElement`.`ownPropertyDeclarationDetails`.`payServiceCharges` as STRING)) as claimElement_ownPropertyDeclarationDetails_payServiceCharges, 
COALESCE(`removed_exploded_claimelement_ownpropertydeclarationdetails_mortgagedetailslist`.`amount`, `exploded_claimelement_ownpropertydeclarationdetails_mortgagedetailslist`.`amount`) as claimElement_ownPropertyDeclarationDetails_mortgageDetailsList_amount, 
COALESCE(CAST(`removed_exploded_claimelement_ownpropertydeclarationdetails_mortgagedetailslist`.`forDisabilityAdaptation` as STRING), CAST(`exploded_claimelement_ownpropertydeclarationdetails_mortgagedetailslist`.`forDisabilityAdaptation` as STRING)) as claimElement_ownPropertyDeclarationDetails_mortgageDetailsList_forDisabilityAdaptation, 
COALESCE(`removed_exploded_claimelement_ownpropertydeclarationdetails_mortgagedetailslist`.`lender`, `exploded_claimelement_ownpropertydeclarationdetails_mortgagedetailslist`.`lender`) as claimElement_ownPropertyDeclarationDetails_mortgageDetailsList_lender, 
COALESCE(`removed_exploded_claimelement_ownpropertydeclarationdetails_mortgagedetailslist`.`lenderCode`, `exploded_claimelement_ownpropertydeclarationdetails_mortgagedetailslist`.`lenderCode`) as claimElement_ownPropertyDeclarationDetails_mortgageDetailsList_lenderCode, 
COALESCE(`removed_exploded_claimelement_ownpropertydeclarationdetails_mortgagedetailslist`.`loanReferenceNumber`, `exploded_claimelement_ownpropertydeclarationdetails_mortgagedetailslist`.`loanReferenceNumber`) as claimElement_ownPropertyDeclarationDetails_mortgageDetailsList_loanReferenceNumber, 
COALESCE(`removed_exploded_claimelement_ownpropertydeclarationdetails_mortgagedetailslist`.`mortgageDetailsId`, `exploded_claimelement_ownpropertydeclarationdetails_mortgagedetailslist`.`mortgageDetailsId`) as claimElement_ownPropertyDeclarationDetails_mortgageDetailsList_mortgageDetailsId, 
COALESCE(`_removed`.`claimElement`.`ownPropertyDeclarationDetails`.`serviceChargeDetails`.`amount`, `claimElement`.`ownPropertyDeclarationDetails`.`serviceChargeDetails`.`amount`) as claimElement_ownPropertyDeclarationDetails_serviceChargeDetails_amount, 
COALESCE(`_removed`.`claimElement`.`ownPropertyDeclarationDetails`.`serviceChargeDetails`.`frequency`, `claimElement`.`ownPropertyDeclarationDetails`.`serviceChargeDetails`.`frequency`) as claimElement_ownPropertyDeclarationDetails_serviceChargeDetails_frequency, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`type`, `claimElement`.`paymentEffectiveDate`.`type`) as claimElement_paymentEffectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`date`, `claimElement`.`paymentEffectiveDate`.`date`) as claimElement_paymentEffectiveDate_date, 
COALESCE(CAST(`_removed`.`claimElement`.`privateRentedPropertyDeclarationDetails`.`additionalRentedHousingInformation`.`exemptFromTheSharedRoomRate` as STRING), CAST(`claimElement`.`privateRentedPropertyDeclarationDetails`.`additionalRentedHousingInformation`.`exemptFromTheSharedRoomRate` as STRING)) as claimElement_privateRentedPropertyDeclarationDetails_additionalRentedHousingInformation_exemptFromTheSharedRoomRate, 
COALESCE(CAST(`_removed`.`claimElement`.`privateRentedPropertyDeclarationDetails`.`additionalRentedHousingInformation`.`nonCommercialRelationshipWithLandlord` as STRING), CAST(`claimElement`.`privateRentedPropertyDeclarationDetails`.`additionalRentedHousingInformation`.`nonCommercialRelationshipWithLandlord` as STRING)) as claimElement_privateRentedPropertyDeclarationDetails_additionalRentedHousingInformation_nonCommercialRelationshipWithLandlord, 
COALESCE(CAST(`_removed`.`claimElement`.`privateRentedPropertyDeclarationDetails`.`additionalRentedHousingInformation`.`supportedExemptAccommodation` as STRING), CAST(`claimElement`.`privateRentedPropertyDeclarationDetails`.`additionalRentedHousingInformation`.`supportedExemptAccommodation` as STRING)) as claimElement_privateRentedPropertyDeclarationDetails_additionalRentedHousingInformation_supportedExemptAccommodation, 
COALESCE(CAST(`_removed`.`claimElement`.`privateRentedPropertyDeclarationDetails`.`bedroomAndTenancy`.`bothYouAndPartnerNamedOnTenancyOrRentalAgreement` as STRING), CAST(`claimElement`.`privateRentedPropertyDeclarationDetails`.`bedroomAndTenancy`.`bothYouAndPartnerNamedOnTenancyOrRentalAgreement` as STRING)) as claimElement_privateRentedPropertyDeclarationDetails_bedroomAndTenancy_bothYouAndPartnerNamedOnTenancyOrRentalAgreement, 
COALESCE(CAST(`_removed`.`claimElement`.`privateRentedPropertyDeclarationDetails`.`bedroomAndTenancy`.`jointTenancyWithSomeoneOtherThanPartner` as STRING), CAST(`claimElement`.`privateRentedPropertyDeclarationDetails`.`bedroomAndTenancy`.`jointTenancyWithSomeoneOtherThanPartner` as STRING)) as claimElement_privateRentedPropertyDeclarationDetails_bedroomAndTenancy_jointTenancyWithSomeoneOtherThanPartner, 
COALESCE(`_removed`.`claimElement`.`privateRentedPropertyDeclarationDetails`.`bedroomAndTenancy`.`numberOfBedrooms`, `claimElement`.`privateRentedPropertyDeclarationDetails`.`bedroomAndTenancy`.`numberOfBedrooms`) as claimElement_privateRentedPropertyDeclarationDetails_bedroomAndTenancy_numberOfBedrooms, 
COALESCE(`_removed`.`claimElement`.`privateRentedPropertyDeclarationDetails`.`jointTenancy`.`numberOfTenants`, `claimElement`.`privateRentedPropertyDeclarationDetails`.`jointTenancy`.`numberOfTenants`) as claimElement_privateRentedPropertyDeclarationDetails_jointTenancy_numberOfTenants, 
COALESCE(`_removed`.`claimElement`.`privateRentedPropertyDeclarationDetails`.`jointTenancy`.`rent`.`amount`, `claimElement`.`privateRentedPropertyDeclarationDetails`.`jointTenancy`.`rent`.`amount`) as claimElement_privateRentedPropertyDeclarationDetails_jointTenancy_rent_amount, 
COALESCE(`_removed`.`claimElement`.`privateRentedPropertyDeclarationDetails`.`jointTenancy`.`rent`.`paymentFrequency`, `claimElement`.`privateRentedPropertyDeclarationDetails`.`jointTenancy`.`rent`.`paymentFrequency`) as claimElement_privateRentedPropertyDeclarationDetails_jointTenancy_rent_paymentFrequency, 
COALESCE(`_removed`.`claimElement`.`privateRentedPropertyDeclarationDetails`.`jointTenancy`.`rent`.`type`, `claimElement`.`privateRentedPropertyDeclarationDetails`.`jointTenancy`.`rent`.`type`) as claimElement_privateRentedPropertyDeclarationDetails_jointTenancy_rent_type, 
COALESCE(`_removed`.`claimElement`.`privateRentedPropertyDeclarationDetails`.`jointTenancy`.`serviceCharges`.`amount`, `claimElement`.`privateRentedPropertyDeclarationDetails`.`jointTenancy`.`serviceCharges`.`amount`) as claimElement_privateRentedPropertyDeclarationDetails_jointTenancy_serviceCharges_amount, 
COALESCE(`_removed`.`claimElement`.`privateRentedPropertyDeclarationDetails`.`jointTenancy`.`serviceCharges`.`paymentFrequency`, `claimElement`.`privateRentedPropertyDeclarationDetails`.`jointTenancy`.`serviceCharges`.`paymentFrequency`) as claimElement_privateRentedPropertyDeclarationDetails_jointTenancy_serviceCharges_paymentFrequency, 
COALESCE(`_removed`.`claimElement`.`privateRentedPropertyDeclarationDetails`.`jointTenancy`.`serviceCharges`.`type`, `claimElement`.`privateRentedPropertyDeclarationDetails`.`jointTenancy`.`serviceCharges`.`type`) as claimElement_privateRentedPropertyDeclarationDetails_jointTenancy_serviceCharges_type, 
COALESCE(`_removed`.`claimElement`.`privateRentedPropertyDeclarationDetails`.`landlordDetails`.`addressNumber`, `claimElement`.`privateRentedPropertyDeclarationDetails`.`landlordDetails`.`addressNumber`) as claimElement_privateRentedPropertyDeclarationDetails_landlordDetails_addressNumber, 
COALESCE(`_removed`.`claimElement`.`privateRentedPropertyDeclarationDetails`.`landlordDetails`.`email`, `claimElement`.`privateRentedPropertyDeclarationDetails`.`landlordDetails`.`email`) as claimElement_privateRentedPropertyDeclarationDetails_landlordDetails_email, 
COALESCE(`_removed`.`claimElement`.`privateRentedPropertyDeclarationDetails`.`landlordDetails`.`name`, `claimElement`.`privateRentedPropertyDeclarationDetails`.`landlordDetails`.`name`) as claimElement_privateRentedPropertyDeclarationDetails_landlordDetails_name, 
COALESCE(`_removed`.`claimElement`.`privateRentedPropertyDeclarationDetails`.`landlordDetails`.`payRentTo`, `claimElement`.`privateRentedPropertyDeclarationDetails`.`landlordDetails`.`payRentTo`) as claimElement_privateRentedPropertyDeclarationDetails_landlordDetails_payRentTo, 
COALESCE(`_removed`.`claimElement`.`privateRentedPropertyDeclarationDetails`.`landlordDetails`.`phoneNumber`, `claimElement`.`privateRentedPropertyDeclarationDetails`.`landlordDetails`.`phoneNumber`) as claimElement_privateRentedPropertyDeclarationDetails_landlordDetails_phoneNumber, 
COALESCE(`_removed`.`claimElement`.`privateRentedPropertyDeclarationDetails`.`landlordDetails`.`postcode`, `claimElement`.`privateRentedPropertyDeclarationDetails`.`landlordDetails`.`postcode`) as claimElement_privateRentedPropertyDeclarationDetails_landlordDetails_postcode, 
COALESCE(`_removed`.`claimElement`.`privateRentedPropertyDeclarationDetails`.`landlordDetails`.`townCity`, `claimElement`.`privateRentedPropertyDeclarationDetails`.`landlordDetails`.`townCity`) as claimElement_privateRentedPropertyDeclarationDetails_landlordDetails_townCity, 
COALESCE(CAST(`_removed`.`claimElement`.`privateRentedPropertyDeclarationDetails`.`landlordRelationshipDetails`.`anyoneCloseRelative` as STRING), CAST(`claimElement`.`privateRentedPropertyDeclarationDetails`.`landlordRelationshipDetails`.`anyoneCloseRelative` as STRING)) as claimElement_privateRentedPropertyDeclarationDetails_landlordRelationshipDetails_anyoneCloseRelative, 
COALESCE(CAST(`_removed`.`claimElement`.`privateRentedPropertyDeclarationDetails`.`landlordRelationshipDetails`.`anyoneFinancialInterest` as STRING), CAST(`claimElement`.`privateRentedPropertyDeclarationDetails`.`landlordRelationshipDetails`.`anyoneFinancialInterest` as STRING)) as claimElement_privateRentedPropertyDeclarationDetails_landlordRelationshipDetails_anyoneFinancialInterest, 
COALESCE(CAST(`_removed`.`claimElement`.`privateRentedPropertyDeclarationDetails`.`landlordRelationshipDetails`.`liveAtSameProperty` as STRING), CAST(`claimElement`.`privateRentedPropertyDeclarationDetails`.`landlordRelationshipDetails`.`liveAtSameProperty` as STRING)) as claimElement_privateRentedPropertyDeclarationDetails_landlordRelationshipDetails_liveAtSameProperty, 
COALESCE(`_removed`.`claimElement`.`privateRentedPropertyDeclarationDetails`.`rentDetails`.`regularPayment`.`amount`, `claimElement`.`privateRentedPropertyDeclarationDetails`.`rentDetails`.`regularPayment`.`amount`) as claimElement_privateRentedPropertyDeclarationDetails_rentDetails_regularPayment_amount, 
COALESCE(`_removed`.`claimElement`.`privateRentedPropertyDeclarationDetails`.`rentDetails`.`regularPayment`.`paymentFrequency`, `claimElement`.`privateRentedPropertyDeclarationDetails`.`rentDetails`.`regularPayment`.`paymentFrequency`) as claimElement_privateRentedPropertyDeclarationDetails_rentDetails_regularPayment_paymentFrequency, 
COALESCE(`_removed`.`claimElement`.`privateRentedPropertyDeclarationDetails`.`rentDetails`.`regularPayment`.`type`, `claimElement`.`privateRentedPropertyDeclarationDetails`.`rentDetails`.`regularPayment`.`type`) as claimElement_privateRentedPropertyDeclarationDetails_rentDetails_regularPayment_type, 
COALESCE(`_removed`.`claimElement`.`privateRentedPropertyDeclarationDetails`.`rentDetails`.`startDate`, `claimElement`.`privateRentedPropertyDeclarationDetails`.`rentDetails`.`startDate`) as claimElement_privateRentedPropertyDeclarationDetails_rentDetails_startDate, 
COALESCE(CAST(`_removed`.`claimElement`.`socialAccommodationDeclarationDetails`.`additionalRentedHousingInformation`.`exemptFromTheSharedRoomRate` as STRING), CAST(`claimElement`.`socialAccommodationDeclarationDetails`.`additionalRentedHousingInformation`.`exemptFromTheSharedRoomRate` as STRING)) as claimElement_socialAccommodationDeclarationDetails_additionalRentedHousingInformation_exemptFromTheSharedRoomRate, 
COALESCE(CAST(`_removed`.`claimElement`.`socialAccommodationDeclarationDetails`.`additionalRentedHousingInformation`.`nonCommercialRelationshipWithLandlord` as STRING), CAST(`claimElement`.`socialAccommodationDeclarationDetails`.`additionalRentedHousingInformation`.`nonCommercialRelationshipWithLandlord` as STRING)) as claimElement_socialAccommodationDeclarationDetails_additionalRentedHousingInformation_nonCommercialRelationshipWithLandlord, 
COALESCE(CAST(`_removed`.`claimElement`.`socialAccommodationDeclarationDetails`.`additionalRentedHousingInformation`.`supportedExemptAccommodation` as STRING), CAST(`claimElement`.`socialAccommodationDeclarationDetails`.`additionalRentedHousingInformation`.`supportedExemptAccommodation` as STRING)) as claimElement_socialAccommodationDeclarationDetails_additionalRentedHousingInformation_supportedExemptAccommodation, 
COALESCE(CAST(`_removed`.`claimElement`.`socialAccommodationDeclarationDetails`.`bedroomAndTenancy`.`bothYouAndPartnerNamedOnTenancyOrRentalAgreement` as STRING), CAST(`claimElement`.`socialAccommodationDeclarationDetails`.`bedroomAndTenancy`.`bothYouAndPartnerNamedOnTenancyOrRentalAgreement` as STRING)) as claimElement_socialAccommodationDeclarationDetails_bedroomAndTenancy_bothYouAndPartnerNamedOnTenancyOrRentalAgreement, 
COALESCE(CAST(`_removed`.`claimElement`.`socialAccommodationDeclarationDetails`.`bedroomAndTenancy`.`jointTenancyWithSomeoneOtherThanPartner` as STRING), CAST(`claimElement`.`socialAccommodationDeclarationDetails`.`bedroomAndTenancy`.`jointTenancyWithSomeoneOtherThanPartner` as STRING)) as claimElement_socialAccommodationDeclarationDetails_bedroomAndTenancy_jointTenancyWithSomeoneOtherThanPartner, 
COALESCE(`_removed`.`claimElement`.`socialAccommodationDeclarationDetails`.`bedroomAndTenancy`.`numberOfBedrooms`, `claimElement`.`socialAccommodationDeclarationDetails`.`bedroomAndTenancy`.`numberOfBedrooms`) as claimElement_socialAccommodationDeclarationDetails_bedroomAndTenancy_numberOfBedrooms, 
COALESCE(`_removed`.`claimElement`.`socialAccommodationDeclarationDetails`.`jointTenancy`.`numberOfTenants`, `claimElement`.`socialAccommodationDeclarationDetails`.`jointTenancy`.`numberOfTenants`) as claimElement_socialAccommodationDeclarationDetails_jointTenancy_numberOfTenants, 
COALESCE(`_removed`.`claimElement`.`socialAccommodationDeclarationDetails`.`jointTenancy`.`rent`.`amount`, `claimElement`.`socialAccommodationDeclarationDetails`.`jointTenancy`.`rent`.`amount`) as claimElement_socialAccommodationDeclarationDetails_jointTenancy_rent_amount, 
COALESCE(`_removed`.`claimElement`.`socialAccommodationDeclarationDetails`.`jointTenancy`.`rent`.`paymentFrequency`, `claimElement`.`socialAccommodationDeclarationDetails`.`jointTenancy`.`rent`.`paymentFrequency`) as claimElement_socialAccommodationDeclarationDetails_jointTenancy_rent_paymentFrequency, 
COALESCE(`_removed`.`claimElement`.`socialAccommodationDeclarationDetails`.`jointTenancy`.`rent`.`type`, `claimElement`.`socialAccommodationDeclarationDetails`.`jointTenancy`.`rent`.`type`) as claimElement_socialAccommodationDeclarationDetails_jointTenancy_rent_type, 
COALESCE(`_removed`.`claimElement`.`socialAccommodationDeclarationDetails`.`jointTenancy`.`serviceCharges`.`amount`, `claimElement`.`socialAccommodationDeclarationDetails`.`jointTenancy`.`serviceCharges`.`amount`) as claimElement_socialAccommodationDeclarationDetails_jointTenancy_serviceCharges_amount, 
COALESCE(`_removed`.`claimElement`.`socialAccommodationDeclarationDetails`.`jointTenancy`.`serviceCharges`.`paymentFrequency`, `claimElement`.`socialAccommodationDeclarationDetails`.`jointTenancy`.`serviceCharges`.`paymentFrequency`) as claimElement_socialAccommodationDeclarationDetails_jointTenancy_serviceCharges_paymentFrequency, 
COALESCE(`_removed`.`claimElement`.`socialAccommodationDeclarationDetails`.`jointTenancy`.`serviceCharges`.`type`, `claimElement`.`socialAccommodationDeclarationDetails`.`jointTenancy`.`serviceCharges`.`type`) as claimElement_socialAccommodationDeclarationDetails_jointTenancy_serviceCharges_type, 
COALESCE(`_removed`.`claimElement`.`socialAccommodationDeclarationDetails`.`landlordDetails`.`addressNumber`, `claimElement`.`socialAccommodationDeclarationDetails`.`landlordDetails`.`addressNumber`) as claimElement_socialAccommodationDeclarationDetails_landlordDetails_addressNumber, 
COALESCE(`_removed`.`claimElement`.`socialAccommodationDeclarationDetails`.`landlordDetails`.`email`, `claimElement`.`socialAccommodationDeclarationDetails`.`landlordDetails`.`email`) as claimElement_socialAccommodationDeclarationDetails_landlordDetails_email, 
COALESCE(`_removed`.`claimElement`.`socialAccommodationDeclarationDetails`.`landlordDetails`.`name`, `claimElement`.`socialAccommodationDeclarationDetails`.`landlordDetails`.`name`) as claimElement_socialAccommodationDeclarationDetails_landlordDetails_name, 
COALESCE(`_removed`.`claimElement`.`socialAccommodationDeclarationDetails`.`landlordDetails`.`payRentTo`, `claimElement`.`socialAccommodationDeclarationDetails`.`landlordDetails`.`payRentTo`) as claimElement_socialAccommodationDeclarationDetails_landlordDetails_payRentTo, 
COALESCE(`_removed`.`claimElement`.`socialAccommodationDeclarationDetails`.`landlordDetails`.`phoneNumber`, `claimElement`.`socialAccommodationDeclarationDetails`.`landlordDetails`.`phoneNumber`) as claimElement_socialAccommodationDeclarationDetails_landlordDetails_phoneNumber, 
COALESCE(`_removed`.`claimElement`.`socialAccommodationDeclarationDetails`.`landlordDetails`.`postcode`, `claimElement`.`socialAccommodationDeclarationDetails`.`landlordDetails`.`postcode`) as claimElement_socialAccommodationDeclarationDetails_landlordDetails_postcode, 
COALESCE(`_removed`.`claimElement`.`socialAccommodationDeclarationDetails`.`landlordDetails`.`townCity`, `claimElement`.`socialAccommodationDeclarationDetails`.`landlordDetails`.`townCity`) as claimElement_socialAccommodationDeclarationDetails_landlordDetails_townCity, 
COALESCE(`_removed`.`claimElement`.`socialAccommodationDeclarationDetails`.`rentDetails`.`regularPayment`.`amount`, `claimElement`.`socialAccommodationDeclarationDetails`.`rentDetails`.`regularPayment`.`amount`) as claimElement_socialAccommodationDeclarationDetails_rentDetails_regularPayment_amount, 
COALESCE(`_removed`.`claimElement`.`socialAccommodationDeclarationDetails`.`rentDetails`.`regularPayment`.`paymentFrequency`, `claimElement`.`socialAccommodationDeclarationDetails`.`rentDetails`.`regularPayment`.`paymentFrequency`) as claimElement_socialAccommodationDeclarationDetails_rentDetails_regularPayment_paymentFrequency, 
COALESCE(`_removed`.`claimElement`.`socialAccommodationDeclarationDetails`.`rentDetails`.`regularPayment`.`type`, `claimElement`.`socialAccommodationDeclarationDetails`.`rentDetails`.`regularPayment`.`type`) as claimElement_socialAccommodationDeclarationDetails_rentDetails_regularPayment_type, 
COALESCE(`_removed`.`claimElement`.`socialAccommodationDeclarationDetails`.`rentDetails`.`startDate`, `claimElement`.`socialAccommodationDeclarationDetails`.`rentDetails`.`startDate`) as claimElement_socialAccommodationDeclarationDetails_rentDetails_startDate, 
COALESCE(`_removed`.`claimElement`.`socialAccommodationDeclarationDetails`.`socialHousing`.`rentFreeWeeks`, `claimElement`.`socialAccommodationDeclarationDetails`.`socialHousing`.`rentFreeWeeks`) as claimElement_socialAccommodationDeclarationDetails_socialHousing_rentFreeWeeks, 
COALESCE(`_removed`.`claimElement`.`socialAccommodationDeclarationDetails`.`socialHousing`.`serviceCharges`.`amount`, `claimElement`.`socialAccommodationDeclarationDetails`.`socialHousing`.`serviceCharges`.`amount`) as claimElement_socialAccommodationDeclarationDetails_socialHousing_serviceCharges_amount, 
COALESCE(`_removed`.`claimElement`.`socialAccommodationDeclarationDetails`.`socialHousing`.`serviceCharges`.`paymentFrequency`, `claimElement`.`socialAccommodationDeclarationDetails`.`socialHousing`.`serviceCharges`.`paymentFrequency`) as claimElement_socialAccommodationDeclarationDetails_socialHousing_serviceCharges_paymentFrequency, 
COALESCE(`_removed`.`claimElement`.`socialAccommodationDeclarationDetails`.`socialHousing`.`serviceCharges`.`type`, `claimElement`.`socialAccommodationDeclarationDetails`.`socialHousing`.`serviceCharges`.`type`) as claimElement_socialAccommodationDeclarationDetails_socialHousing_serviceCharges_type, 
COALESCE(CAST(`_removed`.`claimElement`.`elementAwardAffectedByNonDependents` as STRING), CAST(`claimElement`.`elementAwardAffectedByNonDependents` as STRING)) as claimElement_elementAwardAffectedByNonDependents, 
COALESCE(CAST(`_removed`.`claimElement`.`numberOfBedroomsSignificant` as STRING), CAST(`claimElement`.`numberOfBedroomsSignificant` as STRING)) as claimElement_numberOfBedroomsSignificant, 
COALESCE(`_removed`.`claimElement`.`type`, `claimElement`.`type`) as claimElement_type, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`invalidated`.`dateTime`.`d_date`, LENGTH(`_removed`.`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as invalidated_dateTime_d_date, 
COALESCE(`_removed`.`invalidated`.`reason`, `invalidated`.`reason`) as invalidated_reason, 
COALESCE(`_removed`.`_id`.`d_oid`, `_id`.`d_oid`) as id FROM src_accepted_data_housingcircumstances_housingcircumstances
 LATERAL VIEW OUTER EXPLODE(`claimElement`.`ownPropertyDeclarationDetails`.`mortgageDetailsList`) view_exploded_claimelement_ownpropertydeclarationdetails_mortgagedetailslist AS exploded_claimelement_ownpropertydeclarationdetails_mortgagedetailslist 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`claimElement`.`ownPropertyDeclarationDetails`.`mortgageDetailsList`) view_removed_exploded_claimelement_ownpropertydeclarationdetails_mortgagedetailslist AS removed_exploded_claimelement_ownpropertydeclarationdetails_mortgagedetailslist 
;


!echo ------------------------;
!echo ------------------------ workCapabilityAssessmentDecision;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_workcapabilityassessmentdecision_workcapabilityassessmentdecision
;

CREATE EXTERNAL TABLE src_accepted_data_workcapabilityassessmentdecision_workcapabilityassessmentdecision(
`claimElement` STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`removedDateTime`:STRUCT<`d_date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`claimantId`:STRING
,`decision`:STRING
,`endDate`:STRING
,`isSevereCondition`:BOOLEAN
,`prognosisInMonths`:STRING
,`startDate`:STRING
,`workCapabilityAssessmentDecisionId`:STRING
,`requiredPeriodDoesNotApply`:STRING
,`type`:STRING>
,`invalidated` STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`acceptedDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`d_oid`:STRING>
,`_version` STRING
,`declarationId` STRING
,`_removed` STRUCT<`claimElement`:STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`removedDateTime`:STRUCT<`d_date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`contractId`:STRING
,`claimantId`:STRING
,`decision`:STRING
,`endDate`:STRING
,`isSevereCondition`:BOOLEAN
,`prognosisInMonths`:STRING
,`startDate`:STRING
,`workCapabilityAssessmentDecisionId`:STRING
,`requiredPeriodDoesNotApply`:STRING
,`type`:STRING>
,`invalidated`:STRUCT<`dateTime`:STRUCT<`d_date`:STRING>
,`reason`:STRING>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`acceptedDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`d_oid`:STRING>
,`_version`:STRING
,`declarationId`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/workCapabilityAssessmentDecision';

DROP TABLE IF EXISTS workCapabilityAssessmentDecision;

CREATE TABLE workcapabilityassessmentdecision STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`acceptedDateTime`.`d_date`, LENGTH(`_removed`.`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`acceptedDateTime`.`d_date`, LENGTH(`acceptedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`acceptedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`acceptedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`acceptedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`acceptedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as acceptedDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`contractId`, `claimElement`.`contractId`) as claimElement_contractId, 
COALESCE(`_removed`.`claimElement`.`declaredDateTime`, `claimElement`.`declaredDateTime`) as claimElement_declaredDateTime, 
COALESCE(CAST(`_removed`.`claimElement`.`effectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`effectiveDate`.`hasDate` as STRING)) as claimElement_effectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`knownDate`, `claimElement`.`effectiveDate`.`knownDate`) as claimElement_effectiveDate_knownDate, 
COALESCE(CAST(`_removed`.`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`paymentEffectiveDate`.`hasDate` as STRING)) as claimElement_paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`knownDate`, `claimElement`.`paymentEffectiveDate`.`knownDate`) as claimElement_paymentEffectiveDate_knownDate, 
COALESCE(`_removed`.`claimElement`.`claimantId`, `claimElement`.`claimantId`) as claimElement_claimantId, 
COALESCE(`_removed`.`claimElement`.`decision`, `claimElement`.`decision`) as claimElement_decision, 
COALESCE(`_removed`.`claimElement`.`endDate`, `claimElement`.`endDate`) as claimElement_endDate, 
COALESCE(CAST(`_removed`.`claimElement`.`isSevereCondition` as STRING), CAST(`claimElement`.`isSevereCondition` as STRING)) as claimElement_isSevereCondition, 
COALESCE(`_removed`.`claimElement`.`prognosisInMonths`, `claimElement`.`prognosisInMonths`) as claimElement_prognosisInMonths, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`claimElement`.`removedDateTime`.`d_date`, LENGTH(`_removed`.`claimElement`.`removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`claimElement`.`removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`claimElement`.`removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`claimElement`.`removedDateTime`.`d_date`, LENGTH(`claimElement`.`removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`claimElement`.`removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`claimElement`.`removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`claimElement`.`removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`claimElement`.`removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as claimElement_removedDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`startDate`, `claimElement`.`startDate`) as claimElement_startDate, 
COALESCE(`_removed`.`claimElement`.`workCapabilityAssessmentDecisionId`, `claimElement`.`workCapabilityAssessmentDecisionId`) as claimElement_workCapabilityAssessmentDecisionId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`_removed`.`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as claimElement_declaredDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`type`, `claimElement`.`effectiveDate`.`type`) as claimElement_effectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`date`, `claimElement`.`effectiveDate`.`date`) as claimElement_effectiveDate_date, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`type`, `claimElement`.`paymentEffectiveDate`.`type`) as claimElement_paymentEffectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`date`, `claimElement`.`paymentEffectiveDate`.`date`) as claimElement_paymentEffectiveDate_date, 
COALESCE(`_removed`.`claimElement`.`requiredPeriodDoesNotApply`, `claimElement`.`requiredPeriodDoesNotApply`) as claimElement_requiredPeriodDoesNotApply, 
COALESCE(`_removed`.`claimElement`.`type`, `claimElement`.`type`) as claimElement_type, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`invalidated`.`dateTime`.`d_date`, LENGTH(`_removed`.`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as invalidated_dateTime_d_date, 
COALESCE(`_removed`.`invalidated`.`reason`, `invalidated`.`reason`) as invalidated_reason, 
COALESCE(`_removed`.`_id`.`d_oid`, `_id`.`d_oid`) as id FROM src_accepted_data_workcapabilityassessmentdecision_workcapabilityassessmentdecision
 src_accepted_data_workcapabilityassessmentdecision_workcapabilityassessmentdecision;


