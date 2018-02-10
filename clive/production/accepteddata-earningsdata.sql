use uc_clive;
!echo ------------------------;
!echo ------------------------ earningsData_SELF_REPORTED;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_earningsdata_earningsdata_self_reported
;

CREATE EXTERNAL TABLE src_accepted_data_earningsdata_earningsdata_self_reported(
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
,`amount`:STRING
,`claimantId`:STRING
,`deletedDate`:STRING
,`earningsDataId`:STRING
,`paymentDate`:STRING
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
,`amount`:STRING
,`claimantId`:STRING
,`deletedDate`:STRING
,`earningsDataId`:STRING
,`paymentDate`:STRING
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
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/earningsData';

DROP TABLE IF EXISTS earningsData_SELF_REPORTED;

CREATE TABLE earningsdata_self_reported STORED AS ORC AS SELECT 
 
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
COALESCE(`_removed`.`claimElement`.`claimantId`, `claimElement`.`claimantId`) as claimElement_claimantId, 
COALESCE(`_removed`.`claimElement`.`deletedDate`, `claimElement`.`deletedDate`) as claimElement_deletedDate, 
COALESCE(`_removed`.`claimElement`.`earningsDataId`, `claimElement`.`earningsDataId`) as claimElement_earningsDataId, 
COALESCE(`_removed`.`claimElement`.`paymentDate`, `claimElement`.`paymentDate`) as claimElement_paymentDate, 
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
COALESCE(`_removed`.`_id`.`d_oid`, `_id`.`d_oid`) as id FROM src_accepted_data_earningsdata_earningsdata_self_reported
 src_accepted_data_earningsdata_earningsdata_self_reported 
 where `_removed`.`claimElement`.`type`='SELF_REPORTED' or `claimElement`.`type`='SELF_REPORTED';


!echo ------------------------;
!echo ------------------------ earningsData_SELF_EMPLOYED_EARNINGS_WITH_LATEST_MIF_DATA;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_earningsdata_earningsdata_self_employed_earnings_with_latest_mif_data
;

CREATE EXTERNAL TABLE src_accepted_data_earningsdata_earningsdata_self_employed_earnings_with_latest_mif_data(
`claimElement` STRUCT<`employmentCircumstances`:STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`claimantId`:STRING
,`contractId`:STRING
,`gainfullySelfEmployed`:BOOLEAN
,`mifReviewDate`:STRING
,`mifReviewReason`:STRING
,`minimumIncomeFloor`:STRING
,`minimumIncomeFloorDate`:STRING
,`mustReportSelfEmploymentEarnings`:BOOLEAN
,`type`:STRING>
,`selfEmployedEarningsData`:STRUCT<`declaredDateTime`:STRUCT<`d_date`:STRING>
,`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`amount`:STRING
,`claimantId`:STRING
,`contractId`:STRING
,`deletedDate`:STRING
,`earningsDataId`:STRING
,`paymentDate`:STRING
,`type`:STRING>
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
,`assessmentPeriodEndDate`:STRING
,`claimantId`:STRING
,`deletedDate`:STRING
,`earningsDataId`:STRING
,`paymentDate`:STRING
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
,`_removed` STRUCT<`claimElement`:STRUCT<`employmentCircumstances`:STRUCT<`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`claimantId`:STRING
,`contractId`:STRING
,`gainfullySelfEmployed`:BOOLEAN
,`mifReviewDate`:STRING
,`mifReviewReason`:STRING
,`minimumIncomeFloor`:STRING
,`minimumIncomeFloorDate`:STRING
,`mustReportSelfEmploymentEarnings`:BOOLEAN
,`type`:STRING>
,`selfEmployedEarningsData`:STRUCT<`declaredDateTime`:STRUCT<`d_date`:STRING>
,`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`amount`:STRING
,`claimantId`:STRING
,`contractId`:STRING
,`deletedDate`:STRING
,`earningsDataId`:STRING
,`paymentDate`:STRING
,`type`:STRING>
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
,`assessmentPeriodEndDate`:STRING
,`claimantId`:STRING
,`deletedDate`:STRING
,`earningsDataId`:STRING
,`paymentDate`:STRING
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
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/earningsData';

DROP TABLE IF EXISTS earningsData_SELF_EMPLOYED_EARNINGS_WITH_LATEST_MIF_DATA;

CREATE TABLE earningsdata_self_employed_earnings_with_latest_mif_data STORED AS ORC AS SELECT 
 
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
COALESCE(`_removed`.`claimElement`.`assessmentPeriodEndDate`, `claimElement`.`assessmentPeriodEndDate`) as claimElement_assessmentPeriodEndDate, 
COALESCE(`_removed`.`claimElement`.`employmentCircumstances`.`claimantId`, `claimElement`.`employmentCircumstances`.`claimantId`) as claimElement_employmentCircumstances_claimantId, 
COALESCE(`_removed`.`claimElement`.`employmentCircumstances`.`contractId`, `claimElement`.`employmentCircumstances`.`contractId`) as claimElement_employmentCircumstances_contractId, 
COALESCE(CAST(`_removed`.`claimElement`.`employmentCircumstances`.`effectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`employmentCircumstances`.`effectiveDate`.`hasDate` as STRING)) as claimElement_employmentCircumstances_effectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`employmentCircumstances`.`effectiveDate`.`knownDate`, `claimElement`.`employmentCircumstances`.`effectiveDate`.`knownDate`) as claimElement_employmentCircumstances_effectiveDate_knownDate, 
COALESCE(`_removed`.`claimElement`.`employmentCircumstances`.`effectiveDate`.`type`, `claimElement`.`employmentCircumstances`.`effectiveDate`.`type`) as claimElement_employmentCircumstances_effectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`employmentCircumstances`.`effectiveDate`.`date`, `claimElement`.`employmentCircumstances`.`effectiveDate`.`date`) as claimElement_employmentCircumstances_effectiveDate_date, 
COALESCE(CAST(`_removed`.`claimElement`.`employmentCircumstances`.`gainfullySelfEmployed` as STRING), CAST(`claimElement`.`employmentCircumstances`.`gainfullySelfEmployed` as STRING)) as claimElement_employmentCircumstances_gainfullySelfEmployed, 
COALESCE(`_removed`.`claimElement`.`employmentCircumstances`.`mifReviewDate`, `claimElement`.`employmentCircumstances`.`mifReviewDate`) as claimElement_employmentCircumstances_mifReviewDate, 
COALESCE(`_removed`.`claimElement`.`employmentCircumstances`.`mifReviewReason`, `claimElement`.`employmentCircumstances`.`mifReviewReason`) as claimElement_employmentCircumstances_mifReviewReason, 
COALESCE(`_removed`.`claimElement`.`employmentCircumstances`.`minimumIncomeFloor`, `claimElement`.`employmentCircumstances`.`minimumIncomeFloor`) as claimElement_employmentCircumstances_minimumIncomeFloor, 
COALESCE(`_removed`.`claimElement`.`employmentCircumstances`.`minimumIncomeFloorDate`, `claimElement`.`employmentCircumstances`.`minimumIncomeFloorDate`) as claimElement_employmentCircumstances_minimumIncomeFloorDate, 
COALESCE(CAST(`_removed`.`claimElement`.`employmentCircumstances`.`mustReportSelfEmploymentEarnings` as STRING), CAST(`claimElement`.`employmentCircumstances`.`mustReportSelfEmploymentEarnings` as STRING)) as claimElement_employmentCircumstances_mustReportSelfEmploymentEarnings, 
COALESCE(CAST(`_removed`.`claimElement`.`employmentCircumstances`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`employmentCircumstances`.`paymentEffectiveDate`.`hasDate` as STRING)) as claimElement_employmentCircumstances_paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`employmentCircumstances`.`paymentEffectiveDate`.`knownDate`, `claimElement`.`employmentCircumstances`.`paymentEffectiveDate`.`knownDate`) as claimElement_employmentCircumstances_paymentEffectiveDate_knownDate, 
COALESCE(`_removed`.`claimElement`.`employmentCircumstances`.`paymentEffectiveDate`.`type`, `claimElement`.`employmentCircumstances`.`paymentEffectiveDate`.`type`) as claimElement_employmentCircumstances_paymentEffectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`employmentCircumstances`.`paymentEffectiveDate`.`date`, `claimElement`.`employmentCircumstances`.`paymentEffectiveDate`.`date`) as claimElement_employmentCircumstances_paymentEffectiveDate_date, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`claimElement`.`employmentCircumstances`.`declaredDateTime`.`d_date`, LENGTH(`_removed`.`claimElement`.`employmentCircumstances`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`employmentCircumstances`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`employmentCircumstances`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`employmentCircumstances`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`employmentCircumstances`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`claimElement`.`employmentCircumstances`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`claimElement`.`employmentCircumstances`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`claimElement`.`employmentCircumstances`.`declaredDateTime`.`d_date`, LENGTH(`claimElement`.`employmentCircumstances`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`claimElement`.`employmentCircumstances`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`employmentCircumstances`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`claimElement`.`employmentCircumstances`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`employmentCircumstances`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`claimElement`.`employmentCircumstances`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`claimElement`.`employmentCircumstances`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as claimElement_employmentCircumstances_declaredDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`employmentCircumstances`.`type`, `claimElement`.`employmentCircumstances`.`type`) as claimElement_employmentCircumstances_type, 
COALESCE(`_removed`.`claimElement`.`selfEmployedEarningsData`.`amount`, `claimElement`.`selfEmployedEarningsData`.`amount`) as claimElement_selfEmployedEarningsData_amount, 
COALESCE(`_removed`.`claimElement`.`selfEmployedEarningsData`.`claimantId`, `claimElement`.`selfEmployedEarningsData`.`claimantId`) as claimElement_selfEmployedEarningsData_claimantId, 
COALESCE(`_removed`.`claimElement`.`selfEmployedEarningsData`.`contractId`, `claimElement`.`selfEmployedEarningsData`.`contractId`) as claimElement_selfEmployedEarningsData_contractId, 
COALESCE(`_removed`.`claimElement`.`selfEmployedEarningsData`.`deletedDate`, `claimElement`.`selfEmployedEarningsData`.`deletedDate`) as claimElement_selfEmployedEarningsData_deletedDate, 
COALESCE(`_removed`.`claimElement`.`selfEmployedEarningsData`.`earningsDataId`, `claimElement`.`selfEmployedEarningsData`.`earningsDataId`) as claimElement_selfEmployedEarningsData_earningsDataId, 
COALESCE(`_removed`.`claimElement`.`selfEmployedEarningsData`.`paymentDate`, `claimElement`.`selfEmployedEarningsData`.`paymentDate`) as claimElement_selfEmployedEarningsData_paymentDate, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`claimElement`.`selfEmployedEarningsData`.`declaredDateTime`.`d_date`, LENGTH(`_removed`.`claimElement`.`selfEmployedEarningsData`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`selfEmployedEarningsData`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`selfEmployedEarningsData`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`selfEmployedEarningsData`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`selfEmployedEarningsData`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`claimElement`.`selfEmployedEarningsData`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`claimElement`.`selfEmployedEarningsData`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`claimElement`.`selfEmployedEarningsData`.`declaredDateTime`.`d_date`, LENGTH(`claimElement`.`selfEmployedEarningsData`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`claimElement`.`selfEmployedEarningsData`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`selfEmployedEarningsData`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`claimElement`.`selfEmployedEarningsData`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`selfEmployedEarningsData`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`claimElement`.`selfEmployedEarningsData`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`claimElement`.`selfEmployedEarningsData`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as claimElement_selfEmployedEarningsData_declaredDateTime_d_date, 
COALESCE(CAST(`_removed`.`claimElement`.`selfEmployedEarningsData`.`effectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`selfEmployedEarningsData`.`effectiveDate`.`hasDate` as STRING)) as claimElement_selfEmployedEarningsData_effectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`selfEmployedEarningsData`.`effectiveDate`.`knownDate`, `claimElement`.`selfEmployedEarningsData`.`effectiveDate`.`knownDate`) as claimElement_selfEmployedEarningsData_effectiveDate_knownDate, 
COALESCE(`_removed`.`claimElement`.`selfEmployedEarningsData`.`effectiveDate`.`type`, `claimElement`.`selfEmployedEarningsData`.`effectiveDate`.`type`) as claimElement_selfEmployedEarningsData_effectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`selfEmployedEarningsData`.`effectiveDate`.`date`, `claimElement`.`selfEmployedEarningsData`.`effectiveDate`.`date`) as claimElement_selfEmployedEarningsData_effectiveDate_date, 
COALESCE(CAST(`_removed`.`claimElement`.`selfEmployedEarningsData`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`claimElement`.`selfEmployedEarningsData`.`paymentEffectiveDate`.`hasDate` as STRING)) as claimElement_selfEmployedEarningsData_paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`claimElement`.`selfEmployedEarningsData`.`paymentEffectiveDate`.`knownDate`, `claimElement`.`selfEmployedEarningsData`.`paymentEffectiveDate`.`knownDate`) as claimElement_selfEmployedEarningsData_paymentEffectiveDate_knownDate, 
COALESCE(`_removed`.`claimElement`.`selfEmployedEarningsData`.`paymentEffectiveDate`.`type`, `claimElement`.`selfEmployedEarningsData`.`paymentEffectiveDate`.`type`) as claimElement_selfEmployedEarningsData_paymentEffectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`selfEmployedEarningsData`.`paymentEffectiveDate`.`date`, `claimElement`.`selfEmployedEarningsData`.`paymentEffectiveDate`.`date`) as claimElement_selfEmployedEarningsData_paymentEffectiveDate_date, 
COALESCE(`_removed`.`claimElement`.`selfEmployedEarningsData`.`type`, `claimElement`.`selfEmployedEarningsData`.`type`) as claimElement_selfEmployedEarningsData_type, 
COALESCE(`_removed`.`claimElement`.`claimantId`, `claimElement`.`claimantId`) as claimElement_claimantId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`_removed`.`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as claimElement_declaredDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`deletedDate`, `claimElement`.`deletedDate`) as claimElement_deletedDate, 
COALESCE(`_removed`.`claimElement`.`earningsDataId`, `claimElement`.`earningsDataId`) as claimElement_earningsDataId, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`type`, `claimElement`.`effectiveDate`.`type`) as claimElement_effectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`date`, `claimElement`.`effectiveDate`.`date`) as claimElement_effectiveDate_date, 
COALESCE(`_removed`.`claimElement`.`paymentDate`, `claimElement`.`paymentDate`) as claimElement_paymentDate, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`type`, `claimElement`.`paymentEffectiveDate`.`type`) as claimElement_paymentEffectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`date`, `claimElement`.`paymentEffectiveDate`.`date`) as claimElement_paymentEffectiveDate_date, 
COALESCE(`_removed`.`claimElement`.`type`, `claimElement`.`type`) as claimElement_type, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`invalidated`.`dateTime`.`d_date`, LENGTH(`_removed`.`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as invalidated_dateTime_d_date, 
COALESCE(`_removed`.`invalidated`.`reason`, `invalidated`.`reason`) as invalidated_reason, 
COALESCE(`_removed`.`_id`.`d_oid`, `_id`.`d_oid`) as id FROM src_accepted_data_earningsdata_earningsdata_self_employed_earnings_with_latest_mif_data
 src_accepted_data_earningsdata_earningsdata_self_employed_earnings_with_latest_mif_data 
 where `_removed`.`claimElement`.`type`='SELF_EMPLOYED_EARNINGS_WITH_LATEST_MIF_DATA' or `claimElement`.`type`='SELF_EMPLOYED_EARNINGS_WITH_LATEST_MIF_DATA';


!echo ------------------------;
!echo ------------------------ earningsData_SELF_EMPLOYED;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_earningsdata_earningsdata_self_employed
;

CREATE EXTERNAL TABLE src_accepted_data_earningsdata_earningsdata_self_employed(
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
,`amount`:STRING
,`claimantId`:STRING
,`deletedDate`:STRING
,`earningsDataId`:STRING
,`paymentDate`:STRING
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
,`amount`:STRING
,`claimantId`:STRING
,`deletedDate`:STRING
,`earningsDataId`:STRING
,`paymentDate`:STRING
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
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/earningsData';

DROP TABLE IF EXISTS earningsData_SELF_EMPLOYED;

CREATE TABLE earningsdata_self_employed STORED AS ORC AS SELECT 
 
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
COALESCE(`_removed`.`claimElement`.`claimantId`, `claimElement`.`claimantId`) as claimElement_claimantId, 
COALESCE(`_removed`.`claimElement`.`deletedDate`, `claimElement`.`deletedDate`) as claimElement_deletedDate, 
COALESCE(`_removed`.`claimElement`.`earningsDataId`, `claimElement`.`earningsDataId`) as claimElement_earningsDataId, 
COALESCE(`_removed`.`claimElement`.`paymentDate`, `claimElement`.`paymentDate`) as claimElement_paymentDate, 
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
COALESCE(`_removed`.`_id`.`d_oid`, `_id`.`d_oid`) as id FROM src_accepted_data_earningsdata_earningsdata_self_employed
 src_accepted_data_earningsdata_earningsdata_self_employed 
 where `_removed`.`claimElement`.`type`='SELF_EMPLOYED' or `claimElement`.`type`='SELF_EMPLOYED';


!echo ------------------------;
!echo ------------------------ earningsData_CALCULATED_EARNINGS;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_earningsdata_earningsdata_calculated_earnings
;

CREATE EXTERNAL TABLE src_accepted_data_earningsdata_earningsdata_calculated_earnings(
`claimElement` STRUCT<`employmentEarningsBreakdown`:ARRAY<STRUCT<`employer`:STRING
,`grossEarnings`:STRING
,`netEarnings`:STRING
,`niAmount`:STRING
,`pensionContributedUnderNetPayAmount`:STRING
,`pensionNotContributedUnderNetPayAmount`:STRING
,`taxAmount`:STRING
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
,`deletedDate`:STRING
,`earningsDataId`:STRING
,`interestSet`:BOOLEAN
,`nino`:STRING
,`paymentDate`:STRING
,`netAmount`:STRING
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
,`_removed` STRUCT<`claimElement`:STRUCT<`employmentEarningsBreakdown`:ARRAY<STRUCT<`employer`:STRING
,`grossEarnings`:STRING
,`netEarnings`:STRING
,`niAmount`:STRING
,`pensionContributedUnderNetPayAmount`:STRING
,`pensionNotContributedUnderNetPayAmount`:STRING
,`taxAmount`:STRING
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
,`deletedDate`:STRING
,`earningsDataId`:STRING
,`interestSet`:BOOLEAN
,`nino`:STRING
,`paymentDate`:STRING
,`netAmount`:STRING
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
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/earningsData';

DROP TABLE IF EXISTS earningsData_CALCULATED_EARNINGS;

CREATE TABLE earningsdata_calculated_earnings STORED AS ORC AS SELECT 
 
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
COALESCE(`_removed`.`claimElement`.`deletedDate`, `claimElement`.`deletedDate`) as claimElement_deletedDate, 
COALESCE(`_removed`.`claimElement`.`earningsDataId`, `claimElement`.`earningsDataId`) as claimElement_earningsDataId, 
COALESCE(`removed_exploded_claimelement_employmentearningsbreakdown`.`employer`, `exploded_claimelement_employmentearningsbreakdown`.`employer`) as claimElement_employmentEarningsBreakdown_employer, 
COALESCE(`removed_exploded_claimelement_employmentearningsbreakdown`.`grossEarnings`, `exploded_claimelement_employmentearningsbreakdown`.`grossEarnings`) as claimElement_employmentEarningsBreakdown_grossEarnings, 
COALESCE(`removed_exploded_claimelement_employmentearningsbreakdown`.`netEarnings`, `exploded_claimelement_employmentearningsbreakdown`.`netEarnings`) as claimElement_employmentEarningsBreakdown_netEarnings, 
COALESCE(`removed_exploded_claimelement_employmentearningsbreakdown`.`niAmount`, `exploded_claimelement_employmentearningsbreakdown`.`niAmount`) as claimElement_employmentEarningsBreakdown_niAmount, 
COALESCE(`removed_exploded_claimelement_employmentearningsbreakdown`.`pensionContributedUnderNetPayAmount`, `exploded_claimelement_employmentearningsbreakdown`.`pensionContributedUnderNetPayAmount`) as claimElement_employmentEarningsBreakdown_pensionContributedUnderNetPayAmount, 
COALESCE(`removed_exploded_claimelement_employmentearningsbreakdown`.`pensionNotContributedUnderNetPayAmount`, `exploded_claimelement_employmentearningsbreakdown`.`pensionNotContributedUnderNetPayAmount`) as claimElement_employmentEarningsBreakdown_pensionNotContributedUnderNetPayAmount, 
COALESCE(`removed_exploded_claimelement_employmentearningsbreakdown`.`taxAmount`, `exploded_claimelement_employmentearningsbreakdown`.`taxAmount`) as claimElement_employmentEarningsBreakdown_taxAmount, 
COALESCE(CAST(`_removed`.`claimElement`.`interestSet` as STRING), CAST(`claimElement`.`interestSet` as STRING)) as claimElement_interestSet, 
COALESCE(`_removed`.`claimElement`.`nino`, `claimElement`.`nino`) as claimElement_nino, 
COALESCE(`_removed`.`claimElement`.`paymentDate`, `claimElement`.`paymentDate`) as claimElement_paymentDate, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`_removed`.`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as claimElement_declaredDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`type`, `claimElement`.`effectiveDate`.`type`) as claimElement_effectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`date`, `claimElement`.`effectiveDate`.`date`) as claimElement_effectiveDate_date, 
COALESCE(`_removed`.`claimElement`.`netAmount`, `claimElement`.`netAmount`) as claimElement_netAmount, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`type`, `claimElement`.`paymentEffectiveDate`.`type`) as claimElement_paymentEffectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`date`, `claimElement`.`paymentEffectiveDate`.`date`) as claimElement_paymentEffectiveDate_date, 
COALESCE(`_removed`.`claimElement`.`type`, `claimElement`.`type`) as claimElement_type, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`invalidated`.`dateTime`.`d_date`, LENGTH(`_removed`.`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as invalidated_dateTime_d_date, 
COALESCE(`_removed`.`invalidated`.`reason`, `invalidated`.`reason`) as invalidated_reason, 
COALESCE(`_removed`.`_id`.`d_oid`, `_id`.`d_oid`) as id FROM src_accepted_data_earningsdata_earningsdata_calculated_earnings
 LATERAL VIEW OUTER EXPLODE(`claimElement`.`employmentEarningsBreakdown`) view_exploded_claimelement_employmentearningsbreakdown AS exploded_claimelement_employmentearningsbreakdown 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`claimElement`.`employmentEarningsBreakdown`) view_removed_exploded_claimelement_employmentearningsbreakdown AS removed_exploded_claimelement_employmentearningsbreakdown 
 
 where `_removed`.`claimElement`.`type`='CALCULATED_EARNINGS' or `claimElement`.`type`='CALCULATED_EARNINGS';


!echo ------------------------;
!echo ------------------------ earningsData_RTE_FEED_TO_DO;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_earningsdata_earningsdata_rte_feed_to_do
;

CREATE EXTERNAL TABLE src_accepted_data_earningsdata_earningsdata_rte_feed_to_do(
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
,`deletedDate`:STRING
,`earningsDataId`:STRING
,`nino`:STRING
,`paymentDate`:STRING
,`previousPaymentDate`:STRING
,`yearToDateEarnings`:STRING
,`yearToDateEarningsPreviousPeriod`:STRING
,`earningsForPeriod`:STRING
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
,`deletedDate`:STRING
,`earningsDataId`:STRING
,`nino`:STRING
,`paymentDate`:STRING
,`previousPaymentDate`:STRING
,`yearToDateEarnings`:STRING
,`yearToDateEarningsPreviousPeriod`:STRING
,`earningsForPeriod`:STRING
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
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/earningsData';

DROP TABLE IF EXISTS earningsData_RTE_FEED_TO_DO;

CREATE TABLE earningsdata_rte_feed_to_do STORED AS ORC AS SELECT 
 
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
COALESCE(`_removed`.`claimElement`.`deletedDate`, `claimElement`.`deletedDate`) as claimElement_deletedDate, 
COALESCE(`_removed`.`claimElement`.`earningsDataId`, `claimElement`.`earningsDataId`) as claimElement_earningsDataId, 
COALESCE(`_removed`.`claimElement`.`nino`, `claimElement`.`nino`) as claimElement_nino, 
COALESCE(`_removed`.`claimElement`.`paymentDate`, `claimElement`.`paymentDate`) as claimElement_paymentDate, 
COALESCE(`_removed`.`claimElement`.`previousPaymentDate`, `claimElement`.`previousPaymentDate`) as claimElement_previousPaymentDate, 
COALESCE(`_removed`.`claimElement`.`yearToDateEarnings`, `claimElement`.`yearToDateEarnings`) as claimElement_yearToDateEarnings, 
COALESCE(`_removed`.`claimElement`.`yearToDateEarningsPreviousPeriod`, `claimElement`.`yearToDateEarningsPreviousPeriod`) as claimElement_yearToDateEarningsPreviousPeriod, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`_removed`.`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`claimElement`.`declaredDateTime`.`d_date`, LENGTH(`claimElement`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`claimElement`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as claimElement_declaredDateTime_d_date, 
COALESCE(`_removed`.`claimElement`.`earningsForPeriod`, `claimElement`.`earningsForPeriod`) as claimElement_earningsForPeriod, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`type`, `claimElement`.`effectiveDate`.`type`) as claimElement_effectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`effectiveDate`.`date`, `claimElement`.`effectiveDate`.`date`) as claimElement_effectiveDate_date, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`type`, `claimElement`.`paymentEffectiveDate`.`type`) as claimElement_paymentEffectiveDate_type, 
COALESCE(`_removed`.`claimElement`.`paymentEffectiveDate`.`date`, `claimElement`.`paymentEffectiveDate`.`date`) as claimElement_paymentEffectiveDate_date, 
COALESCE(`_removed`.`claimElement`.`type`, `claimElement`.`type`) as claimElement_type, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`invalidated`.`dateTime`.`d_date`, LENGTH(`_removed`.`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`invalidated`.`dateTime`.`d_date`, LENGTH(`invalidated`.`dateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`invalidated`.`dateTime`.`d_date`, 1, 10), ' ', SUBSTR(`invalidated`.`dateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`invalidated`.`dateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`invalidated`.`dateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as invalidated_dateTime_d_date, 
COALESCE(`_removed`.`invalidated`.`reason`, `invalidated`.`reason`) as invalidated_reason, 
COALESCE(`_removed`.`_id`.`d_oid`, `_id`.`d_oid`) as id FROM src_accepted_data_earningsdata_earningsdata_rte_feed_to_do
 src_accepted_data_earningsdata_earningsdata_rte_feed_to_do 
 where `_removed`.`claimElement`.`type`='RTE_FEED_TO_DO' or `claimElement`.`type`='RTE_FEED_TO_DO';


