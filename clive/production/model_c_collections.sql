use uc_clive;
!echo ------------------------;
!echo ------------------------ agent;
!echo ------------------------;
DROP TABLE IF EXISTS src_agent_core_agent_agent
;

CREATE EXTERNAL TABLE src_agent_core_agent_agent(
`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`agentId`:STRING>
,`deliveryUnits` ARRAY<STRING>
,`_version` STRING
,`agentId` STRING
,`firstName` STRING
,`lastName` STRING
,`fullName` STRING
,`sortName` STRING
,`_removed` STRUCT<`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`agentId`:STRING>
,`deliveryUnits`:ARRAY<STRING>
,`_version`:STRING
,`agentId`:STRING
,`firstName`:STRING
,`lastName`:STRING
,`fullName`:STRING
,`sortName`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/agent-core/agent';

DROP TABLE IF EXISTS agent;

CREATE TABLE agent STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(`_removed`.`agentId`, `agentId`) as agentId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`removed_exploded_deliveryunits`, `exploded_deliveryunits`) as deliveryUnits, 
COALESCE(`_removed`.`firstName`, `firstName`) as firstName, 
COALESCE(`_removed`.`lastName`, `lastName`) as lastName, 
COALESCE(`_removed`.`fullName`, `fullName`) as fullName, 
COALESCE(`_removed`.`sortName`, `sortName`) as sortName, 
COALESCE(`_removed`.`_id`.`agentId`, `_id`.`agentId`) as id FROM src_agent_core_agent_agent
 LATERAL VIEW OUTER EXPLODE(`deliveryUnits`) view_exploded_deliveryunits AS exploded_deliveryunits 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`deliveryUnits`) view_removed_exploded_deliveryunits AS removed_exploded_deliveryunits 
;


!echo ------------------------;
!echo ------------------------ claimantIdentityVerification;
!echo ------------------------;
DROP TABLE IF EXISTS src_accepted_data_claimantidentityverification_claimantidentityverification
;

CREATE EXTERNAL TABLE src_accepted_data_claimantidentityverification_claimantidentityverification(
`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`claimantIdentityVerificationId`:STRING>
,`_version` STRING
,`claimantId` STRING
,`claimantIdentityVerificationId` STRING
,`effectiveDate` STRING
,`source` STRING
,`status` STRING
,`_removed` STRUCT<`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`claimantIdentityVerificationId`:STRING>
,`_version`:STRING
,`claimantId`:STRING
,`claimantIdentityVerificationId`:STRING
,`effectiveDate`:STRING
,`source`:STRING
,`status`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/accepted-data/claimantIdentityVerification';

DROP TABLE IF EXISTS claimantIdentityVerification;

CREATE TABLE claimantidentityverification STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(`_removed`.`claimantId`, `claimantId`) as claimantId, 
COALESCE(`_removed`.`claimantIdentityVerificationId`, `claimantIdentityVerificationId`) as claimantIdentityVerificationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`effectiveDate`, `effectiveDate`) as effectiveDate, 
COALESCE(`_removed`.`source`, `source`) as source, 
COALESCE(`_removed`.`status`, `status`) as status, 
COALESCE(`_removed`.`_id`.`claimantIdentityVerificationId`, `_id`.`claimantIdentityVerificationId`) as id FROM src_accepted_data_claimantidentityverification_claimantidentityverification
 src_accepted_data_claimantidentityverification_claimantidentityverification;


!echo ------------------------;
!echo ------------------------ debtInterest;
!echo ------------------------;
DROP TABLE IF EXISTS src_deductions_debtinterest_debtinterest
;

CREATE EXTERNAL TABLE src_deductions_debtinterest_debtinterest(
`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`claimantId`:STRING>
,`_version` STRING
,`claimantId` STRING
,`hasDebtInterest` BOOLEAN
,`nino` STRING
,`_removed` STRUCT<`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`claimantId`:STRING>
,`_version`:STRING
,`claimantId`:STRING
,`hasDebtInterest`:BOOLEAN
,`nino`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/deductions/debtInterest';

DROP TABLE IF EXISTS debtInterest;

CREATE TABLE debtinterest STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(`_removed`.`claimantId`, `claimantId`) as claimantId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(CAST(`_removed`.`hasDebtInterest` as STRING), CAST(`hasDebtInterest` as STRING)) as hasDebtInterest, 
COALESCE(`_removed`.`nino`, `nino`) as nino, 
COALESCE(`_removed`.`_id`.`claimantId`, `_id`.`claimantId`) as id FROM src_deductions_debtinterest_debtinterest
 src_deductions_debtinterest_debtinterest;


!echo ------------------------;
!echo ------------------------ periodOfSickness;
!echo ------------------------;
DROP TABLE IF EXISTS src_core_periodofsickness_periodofsickness
;

CREATE EXTERNAL TABLE src_core_periodofsickness_periodofsickness(
`fitNotes` ARRAY<STRUCT<`doctorsDetails`:STRUCT<`createdDateTime`:STRUCT<`d_date`:STRING>
,`_version`:STRING
,`address`:STRING
,`name`:STRING
,`phoneNumber`:STRING
,`postcode`:STRING
,`townCity`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`_version`:STRING
,`consentToContactDoctor`:BOOLEAN
,`doctorOrMedicalCentre`:STRING
,`endDate`:STRING
,`fitNoteId`:STRING
,`startDate`:STRING
,`evidenceAccepted`:BOOLEAN
>>
,`selfCertifications` ARRAY<STRUCT<`createdDateTime`:STRUCT<`d_date`:STRING>
,`startDate`:STRING
>>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`periodOfSicknessId`:STRING>
,`_version` STRING
,`claimantId` STRING
,`contractId` STRING
,`endDate` STRING
,`periodOfSicknessId` STRING
,`startDate` STRING
,`_removed` STRUCT<`fitNotes`:ARRAY<STRUCT<`doctorsDetails`:STRUCT<`createdDateTime`:STRUCT<`d_date`:STRING>
,`_version`:STRING
,`address`:STRING
,`name`:STRING
,`phoneNumber`:STRING
,`postcode`:STRING
,`townCity`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`_version`:STRING
,`consentToContactDoctor`:BOOLEAN
,`doctorOrMedicalCentre`:STRING
,`endDate`:STRING
,`fitNoteId`:STRING
,`startDate`:STRING
,`evidenceAccepted`:BOOLEAN
>>
,`selfCertifications`:ARRAY<STRUCT<`createdDateTime`:STRUCT<`d_date`:STRING>
,`startDate`:STRING
>>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`periodOfSicknessId`:STRING>
,`_version`:STRING
,`claimantId`:STRING
,`contractId`:STRING
,`endDate`:STRING
,`periodOfSicknessId`:STRING
,`startDate`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/core/periodOfSickness';

DROP TABLE IF EXISTS periodOfSickness;

CREATE TABLE periodofsickness STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(`_removed`.`claimantId`, `claimantId`) as claimantId, 
COALESCE(`_removed`.`contractId`, `contractId`) as contractId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`endDate`, `endDate`) as endDate, 
COALESCE(`removed_exploded_fitnotes`.`_version`, `exploded_fitnotes`.`_version`) as fitNotes__version, 
COALESCE(CAST(`removed_exploded_fitnotes`.`consentToContactDoctor` as STRING), CAST(`exploded_fitnotes`.`consentToContactDoctor` as STRING)) as fitNotes_consentToContactDoctor, 
COALESCE(CASE WHEN SUBSTRING(`removed_exploded_fitnotes`.`createdDateTime`.`d_date`, LENGTH(`removed_exploded_fitnotes`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`removed_exploded_fitnotes`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`removed_exploded_fitnotes`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`removed_exploded_fitnotes`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`removed_exploded_fitnotes`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`removed_exploded_fitnotes`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`removed_exploded_fitnotes`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`exploded_fitnotes`.`createdDateTime`.`d_date`, LENGTH(`exploded_fitnotes`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`exploded_fitnotes`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`exploded_fitnotes`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`exploded_fitnotes`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`exploded_fitnotes`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`exploded_fitnotes`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`exploded_fitnotes`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as fitNotes_createdDateTime_d_date, 
COALESCE(`removed_exploded_fitnotes`.`doctorOrMedicalCentre`, `exploded_fitnotes`.`doctorOrMedicalCentre`) as fitNotes_doctorOrMedicalCentre, 
COALESCE(`removed_exploded_fitnotes`.`doctorsDetails`.`_version`, `exploded_fitnotes`.`doctorsDetails`.`_version`) as fitNotes_doctorsDetails__version, 
COALESCE(`removed_exploded_fitnotes`.`doctorsDetails`.`address`, `exploded_fitnotes`.`doctorsDetails`.`address`) as fitNotes_doctorsDetails_address, 
COALESCE(CASE WHEN SUBSTRING(`removed_exploded_fitnotes`.`doctorsDetails`.`createdDateTime`.`d_date`, LENGTH(`removed_exploded_fitnotes`.`doctorsDetails`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`removed_exploded_fitnotes`.`doctorsDetails`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`removed_exploded_fitnotes`.`doctorsDetails`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`removed_exploded_fitnotes`.`doctorsDetails`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`removed_exploded_fitnotes`.`doctorsDetails`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`removed_exploded_fitnotes`.`doctorsDetails`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`removed_exploded_fitnotes`.`doctorsDetails`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`exploded_fitnotes`.`doctorsDetails`.`createdDateTime`.`d_date`, LENGTH(`exploded_fitnotes`.`doctorsDetails`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`exploded_fitnotes`.`doctorsDetails`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`exploded_fitnotes`.`doctorsDetails`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`exploded_fitnotes`.`doctorsDetails`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`exploded_fitnotes`.`doctorsDetails`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`exploded_fitnotes`.`doctorsDetails`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`exploded_fitnotes`.`doctorsDetails`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as fitNotes_doctorsDetails_createdDateTime_d_date, 
COALESCE(`removed_exploded_fitnotes`.`doctorsDetails`.`name`, `exploded_fitnotes`.`doctorsDetails`.`name`) as fitNotes_doctorsDetails_name, 
COALESCE(`removed_exploded_fitnotes`.`doctorsDetails`.`phoneNumber`, `exploded_fitnotes`.`doctorsDetails`.`phoneNumber`) as fitNotes_doctorsDetails_phoneNumber, 
COALESCE(`removed_exploded_fitnotes`.`doctorsDetails`.`postcode`, `exploded_fitnotes`.`doctorsDetails`.`postcode`) as fitNotes_doctorsDetails_postcode, 
COALESCE(`removed_exploded_fitnotes`.`doctorsDetails`.`townCity`, `exploded_fitnotes`.`doctorsDetails`.`townCity`) as fitNotes_doctorsDetails_townCity, 
COALESCE(`removed_exploded_fitnotes`.`endDate`, `exploded_fitnotes`.`endDate`) as fitNotes_endDate, 
COALESCE(`removed_exploded_fitnotes`.`fitNoteId`, `exploded_fitnotes`.`fitNoteId`) as fitNotes_fitNoteId, 
COALESCE(`removed_exploded_fitnotes`.`startDate`, `exploded_fitnotes`.`startDate`) as fitNotes_startDate, 
COALESCE(CAST(`removed_exploded_fitnotes`.`evidenceAccepted` as STRING), CAST(`exploded_fitnotes`.`evidenceAccepted` as STRING)) as fitNotes_evidenceAccepted, 
COALESCE(`_removed`.`periodOfSicknessId`, `periodOfSicknessId`) as periodOfSicknessId, 
COALESCE(CASE WHEN SUBSTRING(`removed_exploded_selfcertifications`.`createdDateTime`.`d_date`, LENGTH(`removed_exploded_selfcertifications`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`removed_exploded_selfcertifications`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`removed_exploded_selfcertifications`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`removed_exploded_selfcertifications`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`removed_exploded_selfcertifications`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`removed_exploded_selfcertifications`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`removed_exploded_selfcertifications`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`exploded_selfcertifications`.`createdDateTime`.`d_date`, LENGTH(`exploded_selfcertifications`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`exploded_selfcertifications`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`exploded_selfcertifications`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`exploded_selfcertifications`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`exploded_selfcertifications`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`exploded_selfcertifications`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`exploded_selfcertifications`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as selfCertifications_createdDateTime_d_date, 
COALESCE(`removed_exploded_selfcertifications`.`startDate`, `exploded_selfcertifications`.`startDate`) as selfCertifications_startDate, 
COALESCE(`_removed`.`startDate`, `startDate`) as startDate, 
COALESCE(`_removed`.`_id`.`periodOfSicknessId`, `_id`.`periodOfSicknessId`) as id FROM src_core_periodofsickness_periodofsickness
 LATERAL VIEW OUTER EXPLODE(`fitNotes`) view_exploded_fitnotes AS exploded_fitnotes 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`fitNotes`) view_removed_exploded_fitnotes AS removed_exploded_fitnotes 
 LATERAL VIEW OUTER EXPLODE(`selfCertifications`) view_exploded_selfcertifications AS exploded_selfcertifications 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`selfCertifications`) view_removed_exploded_selfcertifications AS removed_exploded_selfcertifications 
;


!echo ------------------------;
!echo ------------------------ deliveryUnitAddress;
!echo ------------------------;
DROP TABLE IF EXISTS src_organisation_deliveryunitaddress_deliveryunitaddress
;

CREATE EXTERNAL TABLE src_organisation_deliveryunitaddress_deliveryunitaddress(
`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`addressId`:STRING>
,`_version` STRING
,`addressId` STRING
,`county` STRING
,`deliveryUnitId` STRING
,`line1` STRING
,`line2` STRING
,`name` STRING
,`postcode` STRING
,`town` STRING
,`_removed` STRUCT<`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`addressId`:STRING>
,`_version`:STRING
,`addressId`:STRING
,`county`:STRING
,`deliveryUnitId`:STRING
,`line1`:STRING
,`line2`:STRING
,`name`:STRING
,`postcode`:STRING
,`town`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/organisation/deliveryUnitAddress';

DROP TABLE IF EXISTS deliveryUnitAddress;

CREATE TABLE deliveryunitaddress STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(`_removed`.`addressId`, `addressId`) as addressId, 
COALESCE(`_removed`.`county`, `county`) as county, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`deliveryUnitId`, `deliveryUnitId`) as deliveryUnitId, 
COALESCE(`_removed`.`line1`, `line1`) as line1, 
COALESCE(`_removed`.`line2`, `line2`) as line2, 
COALESCE(`_removed`.`name`, `name`) as name, 
COALESCE(`_removed`.`postcode`, `postcode`) as postcode, 
COALESCE(`_removed`.`town`, `town`) as town, 
COALESCE(`_removed`.`_id`.`addressId`, `_id`.`addressId`) as id FROM src_organisation_deliveryunitaddress_deliveryunitaddress
 src_organisation_deliveryunitaddress_deliveryunitaddress;


!echo ------------------------;
!echo ------------------------ healthAndDisabilityDeclaration;
!echo ------------------------;
DROP TABLE IF EXISTS src_core_healthanddisabilitydeclaration_healthanddisabilitydeclaration
;

CREATE EXTERNAL TABLE src_core_healthanddisabilitydeclaration_healthanddisabilitydeclaration(
`medicalTreatments` ARRAY<STRUCT<`startDate`:STRING
,`treatment`:STRING
>>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`abilityToWork` STRUCT<`hasAFitNote`:BOOLEAN
,`restrictedAbilityToWork`:BOOLEAN>
,`effectiveDate` STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`hospitalStays` STRUCT<`admissionDate`:STRING
,`dischargeDate`:STRING
,`hospitalStay`:BOOLEAN>
,`paymentEffectiveDate` STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`pregnancy` STRUCT<`dueDate`:STRING
,`illnessPutsPregnancyAtRisk`:STRING>
,`supportAtWork` STRUCT<`hasGuideDog`:BOOLEAN
,`hasWheelchair`:BOOLEAN
,`needsHearingLoop`:BOOLEAN
,`otherSupport`:STRING>
,`terminalIllness` STRUCT<`prognosis`:STRING
,`terminallyIll`:BOOLEAN>
,`_id` STRUCT<`declarationId`:STRING>
,`healthConditions` ARRAY<STRING>
,`_version` STRING
,`agentToCallTerminallyIllClaimant` BOOLEAN
,`claimantId` STRING
,`contractId` STRING
,`copiedFromDeclarationId` STRING
,`declarationId` STRING
,`periodOfSicknessId` STRING
,`processId` STRING
,`reportingIllness` BOOLEAN
,`type` STRING
,`_removed` STRUCT<`medicalTreatments`:ARRAY<STRUCT<`startDate`:STRING
,`treatment`:STRING
>>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`abilityToWork`:STRUCT<`hasAFitNote`:BOOLEAN
,`restrictedAbilityToWork`:BOOLEAN>
,`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`hospitalStays`:STRUCT<`admissionDate`:STRING
,`dischargeDate`:STRING
,`hospitalStay`:BOOLEAN>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`pregnancy`:STRUCT<`dueDate`:STRING
,`illnessPutsPregnancyAtRisk`:STRING>
,`supportAtWork`:STRUCT<`hasGuideDog`:BOOLEAN
,`hasWheelchair`:BOOLEAN
,`needsHearingLoop`:BOOLEAN
,`otherSupport`:STRING>
,`terminalIllness`:STRUCT<`prognosis`:STRING
,`terminallyIll`:BOOLEAN>
,`_id`:STRUCT<`declarationId`:STRING>
,`healthConditions`:ARRAY<STRING>
,`_version`:STRING
,`agentToCallTerminallyIllClaimant`:BOOLEAN
,`claimantId`:STRING
,`contractId`:STRING
,`copiedFromDeclarationId`:STRING
,`declarationId`:STRING
,`periodOfSicknessId`:STRING
,`processId`:STRING
,`reportingIllness`:BOOLEAN
,`type`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/core/healthAndDisabilityDeclaration';

DROP TABLE IF EXISTS healthAndDisabilityDeclaration;

CREATE TABLE healthanddisabilitydeclaration STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CAST(`_removed`.`abilityToWork`.`hasAFitNote` as STRING), CAST(`abilityToWork`.`hasAFitNote` as STRING)) as abilityToWork_hasAFitNote, 
COALESCE(CAST(`_removed`.`abilityToWork`.`restrictedAbilityToWork` as STRING), CAST(`abilityToWork`.`restrictedAbilityToWork` as STRING)) as abilityToWork_restrictedAbilityToWork, 
COALESCE(CAST(`_removed`.`agentToCallTerminallyIllClaimant` as STRING), CAST(`agentToCallTerminallyIllClaimant` as STRING)) as agentToCallTerminallyIllClaimant, 
COALESCE(`_removed`.`claimantId`, `claimantId`) as claimantId, 
COALESCE(`_removed`.`contractId`, `contractId`) as contractId, 
COALESCE(`_removed`.`copiedFromDeclarationId`, `copiedFromDeclarationId`) as copiedFromDeclarationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CAST(`_removed`.`effectiveDate`.`hasDate` as STRING), CAST(`effectiveDate`.`hasDate` as STRING)) as effectiveDate_hasDate, 
COALESCE(`_removed`.`effectiveDate`.`knownDate`, `effectiveDate`.`knownDate`) as effectiveDate_knownDate, 
COALESCE(`_removed`.`effectiveDate`.`type`, `effectiveDate`.`type`) as effectiveDate_type, 
COALESCE(`_removed`.`effectiveDate`.`date`, `effectiveDate`.`date`) as effectiveDate_date, 
COALESCE(`removed_exploded_healthconditions`, `exploded_healthconditions`) as healthConditions, 
COALESCE(`_removed`.`hospitalStays`.`admissionDate`, `hospitalStays`.`admissionDate`) as hospitalStays_admissionDate, 
COALESCE(`_removed`.`hospitalStays`.`dischargeDate`, `hospitalStays`.`dischargeDate`) as hospitalStays_dischargeDate, 
COALESCE(CAST(`_removed`.`hospitalStays`.`hospitalStay` as STRING), CAST(`hospitalStays`.`hospitalStay` as STRING)) as hospitalStays_hospitalStay, 
COALESCE(`removed_exploded_medicaltreatments`.`startDate`, `exploded_medicaltreatments`.`startDate`) as medicalTreatments_startDate, 
COALESCE(`removed_exploded_medicaltreatments`.`treatment`, `exploded_medicaltreatments`.`treatment`) as medicalTreatments_treatment, 
COALESCE(CAST(`_removed`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`paymentEffectiveDate`.`hasDate` as STRING)) as paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`paymentEffectiveDate`.`knownDate`, `paymentEffectiveDate`.`knownDate`) as paymentEffectiveDate_knownDate, 
COALESCE(`_removed`.`paymentEffectiveDate`.`type`, `paymentEffectiveDate`.`type`) as paymentEffectiveDate_type, 
COALESCE(`_removed`.`paymentEffectiveDate`.`date`, `paymentEffectiveDate`.`date`) as paymentEffectiveDate_date, 
COALESCE(`_removed`.`periodOfSicknessId`, `periodOfSicknessId`) as periodOfSicknessId, 
COALESCE(`_removed`.`pregnancy`.`dueDate`, `pregnancy`.`dueDate`) as pregnancy_dueDate, 
COALESCE(`_removed`.`pregnancy`.`illnessPutsPregnancyAtRisk`, `pregnancy`.`illnessPutsPregnancyAtRisk`) as pregnancy_illnessPutsPregnancyAtRisk, 
COALESCE(`_removed`.`processId`, `processId`) as processId, 
COALESCE(CAST(`_removed`.`reportingIllness` as STRING), CAST(`reportingIllness` as STRING)) as reportingIllness, 
COALESCE(CAST(`_removed`.`supportAtWork`.`hasGuideDog` as STRING), CAST(`supportAtWork`.`hasGuideDog` as STRING)) as supportAtWork_hasGuideDog, 
COALESCE(CAST(`_removed`.`supportAtWork`.`hasWheelchair` as STRING), CAST(`supportAtWork`.`hasWheelchair` as STRING)) as supportAtWork_hasWheelchair, 
COALESCE(CAST(`_removed`.`supportAtWork`.`needsHearingLoop` as STRING), CAST(`supportAtWork`.`needsHearingLoop` as STRING)) as supportAtWork_needsHearingLoop, 
COALESCE(`_removed`.`supportAtWork`.`otherSupport`, `supportAtWork`.`otherSupport`) as supportAtWork_otherSupport, 
COALESCE(`_removed`.`terminalIllness`.`prognosis`, `terminalIllness`.`prognosis`) as terminalIllness_prognosis, 
COALESCE(CAST(`_removed`.`terminalIllness`.`terminallyIll` as STRING), CAST(`terminalIllness`.`terminallyIll` as STRING)) as terminalIllness_terminallyIll, 
COALESCE(`_removed`.`type`, `type`) as type, 
COALESCE(`_removed`.`_id`.`declarationId`, `_id`.`declarationId`) as id FROM src_core_healthanddisabilitydeclaration_healthanddisabilitydeclaration
 LATERAL VIEW OUTER EXPLODE(`healthConditions`) view_exploded_healthconditions AS exploded_healthconditions 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`healthConditions`) view_removed_exploded_healthconditions AS removed_exploded_healthconditions 
 LATERAL VIEW OUTER EXPLODE(`medicalTreatments`) view_exploded_medicaltreatments AS exploded_medicaltreatments 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`medicalTreatments`) view_removed_exploded_medicaltreatments AS removed_exploded_medicaltreatments 
;


!echo ------------------------;
!echo ------------------------ appointment;
!echo ------------------------;
DROP TABLE IF EXISTS src_appointments_appointment_appointment
;

CREATE EXTERNAL TABLE src_appointments_appointment_appointment(
`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`appointmentDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`appointmentId`:STRING>
,`_version` STRING
,`addressId` STRING
,`agentNotes` STRING
,`appointmentChannel` STRING
,`appointmentType` STRING
,`bookedWithAgentId` STRING
,`claimantId` STRING
,`claimantInstructions` STRING
,`claimantToDoId` STRING
,`completedByAgentId` STRING
,`consolidatedAppointmentType` STRING
,`contractId` STRING
,`otherAppointmentName` STRING
,`recordNotes` BOOLEAN
,`specialRequirements` STRING
,`status` STRING
,`thirdPartyAppointmentReference` STRING
,`_removed` STRUCT<`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`appointmentDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`appointmentId`:STRING>
,`_version`:STRING
,`addressId`:STRING
,`agentNotes`:STRING
,`appointmentChannel`:STRING
,`appointmentType`:STRING
,`bookedWithAgentId`:STRING
,`claimantId`:STRING
,`claimantInstructions`:STRING
,`claimantToDoId`:STRING
,`completedByAgentId`:STRING
,`consolidatedAppointmentType`:STRING
,`contractId`:STRING
,`otherAppointmentName`:STRING
,`recordNotes`:BOOLEAN
,`specialRequirements`:STRING
,`status`:STRING
,`thirdPartyAppointmentReference`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/appointments/appointment';

DROP TABLE IF EXISTS appointment;

CREATE TABLE appointment STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(`_removed`.`addressId`, `addressId`) as addressId, 
COALESCE(`_removed`.`agentNotes`, `agentNotes`) as agentNotes, 
COALESCE(`_removed`.`appointmentChannel`, `appointmentChannel`) as appointmentChannel, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`appointmentDateTime`.`d_date`, LENGTH(`_removed`.`appointmentDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`appointmentDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`appointmentDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`appointmentDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`appointmentDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`appointmentDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`appointmentDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`appointmentDateTime`.`d_date`, LENGTH(`appointmentDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`appointmentDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`appointmentDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`appointmentDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`appointmentDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`appointmentDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`appointmentDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as appointmentDateTime_d_date, 
COALESCE(`_removed`.`appointmentType`, `appointmentType`) as appointmentType, 
COALESCE(`_removed`.`bookedWithAgentId`, `bookedWithAgentId`) as bookedWithAgentId, 
COALESCE(`_removed`.`claimantId`, `claimantId`) as claimantId, 
COALESCE(`_removed`.`claimantInstructions`, `claimantInstructions`) as claimantInstructions, 
COALESCE(`_removed`.`claimantToDoId`, `claimantToDoId`) as claimantToDoId, 
COALESCE(`_removed`.`completedByAgentId`, `completedByAgentId`) as completedByAgentId, 
COALESCE(`_removed`.`consolidatedAppointmentType`, `consolidatedAppointmentType`) as consolidatedAppointmentType, 
COALESCE(`_removed`.`contractId`, `contractId`) as contractId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`otherAppointmentName`, `otherAppointmentName`) as otherAppointmentName, 
COALESCE(CAST(`_removed`.`recordNotes` as STRING), CAST(`recordNotes` as STRING)) as recordNotes, 
COALESCE(`_removed`.`specialRequirements`, `specialRequirements`) as specialRequirements, 
COALESCE(`_removed`.`status`, `status`) as status, 
COALESCE(`_removed`.`thirdPartyAppointmentReference`, `thirdPartyAppointmentReference`) as thirdPartyAppointmentReference, 
COALESCE(`_removed`.`_id`.`appointmentId`, `_id`.`appointmentId`) as id FROM src_appointments_appointment_appointment
 src_appointments_appointment_appointment;


!echo ------------------------;
!echo ------------------------ agentWorkGroupAllocation;
!echo ------------------------;
DROP TABLE IF EXISTS src_agent_core_agentworkgroupallocation_agentworkgroupallocation
;

CREATE EXTERNAL TABLE src_agent_core_agentworkgroupallocation_agentworkgroupallocation(
`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`agentWorkGroupAllocationId`:STRING>
,`_version` STRING
,`agentWorkGroupAllocationId` STRING
,`claimantId` STRING
,`contractId` STRING
,`effectiveDate` STRING
,`endDate` STRING
,`endReason` STRING
,`overriddenWorkGroup` STRING
,`overrideReason` STRING
,`removed` BOOLEAN
,`removedReason` STRING
,`_removed` STRUCT<`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`agentWorkGroupAllocationId`:STRING>
,`_version`:STRING
,`agentWorkGroupAllocationId`:STRING
,`claimantId`:STRING
,`contractId`:STRING
,`effectiveDate`:STRING
,`endDate`:STRING
,`endReason`:STRING
,`overriddenWorkGroup`:STRING
,`overrideReason`:STRING
,`removed`:BOOLEAN
,`removedReason`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/agent-core/agentWorkGroupAllocation';

DROP TABLE IF EXISTS agentWorkGroupAllocation;

CREATE TABLE agentworkgroupallocation STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(`_removed`.`agentWorkGroupAllocationId`, `agentWorkGroupAllocationId`) as agentWorkGroupAllocationId, 
COALESCE(`_removed`.`claimantId`, `claimantId`) as claimantId, 
COALESCE(`_removed`.`contractId`, `contractId`) as contractId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`effectiveDate`, `effectiveDate`) as effectiveDate, 
COALESCE(`_removed`.`endDate`, `endDate`) as endDate, 
COALESCE(`_removed`.`endReason`, `endReason`) as endReason, 
COALESCE(`_removed`.`overriddenWorkGroup`, `overriddenWorkGroup`) as overriddenWorkGroup, 
COALESCE(`_removed`.`overrideReason`, `overrideReason`) as overrideReason, 
COALESCE(CAST(`_removed`.`removed` as STRING), CAST(`removed` as STRING)) as removed, 
COALESCE(`_removed`.`removedReason`, `removedReason`) as removedReason, 
COALESCE(`_removed`.`_id`.`agentWorkGroupAllocationId`, `_id`.`agentWorkGroupAllocationId`) as id FROM src_agent_core_agentworkgroupallocation_agentworkgroupallocation
 src_agent_core_agentworkgroupallocation_agentworkgroupallocation;


!echo ------------------------;
!echo ------------------------ proveYourIdentityTrial;
!echo ------------------------;
DROP TABLE IF EXISTS src_core_proveyouridentitytrial_proveyouridentitytrial
;

CREATE EXTERNAL TABLE src_core_proveyouridentitytrial_proveyouridentitytrial(
`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`contractId`:STRING>
,`_version` STRING
,`contractId` STRING
,`_removed` STRUCT<`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`contractId`:STRING>
,`_version`:STRING
,`contractId`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/core/proveYourIdentityTrial';

DROP TABLE IF EXISTS proveYourIdentityTrial;

CREATE TABLE proveyouridentitytrial STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(`_removed`.`contractId`, `contractId`) as contractId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`_id`.`contractId`, `_id`.`contractId`) as id FROM src_core_proveyouridentitytrial_proveyouridentitytrial
 src_core_proveyouridentitytrial_proveyouridentitytrial;


!echo ------------------------;
!echo ------------------------ childDeclaration;
!echo ------------------------;
DROP TABLE IF EXISTS src_core_childdeclaration_childdeclaration
;

CREATE EXTERNAL TABLE src_core_childdeclaration_childdeclaration(
`children` ARRAY<STRUCT<`disabilityDetails`:STRUCT<`dlaCareComponent`:STRING
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
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`effectiveDate` STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate` STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`_id` STRUCT<`declarationId`:STRING>
,`_version` STRING
,`contractId` STRING
,`declarationId` STRING
,`hasExceptionToTwoChildPolicy` BOOLEAN
,`primaryCarerId` STRING
,`processId` STRING
,`type` STRING
,`_removed` STRUCT<`children`:ARRAY<STRUCT<`disabilityDetails`:STRUCT<`dlaCareComponent`:STRING
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
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`_id`:STRUCT<`declarationId`:STRING>
,`_version`:STRING
,`contractId`:STRING
,`declarationId`:STRING
,`hasExceptionToTwoChildPolicy`:BOOLEAN
,`primaryCarerId`:STRING
,`processId`:STRING
,`type`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/core/childDeclaration';

DROP TABLE IF EXISTS childDeclaration;

CREATE TABLE childdeclaration STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CAST(`removed_exploded_children`.`adoptedOrUnderGuardianship` as STRING), CAST(`exploded_children`.`adoptedOrUnderGuardianship` as STRING)) as children_adoptedOrUnderGuardianship, 
COALESCE(`removed_exploded_children`.`dateOfBirth`, `exploded_children`.`dateOfBirth`) as children_dateOfBirth, 
COALESCE(`removed_exploded_children`.`declarationDate`, `exploded_children`.`declarationDate`) as children_declarationDate, 
COALESCE(`removed_exploded_children`.`disabilityDetails`.`dlaCareComponent`, `exploded_children`.`disabilityDetails`.`dlaCareComponent`) as children_disabilityDetails_dlaCareComponent, 
COALESCE(`removed_exploded_children`.`disabilityDetails`.`pipDailyLivingComponent`, `exploded_children`.`disabilityDetails`.`pipDailyLivingComponent`) as children_disabilityDetails_pipDailyLivingComponent, 
COALESCE(CAST(`removed_exploded_children`.`disabilityDetails`.`receivingDisabilityLivingAllowance` as STRING), CAST(`exploded_children`.`disabilityDetails`.`receivingDisabilityLivingAllowance` as STRING)) as children_disabilityDetails_receivingDisabilityLivingAllowance, 
COALESCE(CAST(`removed_exploded_children`.`disabilityDetails`.`receivingPersonalIndependencePayment` as STRING), CAST(`exploded_children`.`disabilityDetails`.`receivingPersonalIndependencePayment` as STRING)) as children_disabilityDetails_receivingPersonalIndependencePayment, 
COALESCE(CAST(`removed_exploded_children`.`disabilityDetails`.`registeredBlind` as STRING), CAST(`exploded_children`.`disabilityDetails`.`registeredBlind` as STRING)) as children_disabilityDetails_registeredBlind, 
COALESCE(`removed_exploded_children`.`fullName`, `exploded_children`.`fullName`) as children_fullName, 
COALESCE(`removed_exploded_children`.`gender`, `exploded_children`.`gender`) as children_gender, 
COALESCE(`removed_exploded_children`.`id`, `exploded_children`.`id`) as children_id, 
COALESCE(CAST(`removed_exploded_children`.`inQualifyingFullTimeEducation` as STRING), CAST(`exploded_children`.`inQualifyingFullTimeEducation` as STRING)) as children_inQualifyingFullTimeEducation, 
COALESCE(CAST(`removed_exploded_children`.`nonDependantDetails`.`awayOnArmedForcesOperations` as STRING), CAST(`exploded_children`.`nonDependantDetails`.`awayOnArmedForcesOperations` as STRING)) as children_nonDependantDetails_awayOnArmedForcesOperations, 
COALESCE(CAST(`removed_exploded_children`.`nonDependantDetails`.`personIsClaimantsChild` as STRING), CAST(`exploded_children`.`nonDependantDetails`.`personIsClaimantsChild` as STRING)) as children_nonDependantDetails_personIsClaimantsChild, 
COALESCE(`removed_exploded_children`.`nonDependantDetails`.`primaryCarer`, `exploded_children`.`nonDependantDetails`.`primaryCarer`) as children_nonDependantDetails_primaryCarer, 
COALESCE(CAST(`removed_exploded_children`.`nonDependantDetails`.`receivingArmedForcesIndependencePayment` as STRING), CAST(`exploded_children`.`nonDependantDetails`.`receivingArmedForcesIndependencePayment` as STRING)) as children_nonDependantDetails_receivingArmedForcesIndependencePayment, 
COALESCE(CAST(`removed_exploded_children`.`nonDependantDetails`.`receivingAttendanceAllowance` as STRING), CAST(`exploded_children`.`nonDependantDetails`.`receivingAttendanceAllowance` as STRING)) as children_nonDependantDetails_receivingAttendanceAllowance, 
COALESCE(CAST(`removed_exploded_children`.`nonDependantDetails`.`receivingCarersAllowance` as STRING), CAST(`exploded_children`.`nonDependantDetails`.`receivingCarersAllowance` as STRING)) as children_nonDependantDetails_receivingCarersAllowance, 
COALESCE(CAST(`removed_exploded_children`.`nonDependantDetails`.`receivingDisabilityLivingAllowance` as STRING), CAST(`exploded_children`.`nonDependantDetails`.`receivingDisabilityLivingAllowance` as STRING)) as children_nonDependantDetails_receivingDisabilityLivingAllowance, 
COALESCE(CAST(`removed_exploded_children`.`nonDependantDetails`.`receivingPersonalIndependencePayment` as STRING), CAST(`exploded_children`.`nonDependantDetails`.`receivingPersonalIndependencePayment` as STRING)) as children_nonDependantDetails_receivingPersonalIndependencePayment, 
COALESCE(CAST(`removed_exploded_children`.`nonDependantDetails`.`receivingStatePensionCredit` as STRING), CAST(`exploded_children`.`nonDependantDetails`.`receivingStatePensionCredit` as STRING)) as children_nonDependantDetails_receivingStatePensionCredit, 
COALESCE(`removed_exploded_children`.`removalDate`, `exploded_children`.`removalDate`) as children_removalDate, 
COALESCE(CAST(`removed_exploded_children`.`temporaryAbsence`.`abroadForMoreThanOneMonth` as STRING), CAST(`exploded_children`.`temporaryAbsence`.`abroadForMoreThanOneMonth` as STRING)) as children_temporaryAbsence_abroadForMoreThanOneMonth, 
COALESCE(CAST(`removed_exploded_children`.`temporaryAbsence`.`inPrison` as STRING), CAST(`exploded_children`.`temporaryAbsence`.`inPrison` as STRING)) as children_temporaryAbsence_inPrison, 
COALESCE(CAST(`removed_exploded_children`.`temporaryAbsence`.`inTheCareOfLocalAuthority` as STRING), CAST(`exploded_children`.`temporaryAbsence`.`inTheCareOfLocalAuthority` as STRING)) as children_temporaryAbsence_inTheCareOfLocalAuthority, 
COALESCE(`_removed`.`contractId`, `contractId`) as contractId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CAST(`_removed`.`effectiveDate`.`hasDate` as STRING), CAST(`effectiveDate`.`hasDate` as STRING)) as effectiveDate_hasDate, 
COALESCE(`_removed`.`effectiveDate`.`knownDate`, `effectiveDate`.`knownDate`) as effectiveDate_knownDate, 
COALESCE(`_removed`.`effectiveDate`.`type`, `effectiveDate`.`type`) as effectiveDate_type, 
COALESCE(`_removed`.`effectiveDate`.`date`, `effectiveDate`.`date`) as effectiveDate_date, 
COALESCE(CAST(`_removed`.`hasExceptionToTwoChildPolicy` as STRING), CAST(`hasExceptionToTwoChildPolicy` as STRING)) as hasExceptionToTwoChildPolicy, 
COALESCE(CAST(`_removed`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`paymentEffectiveDate`.`hasDate` as STRING)) as paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`paymentEffectiveDate`.`knownDate`, `paymentEffectiveDate`.`knownDate`) as paymentEffectiveDate_knownDate, 
COALESCE(`_removed`.`paymentEffectiveDate`.`type`, `paymentEffectiveDate`.`type`) as paymentEffectiveDate_type, 
COALESCE(`_removed`.`paymentEffectiveDate`.`date`, `paymentEffectiveDate`.`date`) as paymentEffectiveDate_date, 
COALESCE(`_removed`.`primaryCarerId`, `primaryCarerId`) as primaryCarerId, 
COALESCE(`_removed`.`processId`, `processId`) as processId, 
COALESCE(`_removed`.`type`, `type`) as type, 
COALESCE(`_removed`.`_id`.`declarationId`, `_id`.`declarationId`) as id FROM src_core_childdeclaration_childdeclaration
 LATERAL VIEW OUTER EXPLODE(`children`) view_exploded_children AS exploded_children 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`children`) view_removed_exploded_children AS removed_exploded_children 
;




!echo ------------------------;
!echo ------------------------ sanction;
!echo ------------------------;
DROP TABLE IF EXISTS src_penalties_and_deductions_sanction_sanction
;

CREATE EXTERNAL TABLE src_penalties_and_deductions_sanction_sanction(
`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`sanctionId`:STRING>
,`_version` STRING
,`dateOfFailure` STRING
,`decisionDate` STRING
,`deleteExplanation` STRING
,`deleted` BOOLEAN
,`durationInDays` STRING
,`personId` STRING
,`sanctionId` STRING
,`sanctionLevel` STRING
,`transferredSanction` BOOLEAN
,`universalCreditSanction` BOOLEAN
,`originalBenefitType` STRING
,`type` STRING
,`failureType` STRING
,`sanctionablePeriodEndDate` STRING
,`complianceDate` STRING
,`complianceRequirement` STRING
,`workGroupChangeOpenEndedPeriodEndDate` STRING
,`_removed` STRUCT<`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`sanctionId`:STRING>
,`_version`:STRING
,`dateOfFailure`:STRING
,`decisionDate`:STRING
,`deleteExplanation`:STRING
,`deleted`:BOOLEAN
,`durationInDays`:STRING
,`personId`:STRING
,`sanctionId`:STRING
,`sanctionLevel`:STRING
,`transferredSanction`:BOOLEAN
,`universalCreditSanction`:BOOLEAN
,`originalBenefitType`:STRING
,`type`:STRING
,`failureType`:STRING
,`sanctionablePeriodEndDate`:STRING
,`complianceDate`:STRING
,`complianceRequirement`:STRING
,`workGroupChangeOpenEndedPeriodEndDate`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/penalties-and-deductions/sanction';

DROP TABLE IF EXISTS sanction;

CREATE TABLE sanction STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`dateOfFailure`, `dateOfFailure`) as dateOfFailure, 
COALESCE(`_removed`.`decisionDate`, `decisionDate`) as decisionDate, 
COALESCE(`_removed`.`deleteExplanation`, `deleteExplanation`) as deleteExplanation, 
COALESCE(CAST(`_removed`.`deleted` as STRING), CAST(`deleted` as STRING)) as deleted, 
COALESCE(`_removed`.`durationInDays`, `durationInDays`) as durationInDays, 
COALESCE(`_removed`.`personId`, `personId`) as personId, 
COALESCE(`_removed`.`sanctionId`, `sanctionId`) as sanctionId, 
COALESCE(`_removed`.`sanctionLevel`, `sanctionLevel`) as sanctionLevel, 
COALESCE(CAST(`_removed`.`transferredSanction` as STRING), CAST(`transferredSanction` as STRING)) as transferredSanction, 
COALESCE(CAST(`_removed`.`universalCreditSanction` as STRING), CAST(`universalCreditSanction` as STRING)) as universalCreditSanction, 
COALESCE(`_removed`.`originalBenefitType`, `originalBenefitType`) as originalBenefitType, 
COALESCE(`_removed`.`type`, `type`) as type, 
COALESCE(`_removed`.`failureType`, `failureType`) as failureType, 
COALESCE(`_removed`.`sanctionablePeriodEndDate`, `sanctionablePeriodEndDate`) as sanctionablePeriodEndDate, 
COALESCE(`_removed`.`complianceDate`, `complianceDate`) as complianceDate, 
COALESCE(`_removed`.`complianceRequirement`, `complianceRequirement`) as complianceRequirement, 
COALESCE(`_removed`.`workGroupChangeOpenEndedPeriodEndDate`, `workGroupChangeOpenEndedPeriodEndDate`) as workGroupChangeOpenEndedPeriodEndDate, 
COALESCE(`_removed`.`_id`.`sanctionId`, `_id`.`sanctionId`) as id FROM src_penalties_and_deductions_sanction_sanction
 src_penalties_and_deductions_sanction_sanction;


!echo ------------------------;
!echo ------------------------ childcareDeclaration;
!echo ------------------------;
DROP TABLE IF EXISTS src_core_childcaredeclaration_childcaredeclaration
;

CREATE EXTERNAL TABLE src_core_childcaredeclaration_childcaredeclaration(
`declaredChildcareProviders` ARRAY<STRUCT<`childcareCosts`:ARRAY<STRUCT<`children`:ARRAY<STRUCT<`ageInYears`:STRING
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
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`effectiveDate` STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate` STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`_id` STRUCT<`declarationId`:STRING>
,`updatedChildcareCostIds` ARRAY<STRING>
,`_version` STRING
,`contractId` STRING
,`declarationId` STRING
,`hasChildcareCosts` BOOLEAN
,`processId` STRING
,`type` STRING
,`_removed` STRUCT<`declaredChildcareProviders`:ARRAY<STRUCT<`childcareCosts`:ARRAY<STRUCT<`children`:ARRAY<STRUCT<`ageInYears`:STRING
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
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`_id`:STRUCT<`declarationId`:STRING>
,`updatedChildcareCostIds`:ARRAY<STRING>
,`_version`:STRING
,`contractId`:STRING
,`declarationId`:STRING
,`hasChildcareCosts`:BOOLEAN
,`processId`:STRING
,`type`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/core/childcareDeclaration';

DROP TABLE IF EXISTS childcareDeclaration;

CREATE TABLE childcaredeclaration STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(`_removed`.`contractId`, `contractId`) as contractId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(`removed_exploded_declaredchildcareproviders_exploded_childcarecosts`.`amountPaid`, `exploded_declaredchildcareproviders_exploded_childcarecosts`.`amountPaid`) as declaredChildcareProviders_childcareCosts_amountPaid, 
COALESCE(`removed_exploded_declaredchildcareproviders_exploded_childcarecosts_exploded_children`.`ageInYears`, `exploded_declaredchildcareproviders_exploded_childcarecosts_exploded_children`.`ageInYears`) as declaredChildcareProviders_childcareCosts_children_ageInYears, 
COALESCE(`removed_exploded_declaredchildcareproviders_exploded_childcarecosts_exploded_children`.`childId`, `exploded_declaredchildcareproviders_exploded_childcarecosts_exploded_children`.`childId`) as declaredChildcareProviders_childcareCosts_children_childId, 
COALESCE(`removed_exploded_declaredchildcareproviders_exploded_childcarecosts_exploded_children`.`firstName`, `exploded_declaredchildcareproviders_exploded_childcarecosts_exploded_children`.`firstName`) as declaredChildcareProviders_childcareCosts_children_firstName, 
COALESCE(`removed_exploded_declaredchildcareproviders_exploded_childcarecosts_exploded_children`.`gender`, `exploded_declaredchildcareproviders_exploded_childcarecosts_exploded_children`.`gender`) as declaredChildcareProviders_childcareCosts_children_gender, 
COALESCE(`removed_exploded_declaredchildcareproviders_exploded_childcarecosts_exploded_children`.`lastName`, `exploded_declaredchildcareproviders_exploded_childcarecosts_exploded_children`.`lastName`) as declaredChildcareProviders_childcareCosts_children_lastName, 
COALESCE(`removed_exploded_declaredchildcareproviders_exploded_childcarecosts`.`dateDeclared`, `exploded_declaredchildcareproviders_exploded_childcarecosts`.`dateDeclared`) as declaredChildcareProviders_childcareCosts_dateDeclared, 
COALESCE(`removed_exploded_declaredchildcareproviders_exploded_childcarecosts`.`datePaid`, `exploded_declaredchildcareproviders_exploded_childcarecosts`.`datePaid`) as declaredChildcareProviders_childcareCosts_datePaid, 
COALESCE(`removed_exploded_declaredchildcareproviders_exploded_childcarecosts`.`dateVerified`, `exploded_declaredchildcareproviders_exploded_childcarecosts`.`dateVerified`) as declaredChildcareProviders_childcareCosts_dateVerified, 
COALESCE(`removed_exploded_declaredchildcareproviders_exploded_childcarecosts`.`id`, `exploded_declaredchildcareproviders_exploded_childcarecosts`.`id`) as declaredChildcareProviders_childcareCosts_id, 
COALESCE(`removed_exploded_declaredchildcareproviders_exploded_childcarecosts`.`periodEnd`, `exploded_declaredchildcareproviders_exploded_childcarecosts`.`periodEnd`) as declaredChildcareProviders_childcareCosts_periodEnd, 
COALESCE(`removed_exploded_declaredchildcareproviders_exploded_childcarecosts`.`periodStart`, `exploded_declaredchildcareproviders_exploded_childcarecosts`.`periodStart`) as declaredChildcareProviders_childcareCosts_periodStart, 
COALESCE(`removed_exploded_declaredchildcareproviders_exploded_childcarecosts`.`providerId`, `exploded_declaredchildcareproviders_exploded_childcarecosts`.`providerId`) as declaredChildcareProviders_childcareCosts_providerId, 
COALESCE(`removed_exploded_declaredchildcareproviders`.`childcareProvider`.`address1`, `exploded_declaredchildcareproviders`.`childcareProvider`.`address1`) as declaredChildcareProviders_childcareProvider_address1, 
COALESCE(`removed_exploded_declaredchildcareproviders`.`childcareProvider`.`address2`, `exploded_declaredchildcareproviders`.`childcareProvider`.`address2`) as declaredChildcareProviders_childcareProvider_address2, 
COALESCE(`removed_exploded_declaredchildcareproviders`.`childcareProvider`.`id`, `exploded_declaredchildcareproviders`.`childcareProvider`.`id`) as declaredChildcareProviders_childcareProvider_id, 
COALESCE(`removed_exploded_declaredchildcareproviders`.`childcareProvider`.`phoneNumber`, `exploded_declaredchildcareproviders`.`childcareProvider`.`phoneNumber`) as declaredChildcareProviders_childcareProvider_phoneNumber, 
COALESCE(`removed_exploded_declaredchildcareproviders`.`childcareProvider`.`postcode`, `exploded_declaredchildcareproviders`.`childcareProvider`.`postcode`) as declaredChildcareProviders_childcareProvider_postcode, 
COALESCE(`removed_exploded_declaredchildcareproviders`.`childcareProvider`.`providerName`, `exploded_declaredchildcareproviders`.`childcareProvider`.`providerName`) as declaredChildcareProviders_childcareProvider_providerName, 
COALESCE(`removed_exploded_declaredchildcareproviders`.`childcareProvider`.`registrationNumber`, `exploded_declaredchildcareproviders`.`childcareProvider`.`registrationNumber`) as declaredChildcareProviders_childcareProvider_registrationNumber, 
COALESCE(`removed_exploded_declaredchildcareproviders`.`childcareProvider`.`town`, `exploded_declaredchildcareproviders`.`childcareProvider`.`town`) as declaredChildcareProviders_childcareProvider_town, 
COALESCE(CAST(`_removed`.`effectiveDate`.`hasDate` as STRING), CAST(`effectiveDate`.`hasDate` as STRING)) as effectiveDate_hasDate, 
COALESCE(`_removed`.`effectiveDate`.`knownDate`, `effectiveDate`.`knownDate`) as effectiveDate_knownDate, 
COALESCE(`_removed`.`effectiveDate`.`type`, `effectiveDate`.`type`) as effectiveDate_type, 
COALESCE(`_removed`.`effectiveDate`.`date`, `effectiveDate`.`date`) as effectiveDate_date, 
COALESCE(CAST(`_removed`.`hasChildcareCosts` as STRING), CAST(`hasChildcareCosts` as STRING)) as hasChildcareCosts, 
COALESCE(CAST(`_removed`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`paymentEffectiveDate`.`hasDate` as STRING)) as paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`paymentEffectiveDate`.`knownDate`, `paymentEffectiveDate`.`knownDate`) as paymentEffectiveDate_knownDate, 
COALESCE(`_removed`.`paymentEffectiveDate`.`type`, `paymentEffectiveDate`.`type`) as paymentEffectiveDate_type, 
COALESCE(`_removed`.`paymentEffectiveDate`.`date`, `paymentEffectiveDate`.`date`) as paymentEffectiveDate_date, 
COALESCE(`_removed`.`processId`, `processId`) as processId, 
COALESCE(`removed_exploded_updatedchildcarecostids`, `exploded_updatedchildcarecostids`) as updatedChildcareCostIds, 
COALESCE(`_removed`.`type`, `type`) as type, 
COALESCE(`_removed`.`_id`.`declarationId`, `_id`.`declarationId`) as id FROM src_core_childcaredeclaration_childcaredeclaration
 LATERAL VIEW OUTER EXPLODE(`declaredChildcareProviders`) view_exploded_declaredchildcareproviders AS exploded_declaredchildcareproviders 
 LATERAL VIEW OUTER EXPLODE(`exploded_declaredchildcareproviders`.`childcareCosts`) view_exploded_declaredchildcareproviders_exploded_childcarecosts AS exploded_declaredchildcareproviders_exploded_childcarecosts 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`declaredChildcareProviders`) view_removed_exploded_declaredchildcareproviders AS removed_exploded_declaredchildcareproviders 
 LATERAL VIEW OUTER EXPLODE(`removed_exploded_declaredchildcareproviders`.`childcareCosts`) view_removed_exploded_declaredchildcareproviders_exploded_childcarecosts AS removed_exploded_declaredchildcareproviders_exploded_childcarecosts 
 LATERAL VIEW OUTER EXPLODE(`exploded_declaredchildcareproviders_exploded_childcarecosts`.`children`) view_exploded_declaredchildcareproviders_exploded_childcarecosts_exploded_children AS exploded_declaredchildcareproviders_exploded_childcarecosts_exploded_children 
 LATERAL VIEW OUTER EXPLODE(`removed_exploded_declaredchildcareproviders_exploded_childcarecosts`.`children`) view_removed_exploded_declaredchildcareproviders_exploded_childcarecosts_exploded_children AS removed_exploded_declaredchildcareproviders_exploded_childcarecosts_exploded_children 
 LATERAL VIEW OUTER EXPLODE(`updatedChildcareCostIds`) view_exploded_updatedchildcarecostids AS exploded_updatedchildcarecostids 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`updatedChildcareCostIds`) view_removed_exploded_updatedchildcarecostids AS removed_exploded_updatedchildcarecostids 
;


!echo ------------------------;
!echo ------------------------ recoverableHardshipPayment;
!echo ------------------------;
DROP TABLE IF EXISTS src_advances_recoverablehardshippayment_recoverablehardshippayment
;

CREATE EXTERNAL TABLE src_advances_recoverablehardshippayment_recoverablehardshippayment(
`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`recoverableHardshipPaymentId`:STRING>
,`claimantIds` ARRAY<STRING>
,`_version` STRING
,`amount` STRING
,`contractId` STRING
,`recoverableHardshipPaymentId` STRING
,`startDate` STRING
,`_removed` STRUCT<`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`recoverableHardshipPaymentId`:STRING>
,`claimantIds`:ARRAY<STRING>
,`_version`:STRING
,`amount`:STRING
,`contractId`:STRING
,`recoverableHardshipPaymentId`:STRING
,`startDate`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/advances/recoverableHardshipPayment';

DROP TABLE IF EXISTS recoverableHardshipPayment;

CREATE TABLE recoverablehardshippayment STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(`_removed`.`amount`, `amount`) as amount, 
COALESCE(`removed_exploded_claimantids`, `exploded_claimantids`) as claimantIds, 
COALESCE(`_removed`.`contractId`, `contractId`) as contractId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`recoverableHardshipPaymentId`, `recoverableHardshipPaymentId`) as recoverableHardshipPaymentId, 
COALESCE(`_removed`.`startDate`, `startDate`) as startDate, 
COALESCE(`_removed`.`_id`.`recoverableHardshipPaymentId`, `_id`.`recoverableHardshipPaymentId`) as id FROM src_advances_recoverablehardshippayment_recoverablehardshippayment
 LATERAL VIEW OUTER EXPLODE(`claimantIds`) view_exploded_claimantids AS exploded_claimantids 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`claimantIds`) view_removed_exploded_claimantids AS removed_exploded_claimantids 
;


!echo ------------------------;
!echo ------------------------ statement;
!echo ------------------------;
DROP TABLE IF EXISTS src_core_statement_statement
;

CREATE EXTERNAL TABLE src_core_statement_statement(
`otherBenefitAwards` ARRAY<STRUCT<`children`:ARRAY<STRUCT<`id`:STRING
,`name`:STRING
>>
,`amount`:STRING
,`claimantId`:STRING
,`otherBenefitAwardType`:STRING
>>
,`people` ARRAY<STRUCT<`declaredDateTime`:STRUCT<`d_date`:STRING>
,`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`citizenId`:STRING
,`contactPreference`:STRING
,`contractId`:STRING
,`email`:STRING
,`firstName`:STRING
,`gender`:STRING
,`lastName`:STRING
,`middleName`:STRING
,`mobileNumber`:STRING
,`sanitisedFirstName`:STRING
,`sanitisedLastName`:STRING
,`sanitisedMiddleName`:STRING
,`verifiedUsingBioQuestionsOrThirdParty`:BOOLEAN
,`verifiedWithNameDateOfBirthOrAddressChange`:BOOLEAN
,`type`:STRING
>>
,`deductionsBreakdown` STRUCT<`deductionRecoveries`:ARRAY<STRUCT<`debtType`:STRING
,`recoveryAmount`:STRING
>>
,`rentArrears`:STRING
,`totalDeductions`:STRING>
,`assessmentPeriod` STRUCT<`createdDateTime`:STRUCT<`d_date`:STRING>
,`assessmentPeriodId`:STRING
,`contractId`:STRING
,`endDate`:STRING
,`paymentDate`:STRING
,`processDate`:STRING
,`startDate`:STRING>
,`housingElementSmiBreakdown` ARRAY<STRUCT<`lender`:STRING
,`mortgageDetailsId`:STRING
,`repayment`:STRING
>>
,`overlappingBenefits` ARRAY<STRUCT<`amount`:STRING
,`claimantId`:STRING
,`otherBenefitAwardType`:STRING
>>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`takeHomeBreakdown` STRUCT<`rte`:STRING
,`selfEmployed`:STRING
,`selfEmployedWithMif`:STRING
,`selfReported`:STRING>
,`workCapabilityElement` STRUCT<`amount`:STRING
,`type`:STRING>
,`_id` STRUCT<`statementId`:STRING>
,`_version` STRING
,`advances` STRING
,`benefitCapAdjustment` STRING
,`benefitCapThreshold` STRING
,`capApplied` BOOLEAN
,`capitalAdjustment` STRING
,`carerElement` STRING
,`childElement` STRING
,`childcareElement` STRING
,`deductions` STRING
,`disabledChildElement` STRING
,`earningsSource` STRING
,`entitledToHousingElement` BOOLEAN
,`fraudPenalties` STRING
,`gracePeriodEndDate` STRING
,`housingElement` STRING
,`housingElementRent` STRING
,`housingElementServiceCharges` STRING
,`housingElementSmiTotalOwed` STRING
,`landlordPayment` STRING
,`numberEligibleChildren` STRING
,`numberEligibleChildrenInChildCare` STRING
,`numberEligibleDisabledChildren` STRING
,`numberPeopleCaredFor` STRING
,`otherIncomeAdjustment` STRING
,`sanctions` STRING
,`standardAllowanceElement` STRING
,`statementId` STRING
,`takeHomePay` STRING
,`totalAdjustments` STRING
,`totalOtherBenefitsAdjustment` STRING
,`totalPayment` STRING
,`totalReducedForHomePay` STRING
,`unaffectedPayElement` STRING
,`housingElementSmiPayment` STRING
,`preAdjustmentTotal` STRING
,`type` STRING
,`_removed` STRUCT<`otherBenefitAwards`:ARRAY<STRUCT<`children`:ARRAY<STRUCT<`id`:STRING
,`name`:STRING
>>
,`amount`:STRING
,`claimantId`:STRING
,`otherBenefitAwardType`:STRING
>>
,`people`:ARRAY<STRUCT<`declaredDateTime`:STRUCT<`d_date`:STRING>
,`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`citizenId`:STRING
,`contactPreference`:STRING
,`contractId`:STRING
,`email`:STRING
,`firstName`:STRING
,`gender`:STRING
,`lastName`:STRING
,`middleName`:STRING
,`mobileNumber`:STRING
,`sanitisedFirstName`:STRING
,`sanitisedLastName`:STRING
,`sanitisedMiddleName`:STRING
,`verifiedUsingBioQuestionsOrThirdParty`:BOOLEAN
,`verifiedWithNameDateOfBirthOrAddressChange`:BOOLEAN
,`type`:STRING
>>
,`deductionsBreakdown`:STRUCT<`deductionRecoveries`:ARRAY<STRUCT<`debtType`:STRING
,`recoveryAmount`:STRING
>>
,`rentArrears`:STRING
,`totalDeductions`:STRING>
,`assessmentPeriod`:STRUCT<`createdDateTime`:STRUCT<`d_date`:STRING>
,`assessmentPeriodId`:STRING
,`contractId`:STRING
,`endDate`:STRING
,`paymentDate`:STRING
,`processDate`:STRING
,`startDate`:STRING>
,`housingElementSmiBreakdown`:ARRAY<STRUCT<`lender`:STRING
,`mortgageDetailsId`:STRING
,`repayment`:STRING
>>
,`overlappingBenefits`:ARRAY<STRUCT<`amount`:STRING
,`claimantId`:STRING
,`otherBenefitAwardType`:STRING
>>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`takeHomeBreakdown`:STRUCT<`rte`:STRING
,`selfEmployed`:STRING
,`selfEmployedWithMif`:STRING
,`selfReported`:STRING>
,`workCapabilityElement`:STRUCT<`amount`:STRING
,`type`:STRING>
,`_id`:STRUCT<`statementId`:STRING>
,`_version`:STRING
,`advances`:STRING
,`benefitCapAdjustment`:STRING
,`benefitCapThreshold`:STRING
,`capApplied`:BOOLEAN
,`capitalAdjustment`:STRING
,`carerElement`:STRING
,`childElement`:STRING
,`childcareElement`:STRING
,`deductions`:STRING
,`disabledChildElement`:STRING
,`earningsSource`:STRING
,`entitledToHousingElement`:BOOLEAN
,`fraudPenalties`:STRING
,`gracePeriodEndDate`:STRING
,`housingElement`:STRING
,`housingElementRent`:STRING
,`housingElementServiceCharges`:STRING
,`housingElementSmiTotalOwed`:STRING
,`landlordPayment`:STRING
,`numberEligibleChildren`:STRING
,`numberEligibleChildrenInChildCare`:STRING
,`numberEligibleDisabledChildren`:STRING
,`numberPeopleCaredFor`:STRING
,`otherIncomeAdjustment`:STRING
,`sanctions`:STRING
,`standardAllowanceElement`:STRING
,`statementId`:STRING
,`takeHomePay`:STRING
,`totalAdjustments`:STRING
,`totalOtherBenefitsAdjustment`:STRING
,`totalPayment`:STRING
,`totalReducedForHomePay`:STRING
,`unaffectedPayElement`:STRING
,`housingElementSmiPayment`:STRING
,`preAdjustmentTotal`:STRING
,`type`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/core/statement';

DROP TABLE IF EXISTS statement;

CREATE TABLE statement STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(`_removed`.`advances`, `advances`) as advances, 
COALESCE(`_removed`.`assessmentPeriod`.`assessmentPeriodId`, `assessmentPeriod`.`assessmentPeriodId`) as assessmentPeriod_assessmentPeriodId, 
COALESCE(`_removed`.`assessmentPeriod`.`contractId`, `assessmentPeriod`.`contractId`) as assessmentPeriod_contractId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`assessmentPeriod`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`assessmentPeriod`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`assessmentPeriod`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`assessmentPeriod`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`assessmentPeriod`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`assessmentPeriod`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`assessmentPeriod`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`assessmentPeriod`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`assessmentPeriod`.`createdDateTime`.`d_date`, LENGTH(`assessmentPeriod`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`assessmentPeriod`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`assessmentPeriod`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`assessmentPeriod`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`assessmentPeriod`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`assessmentPeriod`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`assessmentPeriod`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as assessmentPeriod_createdDateTime_d_date, 
COALESCE(`_removed`.`assessmentPeriod`.`endDate`, `assessmentPeriod`.`endDate`) as assessmentPeriod_endDate, 
COALESCE(`_removed`.`assessmentPeriod`.`paymentDate`, `assessmentPeriod`.`paymentDate`) as assessmentPeriod_paymentDate, 
COALESCE(`_removed`.`assessmentPeriod`.`processDate`, `assessmentPeriod`.`processDate`) as assessmentPeriod_processDate, 
COALESCE(`_removed`.`assessmentPeriod`.`startDate`, `assessmentPeriod`.`startDate`) as assessmentPeriod_startDate, 
COALESCE(`_removed`.`benefitCapAdjustment`, `benefitCapAdjustment`) as benefitCapAdjustment, 
COALESCE(`_removed`.`benefitCapThreshold`, `benefitCapThreshold`) as benefitCapThreshold, 
COALESCE(CAST(`_removed`.`capApplied` as STRING), CAST(`capApplied` as STRING)) as capApplied, 
COALESCE(`_removed`.`capitalAdjustment`, `capitalAdjustment`) as capitalAdjustment, 
COALESCE(`_removed`.`carerElement`, `carerElement`) as carerElement, 
COALESCE(`_removed`.`childElement`, `childElement`) as childElement, 
COALESCE(`_removed`.`childcareElement`, `childcareElement`) as childcareElement, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`deductions`, `deductions`) as deductions, 
COALESCE(`removed_exploded_deductionsbreakdown_deductionrecoveries`.`debtType`, `exploded_deductionsbreakdown_deductionrecoveries`.`debtType`) as deductionsBreakdown_deductionRecoveries_debtType, 
COALESCE(`removed_exploded_deductionsbreakdown_deductionrecoveries`.`recoveryAmount`, `exploded_deductionsbreakdown_deductionrecoveries`.`recoveryAmount`) as deductionsBreakdown_deductionRecoveries_recoveryAmount, 
COALESCE(`_removed`.`deductionsBreakdown`.`rentArrears`, `deductionsBreakdown`.`rentArrears`) as deductionsBreakdown_rentArrears, 
COALESCE(`_removed`.`deductionsBreakdown`.`totalDeductions`, `deductionsBreakdown`.`totalDeductions`) as deductionsBreakdown_totalDeductions, 
COALESCE(`_removed`.`disabledChildElement`, `disabledChildElement`) as disabledChildElement, 
COALESCE(`_removed`.`earningsSource`, `earningsSource`) as earningsSource, 
COALESCE(CAST(`_removed`.`entitledToHousingElement` as STRING), CAST(`entitledToHousingElement` as STRING)) as entitledToHousingElement, 
COALESCE(`_removed`.`fraudPenalties`, `fraudPenalties`) as fraudPenalties, 
COALESCE(`_removed`.`gracePeriodEndDate`, `gracePeriodEndDate`) as gracePeriodEndDate, 
COALESCE(`_removed`.`housingElement`, `housingElement`) as housingElement, 
COALESCE(`_removed`.`housingElementRent`, `housingElementRent`) as housingElementRent, 
COALESCE(`_removed`.`housingElementServiceCharges`, `housingElementServiceCharges`) as housingElementServiceCharges, 
COALESCE(`removed_exploded_housingelementsmibreakdown`.`lender`, `exploded_housingelementsmibreakdown`.`lender`) as housingElementSmiBreakdown_lender, 
COALESCE(`removed_exploded_housingelementsmibreakdown`.`mortgageDetailsId`, `exploded_housingelementsmibreakdown`.`mortgageDetailsId`) as housingElementSmiBreakdown_mortgageDetailsId, 
COALESCE(`removed_exploded_housingelementsmibreakdown`.`repayment`, `exploded_housingelementsmibreakdown`.`repayment`) as housingElementSmiBreakdown_repayment, 
COALESCE(`_removed`.`housingElementSmiTotalOwed`, `housingElementSmiTotalOwed`) as housingElementSmiTotalOwed, 
COALESCE(`_removed`.`landlordPayment`, `landlordPayment`) as landlordPayment, 
COALESCE(`_removed`.`numberEligibleChildren`, `numberEligibleChildren`) as numberEligibleChildren, 
COALESCE(`_removed`.`numberEligibleChildrenInChildCare`, `numberEligibleChildrenInChildCare`) as numberEligibleChildrenInChildCare, 
COALESCE(`_removed`.`numberEligibleDisabledChildren`, `numberEligibleDisabledChildren`) as numberEligibleDisabledChildren, 
COALESCE(`_removed`.`numberPeopleCaredFor`, `numberPeopleCaredFor`) as numberPeopleCaredFor, 
COALESCE(`removed_exploded_otherbenefitawards`.`amount`, `exploded_otherbenefitawards`.`amount`) as otherBenefitAwards_amount, 
COALESCE(`removed_exploded_otherbenefitawards_exploded_children`.`id`, `exploded_otherbenefitawards_exploded_children`.`id`) as otherBenefitAwards_children_id, 
COALESCE(`removed_exploded_otherbenefitawards_exploded_children`.`name`, `exploded_otherbenefitawards_exploded_children`.`name`) as otherBenefitAwards_children_name, 
COALESCE(`removed_exploded_otherbenefitawards`.`claimantId`, `exploded_otherbenefitawards`.`claimantId`) as otherBenefitAwards_claimantId, 
COALESCE(`removed_exploded_otherbenefitawards`.`otherBenefitAwardType`, `exploded_otherbenefitawards`.`otherBenefitAwardType`) as otherBenefitAwards_otherBenefitAwardType, 
COALESCE(`_removed`.`otherIncomeAdjustment`, `otherIncomeAdjustment`) as otherIncomeAdjustment, 
COALESCE(`removed_exploded_overlappingbenefits`.`amount`, `exploded_overlappingbenefits`.`amount`) as overlappingBenefits_amount, 
COALESCE(`removed_exploded_overlappingbenefits`.`claimantId`, `exploded_overlappingbenefits`.`claimantId`) as overlappingBenefits_claimantId, 
COALESCE(`removed_exploded_overlappingbenefits`.`otherBenefitAwardType`, `exploded_overlappingbenefits`.`otherBenefitAwardType`) as overlappingBenefits_otherBenefitAwardType, 
COALESCE(`removed_exploded_people`.`citizenId`, `exploded_people`.`citizenId`) as people_citizenId, 
COALESCE(`removed_exploded_people`.`contactPreference`, `exploded_people`.`contactPreference`) as people_contactPreference, 
COALESCE(`removed_exploded_people`.`contractId`, `exploded_people`.`contractId`) as people_contractId, 
COALESCE(CASE WHEN SUBSTRING(`removed_exploded_people`.`declaredDateTime`.`d_date`, LENGTH(`removed_exploded_people`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`removed_exploded_people`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`removed_exploded_people`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`removed_exploded_people`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`removed_exploded_people`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`removed_exploded_people`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`removed_exploded_people`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`exploded_people`.`declaredDateTime`.`d_date`, LENGTH(`exploded_people`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`exploded_people`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`exploded_people`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`exploded_people`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`exploded_people`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`exploded_people`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`exploded_people`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as people_declaredDateTime_d_date, 
COALESCE(CAST(`removed_exploded_people`.`effectiveDate`.`hasDate` as STRING), CAST(`exploded_people`.`effectiveDate`.`hasDate` as STRING)) as people_effectiveDate_hasDate, 
COALESCE(`removed_exploded_people`.`effectiveDate`.`knownDate`, `exploded_people`.`effectiveDate`.`knownDate`) as people_effectiveDate_knownDate, 
COALESCE(`removed_exploded_people`.`effectiveDate`.`type`, `exploded_people`.`effectiveDate`.`type`) as people_effectiveDate_type, 
COALESCE(`removed_exploded_people`.`effectiveDate`.`date`, `exploded_people`.`effectiveDate`.`date`) as people_effectiveDate_date, 
COALESCE(`removed_exploded_people`.`email`, `exploded_people`.`email`) as people_email, 
COALESCE(`removed_exploded_people`.`firstName`, `exploded_people`.`firstName`) as people_firstName, 
COALESCE(`removed_exploded_people`.`gender`, `exploded_people`.`gender`) as people_gender, 
COALESCE(`removed_exploded_people`.`lastName`, `exploded_people`.`lastName`) as people_lastName, 
COALESCE(`removed_exploded_people`.`middleName`, `exploded_people`.`middleName`) as people_middleName, 
COALESCE(`removed_exploded_people`.`mobileNumber`, `exploded_people`.`mobileNumber`) as people_mobileNumber, 
COALESCE(`removed_exploded_people`.`sanitisedFirstName`, `exploded_people`.`sanitisedFirstName`) as people_sanitisedFirstName, 
COALESCE(`removed_exploded_people`.`sanitisedLastName`, `exploded_people`.`sanitisedLastName`) as people_sanitisedLastName, 
COALESCE(`removed_exploded_people`.`sanitisedMiddleName`, `exploded_people`.`sanitisedMiddleName`) as people_sanitisedMiddleName, 
COALESCE(CAST(`removed_exploded_people`.`verifiedUsingBioQuestionsOrThirdParty` as STRING), CAST(`exploded_people`.`verifiedUsingBioQuestionsOrThirdParty` as STRING)) as people_verifiedUsingBioQuestionsOrThirdParty, 
COALESCE(CAST(`removed_exploded_people`.`verifiedWithNameDateOfBirthOrAddressChange` as STRING), CAST(`exploded_people`.`verifiedWithNameDateOfBirthOrAddressChange` as STRING)) as people_verifiedWithNameDateOfBirthOrAddressChange, 
COALESCE(`removed_exploded_people`.`type`, `exploded_people`.`type`) as people_type, 
COALESCE(`_removed`.`sanctions`, `sanctions`) as sanctions, 
COALESCE(`_removed`.`standardAllowanceElement`, `standardAllowanceElement`) as standardAllowanceElement, 
COALESCE(`_removed`.`statementId`, `statementId`) as statementId, 
COALESCE(`_removed`.`takeHomeBreakdown`.`rte`, `takeHomeBreakdown`.`rte`) as takeHomeBreakdown_rte, 
COALESCE(`_removed`.`takeHomeBreakdown`.`selfEmployed`, `takeHomeBreakdown`.`selfEmployed`) as takeHomeBreakdown_selfEmployed, 
COALESCE(`_removed`.`takeHomeBreakdown`.`selfEmployedWithMif`, `takeHomeBreakdown`.`selfEmployedWithMif`) as takeHomeBreakdown_selfEmployedWithMif, 
COALESCE(`_removed`.`takeHomeBreakdown`.`selfReported`, `takeHomeBreakdown`.`selfReported`) as takeHomeBreakdown_selfReported, 
COALESCE(`_removed`.`takeHomePay`, `takeHomePay`) as takeHomePay, 
COALESCE(`_removed`.`totalAdjustments`, `totalAdjustments`) as totalAdjustments, 
COALESCE(`_removed`.`totalOtherBenefitsAdjustment`, `totalOtherBenefitsAdjustment`) as totalOtherBenefitsAdjustment, 
COALESCE(`_removed`.`totalPayment`, `totalPayment`) as totalPayment, 
COALESCE(`_removed`.`totalReducedForHomePay`, `totalReducedForHomePay`) as totalReducedForHomePay, 
COALESCE(`_removed`.`unaffectedPayElement`, `unaffectedPayElement`) as unaffectedPayElement, 
COALESCE(`_removed`.`workCapabilityElement`.`amount`, `workCapabilityElement`.`amount`) as workCapabilityElement_amount, 
COALESCE(`_removed`.`workCapabilityElement`.`type`, `workCapabilityElement`.`type`) as workCapabilityElement_type, 
COALESCE(`_removed`.`housingElementSmiPayment`, `housingElementSmiPayment`) as housingElementSmiPayment, 
COALESCE(`_removed`.`preAdjustmentTotal`, `preAdjustmentTotal`) as preAdjustmentTotal, 
COALESCE(`_removed`.`type`, `type`) as type, 
COALESCE(`_removed`.`_id`.`statementId`, `_id`.`statementId`) as id FROM src_core_statement_statement
 LATERAL VIEW OUTER EXPLODE(`deductionsBreakdown`.`deductionRecoveries`) view_exploded_deductionsbreakdown_deductionrecoveries AS exploded_deductionsbreakdown_deductionrecoveries 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`deductionsBreakdown`.`deductionRecoveries`) view_removed_exploded_deductionsbreakdown_deductionrecoveries AS removed_exploded_deductionsbreakdown_deductionrecoveries 
 LATERAL VIEW OUTER EXPLODE(`housingElementSmiBreakdown`) view_exploded_housingelementsmibreakdown AS exploded_housingelementsmibreakdown 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`housingElementSmiBreakdown`) view_removed_exploded_housingelementsmibreakdown AS removed_exploded_housingelementsmibreakdown 
 LATERAL VIEW OUTER EXPLODE(`otherBenefitAwards`) view_exploded_otherbenefitawards AS exploded_otherbenefitawards 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`otherBenefitAwards`) view_removed_exploded_otherbenefitawards AS removed_exploded_otherbenefitawards 
 LATERAL VIEW OUTER EXPLODE(`exploded_otherbenefitawards`.`children`) view_exploded_otherbenefitawards_exploded_children AS exploded_otherbenefitawards_exploded_children 
 LATERAL VIEW OUTER EXPLODE(`removed_exploded_otherbenefitawards`.`children`) view_removed_exploded_otherbenefitawards_exploded_children AS removed_exploded_otherbenefitawards_exploded_children 
 LATERAL VIEW OUTER EXPLODE(`overlappingBenefits`) view_exploded_overlappingbenefits AS exploded_overlappingbenefits 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`overlappingBenefits`) view_removed_exploded_overlappingbenefits AS removed_exploded_overlappingbenefits 
 LATERAL VIEW OUTER EXPLODE(`people`) view_exploded_people AS exploded_people 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`people`) view_removed_exploded_people AS removed_exploded_people 
;


!echo ------------------------;
!echo ------------------------ youthObligationDetails;
!echo ------------------------;
DROP TABLE IF EXISTS src_agent_core_youthobligationdetails_youthobligationdetails
;

CREATE EXTERNAL TABLE src_agent_core_youthobligationdetails_youthobligationdetails(
`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`youthObligationDetailsId`:STRING>
,`youthObligationEasements` ARRAY<STRING>
,`youthObligationReferrals` ARRAY<STRING>
,`_version` STRING
,`claimantId` STRING
,`startedApprenticeship` BOOLEAN
,`statusLastChangedDate` STRING
,`youthObligationDetailsId` STRING
,`youthObligationStatus` STRING
,`_removed` STRUCT<`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`youthObligationDetailsId`:STRING>
,`youthObligationEasements`:ARRAY<STRING>
,`youthObligationReferrals`:ARRAY<STRING>
,`_version`:STRING
,`claimantId`:STRING
,`startedApprenticeship`:BOOLEAN
,`statusLastChangedDate`:STRING
,`youthObligationDetailsId`:STRING
,`youthObligationStatus`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/agent-core/youthObligationDetails';

DROP TABLE IF EXISTS youthObligationDetails;

CREATE TABLE youthobligationdetails STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(`_removed`.`claimantId`, `claimantId`) as claimantId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(CAST(`_removed`.`startedApprenticeship` as STRING), CAST(`startedApprenticeship` as STRING)) as startedApprenticeship, 
COALESCE(`_removed`.`statusLastChangedDate`, `statusLastChangedDate`) as statusLastChangedDate, 
COALESCE(`_removed`.`youthObligationDetailsId`, `youthObligationDetailsId`) as youthObligationDetailsId, 
COALESCE(`removed_exploded_youthobligationeasements`, `exploded_youthobligationeasements`) as youthObligationEasements, 
COALESCE(`removed_exploded_youthobligationreferrals`, `exploded_youthobligationreferrals`) as youthObligationReferrals, 
COALESCE(`_removed`.`youthObligationStatus`, `youthObligationStatus`) as youthObligationStatus, 
COALESCE(`_removed`.`_id`.`youthObligationDetailsId`, `_id`.`youthObligationDetailsId`) as id FROM src_agent_core_youthobligationdetails_youthobligationdetails
 LATERAL VIEW OUTER EXPLODE(`youthObligationEasements`) view_exploded_youthobligationeasements AS exploded_youthobligationeasements 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`youthObligationEasements`) view_removed_exploded_youthobligationeasements AS removed_exploded_youthobligationeasements 
 LATERAL VIEW OUTER EXPLODE(`youthObligationReferrals`) view_exploded_youthobligationreferrals AS exploded_youthobligationreferrals 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`youthObligationReferrals`) view_removed_exploded_youthobligationreferrals AS removed_exploded_youthobligationreferrals 
;


!echo ------------------------;
!echo ------------------------ claimant;
!echo ------------------------;
DROP TABLE IF EXISTS src_core_claimant_claimant
;

CREATE EXTERNAL TABLE src_core_claimant_claimant(
`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`claimantDeathDetails` STRUCT<`dateOfDeath`:STRING
,`deathVerified`:BOOLEAN>
,`inactiveAccountEmailReminderSentDate` STRUCT<`d_date`:STRING>
,`managedJourney` STRUCT<`ableToViewJournal`:BOOLEAN
,`currentStep`:STRING
,`type`:STRING>
,`workCoach` STRUCT<`agentId`:STRING
,`type`:STRING>
,`_id` STRUCT<`citizenId`:STRING>
,`deliveryUnits` ARRAY<STRING>
,`supportingAgents` ARRAY<STRING>
,`searchableNames` ARRAY<STRING>
,`_version` STRING
,`anonymous` BOOLEAN
,`bookAppointmentStatus` STRING
,`caseManager` STRING
,`cisInterestStatus` STRING
,`citizenId` STRING
,`claimantProvidedNino` STRING
,`currentWorkGroupOnCurrentContract` STRING
,`firstName` STRING
,`govUkVerifyStatus` STRING
,`hasVerifiedEmailId` BOOLEAN
,`identityDocumentsStatus` STRING
,`lastName` STRING
,`nino` STRING
,`personId` STRING
,`pilotGroup` STRING
,`webAnalyticsUserId` STRING
,`firstNameCaseInsensitive` STRING
,`fullName` STRING
,`lastNameCaseInsensitive` STRING
,`_removed` STRUCT<`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`claimantDeathDetails`:STRUCT<`dateOfDeath`:STRING
,`deathVerified`:BOOLEAN>
,`inactiveAccountEmailReminderSentDate`:STRUCT<`d_date`:STRING>
,`managedJourney`:STRUCT<`ableToViewJournal`:BOOLEAN
,`currentStep`:STRING
,`type`:STRING>
,`workCoach`:STRUCT<`agentId`:STRING
,`type`:STRING>
,`_id`:STRUCT<`citizenId`:STRING>
,`deliveryUnits`:ARRAY<STRING>
,`supportingAgents`:ARRAY<STRING>
,`searchableNames`:ARRAY<STRING>
,`_version`:STRING
,`anonymous`:BOOLEAN
,`bookAppointmentStatus`:STRING
,`caseManager`:STRING
,`cisInterestStatus`:STRING
,`citizenId`:STRING
,`claimantProvidedNino`:STRING
,`currentWorkGroupOnCurrentContract`:STRING
,`firstName`:STRING
,`govUkVerifyStatus`:STRING
,`hasVerifiedEmailId`:BOOLEAN
,`identityDocumentsStatus`:STRING
,`lastName`:STRING
,`nino`:STRING
,`personId`:STRING
,`pilotGroup`:STRING
,`webAnalyticsUserId`:STRING
,`firstNameCaseInsensitive`:STRING
,`fullName`:STRING
,`lastNameCaseInsensitive`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/core/claimant';

DROP TABLE IF EXISTS claimant;

CREATE TABLE claimant STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CAST(`_removed`.`anonymous` as STRING), CAST(`anonymous` as STRING)) as anonymous, 
COALESCE(`_removed`.`bookAppointmentStatus`, `bookAppointmentStatus`) as bookAppointmentStatus, 
COALESCE(`_removed`.`caseManager`, `caseManager`) as caseManager, 
COALESCE(`_removed`.`cisInterestStatus`, `cisInterestStatus`) as cisInterestStatus, 
COALESCE(`_removed`.`citizenId`, `citizenId`) as citizenId, 
COALESCE(`_removed`.`claimantDeathDetails`.`dateOfDeath`, `claimantDeathDetails`.`dateOfDeath`) as claimantDeathDetails_dateOfDeath, 
COALESCE(CAST(`_removed`.`claimantDeathDetails`.`deathVerified` as STRING), CAST(`claimantDeathDetails`.`deathVerified` as STRING)) as claimantDeathDetails_deathVerified, 
COALESCE(`_removed`.`claimantProvidedNino`, `claimantProvidedNino`) as claimantProvidedNino, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`currentWorkGroupOnCurrentContract`, `currentWorkGroupOnCurrentContract`) as currentWorkGroupOnCurrentContract, 
COALESCE(`removed_exploded_deliveryunits`, `exploded_deliveryunits`) as deliveryUnits, 
COALESCE(`_removed`.`firstName`, `firstName`) as firstName, 
COALESCE(`_removed`.`govUkVerifyStatus`, `govUkVerifyStatus`) as govUkVerifyStatus, 
COALESCE(CAST(`_removed`.`hasVerifiedEmailId` as STRING), CAST(`hasVerifiedEmailId` as STRING)) as hasVerifiedEmailId, 
COALESCE(`_removed`.`identityDocumentsStatus`, `identityDocumentsStatus`) as identityDocumentsStatus, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`inactiveAccountEmailReminderSentDate`.`d_date`, LENGTH(`_removed`.`inactiveAccountEmailReminderSentDate`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`inactiveAccountEmailReminderSentDate`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`inactiveAccountEmailReminderSentDate`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`inactiveAccountEmailReminderSentDate`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`inactiveAccountEmailReminderSentDate`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`inactiveAccountEmailReminderSentDate`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`inactiveAccountEmailReminderSentDate`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`inactiveAccountEmailReminderSentDate`.`d_date`, LENGTH(`inactiveAccountEmailReminderSentDate`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`inactiveAccountEmailReminderSentDate`.`d_date`, 1, 10), ' ', SUBSTR(`inactiveAccountEmailReminderSentDate`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`inactiveAccountEmailReminderSentDate`.`d_date`, 1, 10), ' ', SUBSTR(`inactiveAccountEmailReminderSentDate`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`inactiveAccountEmailReminderSentDate`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`inactiveAccountEmailReminderSentDate`.`d_date`, 21, 3)) AS TIMESTAMP) END) as inactiveAccountEmailReminderSentDate_d_date, 
COALESCE(`_removed`.`lastName`, `lastName`) as lastName, 
COALESCE(CAST(`_removed`.`managedJourney`.`ableToViewJournal` as STRING), CAST(`managedJourney`.`ableToViewJournal` as STRING)) as managedJourney_ableToViewJournal, 
COALESCE(`_removed`.`managedJourney`.`currentStep`, `managedJourney`.`currentStep`) as managedJourney_currentStep, 
COALESCE(`_removed`.`managedJourney`.`type`, `managedJourney`.`type`) as managedJourney_type, 
COALESCE(`_removed`.`nino`, `nino`) as nino, 
COALESCE(`_removed`.`personId`, `personId`) as personId, 
COALESCE(`_removed`.`pilotGroup`, `pilotGroup`) as pilotGroup, 
COALESCE(`removed_exploded_supportingagents`, `exploded_supportingagents`) as supportingAgents, 
COALESCE(`_removed`.`webAnalyticsUserId`, `webAnalyticsUserId`) as webAnalyticsUserId, 
COALESCE(`_removed`.`workCoach`.`agentId`, `workCoach`.`agentId`) as workCoach_agentId, 
COALESCE(`_removed`.`workCoach`.`type`, `workCoach`.`type`) as workCoach_type, 
COALESCE(`_removed`.`firstNameCaseInsensitive`, `firstNameCaseInsensitive`) as firstNameCaseInsensitive, 
COALESCE(`_removed`.`fullName`, `fullName`) as fullName, 
COALESCE(`_removed`.`lastNameCaseInsensitive`, `lastNameCaseInsensitive`) as lastNameCaseInsensitive, 
COALESCE(`removed_exploded_searchablenames`, `exploded_searchablenames`) as searchableNames, 
COALESCE(`_removed`.`_id`.`citizenId`, `_id`.`citizenId`) as id FROM src_core_claimant_claimant
 LATERAL VIEW OUTER EXPLODE(`deliveryUnits`) view_exploded_deliveryunits AS exploded_deliveryunits 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`deliveryUnits`) view_removed_exploded_deliveryunits AS removed_exploded_deliveryunits 
 LATERAL VIEW OUTER EXPLODE(`supportingAgents`) view_exploded_supportingagents AS exploded_supportingagents 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`supportingAgents`) view_removed_exploded_supportingagents AS removed_exploded_supportingagents 
 LATERAL VIEW OUTER EXPLODE(`searchableNames`) view_exploded_searchablenames AS exploded_searchablenames 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`searchableNames`) view_removed_exploded_searchablenames AS removed_exploded_searchablenames 
;


!echo ------------------------;
!echo ------------------------ manualOverride;
!echo ------------------------;
DROP TABLE IF EXISTS src_agent_core_manualoverride_manualoverride
;

CREATE EXTERNAL TABLE src_agent_core_manualoverride_manualoverride(
`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`entitlementOverrideProperties` STRUCT<`childId`:STRING
,`type`:STRING
,`continueCostContributionDeduction`:BOOLEAN
,`nonDepId`:STRING>
,`_id` STRUCT<`contractId`:STRING>
,`_version` STRING
,`contractId` STRING
,`forType` STRING
,`_removed` STRUCT<`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`entitlementOverrideProperties`:STRUCT<`childId`:STRING
,`type`:STRING
,`continueCostContributionDeduction`:BOOLEAN
,`nonDepId`:STRING>
,`_id`:STRUCT<`contractId`:STRING>
,`_version`:STRING
,`contractId`:STRING
,`forType`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/agent-core/manualOverride';

DROP TABLE IF EXISTS manualOverride;

CREATE TABLE manualoverride STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(`_removed`.`contractId`, `contractId`) as contractId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`forType`, `forType`) as forType, 
COALESCE(`_removed`.`entitlementOverrideProperties`.`childId`, `entitlementOverrideProperties`.`childId`) as entitlementOverrideProperties_childId, 
COALESCE(`_removed`.`entitlementOverrideProperties`.`type`, `entitlementOverrideProperties`.`type`) as entitlementOverrideProperties_type, 
COALESCE(CAST(`_removed`.`entitlementOverrideProperties`.`continueCostContributionDeduction` as STRING), CAST(`entitlementOverrideProperties`.`continueCostContributionDeduction` as STRING)) as entitlementOverrideProperties_continueCostContributionDeduction, 
COALESCE(`_removed`.`entitlementOverrideProperties`.`nonDepId`, `entitlementOverrideProperties`.`nonDepId`) as entitlementOverrideProperties_nonDepId, 
COALESCE(`_removed`.`_id`.`contractId`, `_id`.`contractId`) as id FROM src_agent_core_manualoverride_manualoverride
 src_agent_core_manualoverride_manualoverride;


!echo ------------------------;
!echo ------------------------ educationDeclaration;
!echo ------------------------;
DROP TABLE IF EXISTS src_core_educationdeclaration_educationdeclaration
;

CREATE EXTERNAL TABLE src_core_educationdeclaration_educationdeclaration(
`educationCircumstances` STRUCT<`declaredDateTime`:STRUCT<`d_date`:STRING>
,`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`claimantId`:STRING
,`contractId`:STRING
,`fullTimeEducation`:BOOLEAN
,`type`:STRING>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`declarationId`:STRING>
,`_version` STRING
,`declarationId` STRING
,`processId` STRING
,`type` STRING
,`_removed` STRUCT<`educationCircumstances`:STRUCT<`declaredDateTime`:STRUCT<`d_date`:STRING>
,`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`claimantId`:STRING
,`contractId`:STRING
,`fullTimeEducation`:BOOLEAN
,`type`:STRING>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`declarationId`:STRING>
,`_version`:STRING
,`declarationId`:STRING
,`processId`:STRING
,`type`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/core/educationDeclaration';

DROP TABLE IF EXISTS educationDeclaration;

CREATE TABLE educationdeclaration STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(`_removed`.`educationCircumstances`.`claimantId`, `educationCircumstances`.`claimantId`) as educationCircumstances_claimantId, 
COALESCE(`_removed`.`educationCircumstances`.`contractId`, `educationCircumstances`.`contractId`) as educationCircumstances_contractId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`educationCircumstances`.`declaredDateTime`.`d_date`, LENGTH(`_removed`.`educationCircumstances`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`educationCircumstances`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`educationCircumstances`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`educationCircumstances`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`educationCircumstances`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`educationCircumstances`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`educationCircumstances`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`educationCircumstances`.`declaredDateTime`.`d_date`, LENGTH(`educationCircumstances`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`educationCircumstances`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`educationCircumstances`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`educationCircumstances`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`educationCircumstances`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`educationCircumstances`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`educationCircumstances`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as educationCircumstances_declaredDateTime_d_date, 
COALESCE(CAST(`_removed`.`educationCircumstances`.`effectiveDate`.`hasDate` as STRING), CAST(`educationCircumstances`.`effectiveDate`.`hasDate` as STRING)) as educationCircumstances_effectiveDate_hasDate, 
COALESCE(`_removed`.`educationCircumstances`.`effectiveDate`.`knownDate`, `educationCircumstances`.`effectiveDate`.`knownDate`) as educationCircumstances_effectiveDate_knownDate, 
COALESCE(`_removed`.`educationCircumstances`.`effectiveDate`.`type`, `educationCircumstances`.`effectiveDate`.`type`) as educationCircumstances_effectiveDate_type, 
COALESCE(`_removed`.`educationCircumstances`.`effectiveDate`.`date`, `educationCircumstances`.`effectiveDate`.`date`) as educationCircumstances_effectiveDate_date, 
COALESCE(CAST(`_removed`.`educationCircumstances`.`fullTimeEducation` as STRING), CAST(`educationCircumstances`.`fullTimeEducation` as STRING)) as educationCircumstances_fullTimeEducation, 
COALESCE(CAST(`_removed`.`educationCircumstances`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`educationCircumstances`.`paymentEffectiveDate`.`hasDate` as STRING)) as educationCircumstances_paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`educationCircumstances`.`paymentEffectiveDate`.`knownDate`, `educationCircumstances`.`paymentEffectiveDate`.`knownDate`) as educationCircumstances_paymentEffectiveDate_knownDate, 
COALESCE(`_removed`.`educationCircumstances`.`paymentEffectiveDate`.`type`, `educationCircumstances`.`paymentEffectiveDate`.`type`) as educationCircumstances_paymentEffectiveDate_type, 
COALESCE(`_removed`.`educationCircumstances`.`paymentEffectiveDate`.`date`, `educationCircumstances`.`paymentEffectiveDate`.`date`) as educationCircumstances_paymentEffectiveDate_date, 
COALESCE(`_removed`.`educationCircumstances`.`type`, `educationCircumstances`.`type`) as educationCircumstances_type, 
COALESCE(`_removed`.`processId`, `processId`) as processId, 
COALESCE(`_removed`.`type`, `type`) as type, 
COALESCE(`_removed`.`_id`.`declarationId`, `_id`.`declarationId`) as id FROM src_core_educationdeclaration_educationdeclaration
 src_core_educationdeclaration_educationdeclaration;


!echo ------------------------;
!echo ------------------------ localUserMatchingData;
!echo ------------------------;
DROP TABLE IF EXISTS src_matchingservice_localusermatchingdata_localusermatchingdata
;

CREATE EXTERNAL TABLE src_matchingservice_localusermatchingdata_localusermatchingdata(
`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`personId`:STRING>
,`_version` STRING
,`dateOfBirth` STRING
,`firstName` STRING
,`normalisedSurname` STRING
,`personId` STRING
,`postcode` STRING
,`surname` STRING
,`_removed` STRUCT<`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`personId`:STRING>
,`_version`:STRING
,`dateOfBirth`:STRING
,`firstName`:STRING
,`normalisedSurname`:STRING
,`personId`:STRING
,`postcode`:STRING
,`surname`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/matchingService/localUserMatchingData';

DROP TABLE IF EXISTS localUserMatchingData;

CREATE TABLE localusermatchingdata STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`dateOfBirth`, `dateOfBirth`) as dateOfBirth, 
COALESCE(`_removed`.`firstName`, `firstName`) as firstName, 
COALESCE(`_removed`.`normalisedSurname`, `normalisedSurname`) as normalisedSurname, 
COALESCE(`_removed`.`personId`, `personId`) as personId, 
COALESCE(`_removed`.`postcode`, `postcode`) as postcode, 
COALESCE(`_removed`.`surname`, `surname`) as surname, 
COALESCE(`_removed`.`_id`.`personId`, `_id`.`personId`) as id FROM src_matchingservice_localusermatchingdata_localusermatchingdata
 src_matchingservice_localusermatchingdata_localusermatchingdata;


!echo ------------------------;
!echo ------------------------ sanctionEscalation;
!echo ------------------------;
DROP TABLE IF EXISTS src_penalties_and_deductions_sanctionescalation_sanctionescalation
;

CREATE EXTERNAL TABLE src_penalties_and_deductions_sanctionescalation_sanctionescalation(
`escalationLevel` STRUCT<`trace`:STRUCT<`elements`:ARRAY<STRUCT<`code`:STRING
,`description`:STRING
,`elementType`:STRING
>>>
,`value`:STRING>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`sanctionId`:STRING>
,`_version` STRING
,`dateOfFailure` STRING
,`over17AtDateOfFailure` BOOLEAN
,`personId` STRING
,`sanctionId` STRING
,`sanctionLevel` STRING
,`universalCreditSanction` BOOLEAN
,`_removed` STRUCT<`escalationLevel`:STRUCT<`trace`:STRUCT<`elements`:ARRAY<STRUCT<`code`:STRING
,`description`:STRING
,`elementType`:STRING
>>>
,`value`:STRING>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`sanctionId`:STRING>
,`_version`:STRING
,`dateOfFailure`:STRING
,`over17AtDateOfFailure`:BOOLEAN
,`personId`:STRING
,`sanctionId`:STRING
,`sanctionLevel`:STRING
,`universalCreditSanction`:BOOLEAN
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/penalties-and-deductions/sanctionEscalation';

DROP TABLE IF EXISTS sanctionEscalation;

CREATE TABLE sanctionescalation STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`dateOfFailure`, `dateOfFailure`) as dateOfFailure, 
COALESCE(`removed_exploded_escalationlevel_trace_elements`.`code`, `exploded_escalationlevel_trace_elements`.`code`) as escalationLevel_trace_elements_code, 
COALESCE(`removed_exploded_escalationlevel_trace_elements`.`description`, `exploded_escalationlevel_trace_elements`.`description`) as escalationLevel_trace_elements_description, 
COALESCE(`removed_exploded_escalationlevel_trace_elements`.`elementType`, `exploded_escalationlevel_trace_elements`.`elementType`) as escalationLevel_trace_elements_elementType, 
COALESCE(`_removed`.`escalationLevel`.`value`, `escalationLevel`.`value`) as escalationLevel_value, 
COALESCE(CAST(`_removed`.`over17AtDateOfFailure` as STRING), CAST(`over17AtDateOfFailure` as STRING)) as over17AtDateOfFailure, 
COALESCE(`_removed`.`personId`, `personId`) as personId, 
COALESCE(`_removed`.`sanctionId`, `sanctionId`) as sanctionId, 
COALESCE(`_removed`.`sanctionLevel`, `sanctionLevel`) as sanctionLevel, 
COALESCE(CAST(`_removed`.`universalCreditSanction` as STRING), CAST(`universalCreditSanction` as STRING)) as universalCreditSanction, 
COALESCE(`_removed`.`_id`.`sanctionId`, `_id`.`sanctionId`) as id FROM src_penalties_and_deductions_sanctionescalation_sanctionescalation
 LATERAL VIEW OUTER EXPLODE(`escalationLevel`.`trace`.`elements`) view_exploded_escalationlevel_trace_elements AS exploded_escalationlevel_trace_elements 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`escalationLevel`.`trace`.`elements`) view_removed_exploded_escalationlevel_trace_elements AS removed_exploded_escalationlevel_trace_elements 
;


!echo ------------------------;
!echo ------------------------ managementInformation;
!echo ------------------------;
DROP TABLE IF EXISTS src_matchingservice_managementinformation_managementinformation
;

CREATE EXTERNAL TABLE src_matchingservice_managementinformation_managementinformation(
`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`managementInformationId`:STRING>
,`_version` STRING
,`managementInformationId` STRING
,`miSubEvent` STRING
,`type` STRING
,`_removed` STRUCT<`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`managementInformationId`:STRING>
,`_version`:STRING
,`managementInformationId`:STRING
,`miSubEvent`:STRING
,`type`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/matchingService/managementInformation';

DROP TABLE IF EXISTS managementInformation;

CREATE TABLE managementinformation STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`managementInformationId`, `managementInformationId`) as managementInformationId, 
COALESCE(`_removed`.`miSubEvent`, `miSubEvent`) as miSubEvent, 
COALESCE(`_removed`.`type`, `type`) as type, 
COALESCE(`_removed`.`_id`.`managementInformationId`, `_id`.`managementInformationId`) as id FROM src_matchingservice_managementinformation_managementinformation
 src_matchingservice_managementinformation_managementinformation;


!echo ------------------------;
!echo ------------------------ claimantProfile;
!echo ------------------------;
DROP TABLE IF EXISTS src_agent_core_claimantprofile_claimantprofile
;

CREATE EXTERNAL TABLE src_agent_core_claimantprofile_claimantprofile(
`claimantProfileItems` ARRAY<STRUCT<`createdDateTime`:STRUCT<`d_date`:STRING>
,`createdByAgentId`:STRING
,`description`:STRING
,`id`:STRING
,`value`:STRING
>>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`claimantId`:STRING>
,`_version` STRING
,`claimantProfileId` STRING
,`_removed` STRUCT<`claimantProfileItems`:ARRAY<STRUCT<`createdDateTime`:STRUCT<`d_date`:STRING>
,`createdByAgentId`:STRING
,`description`:STRING
,`id`:STRING
,`value`:STRING
>>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`claimantId`:STRING>
,`_version`:STRING
,`claimantProfileId`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/agent-core/claimantProfile';

DROP TABLE IF EXISTS claimantProfile;

CREATE TABLE claimantprofile STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(`removed_exploded_claimantprofileitems`.`createdByAgentId`, `exploded_claimantprofileitems`.`createdByAgentId`) as claimantProfileItems_createdByAgentId, 
COALESCE(CASE WHEN SUBSTRING(`removed_exploded_claimantprofileitems`.`createdDateTime`.`d_date`, LENGTH(`removed_exploded_claimantprofileitems`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`removed_exploded_claimantprofileitems`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`removed_exploded_claimantprofileitems`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`removed_exploded_claimantprofileitems`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`removed_exploded_claimantprofileitems`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`removed_exploded_claimantprofileitems`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`removed_exploded_claimantprofileitems`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`exploded_claimantprofileitems`.`createdDateTime`.`d_date`, LENGTH(`exploded_claimantprofileitems`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`exploded_claimantprofileitems`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`exploded_claimantprofileitems`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`exploded_claimantprofileitems`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`exploded_claimantprofileitems`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`exploded_claimantprofileitems`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`exploded_claimantprofileitems`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as claimantProfileItems_createdDateTime_d_date, 
COALESCE(`removed_exploded_claimantprofileitems`.`description`, `exploded_claimantprofileitems`.`description`) as claimantProfileItems_description, 
COALESCE(`removed_exploded_claimantprofileitems`.`id`, `exploded_claimantprofileitems`.`id`) as claimantProfileItems_id, 
COALESCE(`removed_exploded_claimantprofileitems`.`value`, `exploded_claimantprofileitems`.`value`) as claimantProfileItems_value, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`claimantProfileId`, `claimantProfileId`) as claimantProfileId, 
COALESCE(`_removed`.`_id`.`claimantId`, `_id`.`claimantId`) as id FROM src_agent_core_claimantprofile_claimantprofile
 LATERAL VIEW OUTER EXPLODE(`claimantProfileItems`) view_exploded_claimantprofileitems AS exploded_claimantprofileitems 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`claimantProfileItems`) view_removed_exploded_claimantprofileitems AS removed_exploded_claimantprofileitems 
;


!echo ------------------------;
!echo ------------------------ contract;
!echo ------------------------;
DROP TABLE IF EXISTS src_core_contract_contract
;

CREATE EXTERNAL TABLE src_core_contract_contract(
`assessmentPeriods` ARRAY<STRUCT<`createdDateTime`:STRUCT<`d_date`:STRING>
,`assessmentPeriodId`:STRING
,`contractId`:STRING
,`endDate`:STRING
,`paymentDate`:STRING
,`processDate`:STRING
,`startDate`:STRING
>>
,`claimSuspension` STRUCT<`suspensionProperties`:STRUCT<`suspensionType`:STRING
,`fraudOfficerContactNumber`:STRING
,`fraudOfficerName`:STRING
,`type`:STRING
,`otherSuspensionType`:STRING>
,`reasonsForSuspension`:ARRAY<STRING>
,`suspensionDate`:STRING>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`closeProcessStartDate` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`contractId`:STRING>
,`claimantsExemptFromWaitingDays` ARRAY<STRING>
,`flags` ARRAY<STRING>
,`people` ARRAY<STRING>
,`_version` STRING
,`annualVerificationCompletionDate` STRING
,`annualVerificationEligibilityDate` STRING
,`claimClosureReason` STRING
,`closedDate` STRING
,`contractId` STRING
,`contractType` STRING
,`coupleContract` BOOLEAN
,`declaredDate` STRING
,`entitlementDate` STRING
,`paymentDayOfMonth` STRING
,`startDate` STRING
,`_removed` STRUCT<`assessmentPeriods`:ARRAY<STRUCT<`createdDateTime`:STRUCT<`d_date`:STRING>
,`assessmentPeriodId`:STRING
,`contractId`:STRING
,`endDate`:STRING
,`paymentDate`:STRING
,`processDate`:STRING
,`startDate`:STRING
>>
,`claimSuspension`:STRUCT<`suspensionProperties`:STRUCT<`suspensionType`:STRING
,`fraudOfficerContactNumber`:STRING
,`fraudOfficerName`:STRING
,`type`:STRING
,`otherSuspensionType`:STRING>
,`reasonsForSuspension`:ARRAY<STRING>
,`suspensionDate`:STRING>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`closeProcessStartDate`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`contractId`:STRING>
,`claimantsExemptFromWaitingDays`:ARRAY<STRING>
,`flags`:ARRAY<STRING>
,`people`:ARRAY<STRING>
,`_version`:STRING
,`annualVerificationCompletionDate`:STRING
,`annualVerificationEligibilityDate`:STRING
,`claimClosureReason`:STRING
,`closedDate`:STRING
,`contractId`:STRING
,`contractType`:STRING
,`coupleContract`:BOOLEAN
,`declaredDate`:STRING
,`entitlementDate`:STRING
,`paymentDayOfMonth`:STRING
,`startDate`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/core/contract';

DROP TABLE IF EXISTS contract;

CREATE TABLE contract STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(`_removed`.`annualVerificationCompletionDate`, `annualVerificationCompletionDate`) as annualVerificationCompletionDate, 
COALESCE(`_removed`.`annualVerificationEligibilityDate`, `annualVerificationEligibilityDate`) as annualVerificationEligibilityDate, 
COALESCE(`removed_exploded_assessmentperiods`.`assessmentPeriodId`, `exploded_assessmentperiods`.`assessmentPeriodId`) as assessmentPeriods_assessmentPeriodId, 
COALESCE(`removed_exploded_assessmentperiods`.`contractId`, `exploded_assessmentperiods`.`contractId`) as assessmentPeriods_contractId, 
COALESCE(CASE WHEN SUBSTRING(`removed_exploded_assessmentperiods`.`createdDateTime`.`d_date`, LENGTH(`removed_exploded_assessmentperiods`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`removed_exploded_assessmentperiods`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`removed_exploded_assessmentperiods`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`removed_exploded_assessmentperiods`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`removed_exploded_assessmentperiods`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`removed_exploded_assessmentperiods`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`removed_exploded_assessmentperiods`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`exploded_assessmentperiods`.`createdDateTime`.`d_date`, LENGTH(`exploded_assessmentperiods`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`exploded_assessmentperiods`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`exploded_assessmentperiods`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`exploded_assessmentperiods`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`exploded_assessmentperiods`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`exploded_assessmentperiods`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`exploded_assessmentperiods`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as assessmentPeriods_createdDateTime_d_date, 
COALESCE(`removed_exploded_assessmentperiods`.`endDate`, `exploded_assessmentperiods`.`endDate`) as assessmentPeriods_endDate, 
COALESCE(`removed_exploded_assessmentperiods`.`paymentDate`, `exploded_assessmentperiods`.`paymentDate`) as assessmentPeriods_paymentDate, 
COALESCE(`removed_exploded_assessmentperiods`.`processDate`, `exploded_assessmentperiods`.`processDate`) as assessmentPeriods_processDate, 
COALESCE(`removed_exploded_assessmentperiods`.`startDate`, `exploded_assessmentperiods`.`startDate`) as assessmentPeriods_startDate, 
COALESCE(`_removed`.`claimClosureReason`, `claimClosureReason`) as claimClosureReason, 
COALESCE(`removed_exploded_claimsuspension_reasonsforsuspension`, `exploded_claimsuspension_reasonsforsuspension`) as claimSuspension_reasonsForSuspension, 
COALESCE(`_removed`.`claimSuspension`.`suspensionDate`, `claimSuspension`.`suspensionDate`) as claimSuspension_suspensionDate, 
COALESCE(`_removed`.`claimSuspension`.`suspensionProperties`.`suspensionType`, `claimSuspension`.`suspensionProperties`.`suspensionType`) as claimSuspension_suspensionProperties_suspensionType, 
COALESCE(`_removed`.`claimSuspension`.`suspensionProperties`.`fraudOfficerContactNumber`, `claimSuspension`.`suspensionProperties`.`fraudOfficerContactNumber`) as claimSuspension_suspensionProperties_fraudOfficerContactNumber, 
COALESCE(`_removed`.`claimSuspension`.`suspensionProperties`.`fraudOfficerName`, `claimSuspension`.`suspensionProperties`.`fraudOfficerName`) as claimSuspension_suspensionProperties_fraudOfficerName, 
COALESCE(`_removed`.`claimSuspension`.`suspensionProperties`.`type`, `claimSuspension`.`suspensionProperties`.`type`) as claimSuspension_suspensionProperties_type, 
COALESCE(`_removed`.`claimSuspension`.`suspensionProperties`.`otherSuspensionType`, `claimSuspension`.`suspensionProperties`.`otherSuspensionType`) as claimSuspension_suspensionProperties_otherSuspensionType, 
COALESCE(`removed_exploded_claimantsexemptfromwaitingdays`, `exploded_claimantsexemptfromwaitingdays`) as claimantsExemptFromWaitingDays, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`closeProcessStartDate`.`d_date`, LENGTH(`_removed`.`closeProcessStartDate`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`closeProcessStartDate`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`closeProcessStartDate`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`closeProcessStartDate`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`closeProcessStartDate`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`closeProcessStartDate`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`closeProcessStartDate`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`closeProcessStartDate`.`d_date`, LENGTH(`closeProcessStartDate`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`closeProcessStartDate`.`d_date`, 1, 10), ' ', SUBSTR(`closeProcessStartDate`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`closeProcessStartDate`.`d_date`, 1, 10), ' ', SUBSTR(`closeProcessStartDate`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`closeProcessStartDate`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`closeProcessStartDate`.`d_date`, 21, 3)) AS TIMESTAMP) END) as closeProcessStartDate_d_date, 
COALESCE(`_removed`.`closedDate`, `closedDate`) as closedDate, 
COALESCE(`_removed`.`contractId`, `contractId`) as contractId, 
COALESCE(`_removed`.`contractType`, `contractType`) as contractType, 
COALESCE(CAST(`_removed`.`coupleContract` as STRING), CAST(`coupleContract` as STRING)) as coupleContract, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declaredDate`, `declaredDate`) as declaredDate, 
COALESCE(`_removed`.`entitlementDate`, `entitlementDate`) as entitlementDate, 
COALESCE(`removed_exploded_flags`, `exploded_flags`) as flags, 
COALESCE(`_removed`.`paymentDayOfMonth`, `paymentDayOfMonth`) as paymentDayOfMonth, 
COALESCE(`removed_exploded_people`, `exploded_people`) as people, 
COALESCE(`_removed`.`startDate`, `startDate`) as startDate, 
COALESCE(`_removed`.`_id`.`contractId`, `_id`.`contractId`) as id FROM src_core_contract_contract
 LATERAL VIEW OUTER EXPLODE(`assessmentPeriods`) view_exploded_assessmentperiods AS exploded_assessmentperiods 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`assessmentPeriods`) view_removed_exploded_assessmentperiods AS removed_exploded_assessmentperiods 
 LATERAL VIEW OUTER EXPLODE(`claimSuspension`.`reasonsForSuspension`) view_exploded_claimsuspension_reasonsforsuspension AS exploded_claimsuspension_reasonsforsuspension 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`claimSuspension`.`reasonsForSuspension`) view_removed_exploded_claimsuspension_reasonsforsuspension AS removed_exploded_claimsuspension_reasonsforsuspension 
 LATERAL VIEW OUTER EXPLODE(`claimantsExemptFromWaitingDays`) view_exploded_claimantsexemptfromwaitingdays AS exploded_claimantsexemptfromwaitingdays 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`claimantsExemptFromWaitingDays`) view_removed_exploded_claimantsexemptfromwaitingdays AS removed_exploded_claimantsexemptfromwaitingdays 
 LATERAL VIEW OUTER EXPLODE(`flags`) view_exploded_flags AS exploded_flags 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`flags`) view_removed_exploded_flags AS removed_exploded_flags 
 LATERAL VIEW OUTER EXPLODE(`people`) view_exploded_people AS exploded_people 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`people`) view_removed_exploded_people AS removed_exploded_people 
;


!echo ------------------------;
!echo ------------------------ carerDeclaration;
!echo ------------------------;
DROP TABLE IF EXISTS src_core_carerdeclaration_carerdeclaration
;

CREATE EXTERNAL TABLE src_core_carerdeclaration_carerdeclaration(
`carerCircumstances` STRUCT<`careeCircumstances`:STRUCT<`addressNumber`:STRING
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
,`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`otherCaringResponsibilities`:STRUCT<`caringForSomeoneElse`:BOOLEAN
,`personReceivesBenefit`:BOOLEAN>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`stoppedCaringCircumstances`:STRUCT<`dueToDeath`:BOOLEAN
,`stoppedDate`:STRING>
,`timeOffCircumstances`:STRUCT<`endDate`:STRING
,`startDate`:STRING>
,`claimantId`:STRING
,`contractId`:STRING
,`moreThan35Hours`:BOOLEAN
,`type`:STRING>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`declarationId`:STRING>
,`_version` STRING
,`declarationId` STRING
,`processId` STRING
,`type` STRING
,`_removed` STRUCT<`carerCircumstances`:STRUCT<`careeCircumstances`:STRUCT<`addressNumber`:STRING
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
,`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`otherCaringResponsibilities`:STRUCT<`caringForSomeoneElse`:BOOLEAN
,`personReceivesBenefit`:BOOLEAN>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`stoppedCaringCircumstances`:STRUCT<`dueToDeath`:BOOLEAN
,`stoppedDate`:STRING>
,`timeOffCircumstances`:STRUCT<`endDate`:STRING
,`startDate`:STRING>
,`claimantId`:STRING
,`contractId`:STRING
,`moreThan35Hours`:BOOLEAN
,`type`:STRING>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`declarationId`:STRING>
,`_version`:STRING
,`declarationId`:STRING
,`processId`:STRING
,`type`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/core/carerDeclaration';

DROP TABLE IF EXISTS carerDeclaration;

CREATE TABLE carerdeclaration STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(`_removed`.`carerCircumstances`.`careeCircumstances`.`addressNumber`, `carerCircumstances`.`careeCircumstances`.`addressNumber`) as carerCircumstances_careeCircumstances_addressNumber, 
COALESCE(`_removed`.`carerCircumstances`.`careeCircumstances`.`dateOfBirth`, `carerCircumstances`.`careeCircumstances`.`dateOfBirth`) as carerCircumstances_careeCircumstances_dateOfBirth, 
COALESCE(`_removed`.`carerCircumstances`.`careeCircumstances`.`firstName`, `carerCircumstances`.`careeCircumstances`.`firstName`) as carerCircumstances_careeCircumstances_firstName, 
COALESCE(`_removed`.`carerCircumstances`.`careeCircumstances`.`lastName`, `carerCircumstances`.`careeCircumstances`.`lastName`) as carerCircumstances_careeCircumstances_lastName, 
COALESCE(`_removed`.`carerCircumstances`.`careeCircumstances`.`middleName`, `carerCircumstances`.`careeCircumstances`.`middleName`) as carerCircumstances_careeCircumstances_middleName, 
COALESCE(`_removed`.`carerCircumstances`.`careeCircumstances`.`postcode`, `carerCircumstances`.`careeCircumstances`.`postcode`) as carerCircumstances_careeCircumstances_postcode, 
COALESCE(`_removed`.`carerCircumstances`.`careeCircumstances`.`relationship`, `carerCircumstances`.`careeCircumstances`.`relationship`) as carerCircumstances_careeCircumstances_relationship, 
COALESCE(`_removed`.`carerCircumstances`.`careeCircumstances`.`townCity`, `carerCircumstances`.`careeCircumstances`.`townCity`) as carerCircumstances_careeCircumstances_townCity, 
COALESCE(CAST(`_removed`.`carerCircumstances`.`caringCircumstances`.`caringForSomeone` as STRING), CAST(`carerCircumstances`.`caringCircumstances`.`caringForSomeone` as STRING)) as carerCircumstances_caringCircumstances_caringForSomeone, 
COALESCE(CAST(`_removed`.`carerCircumstances`.`caringCircumstances`.`personReceivesBenefit` as STRING), CAST(`carerCircumstances`.`caringCircumstances`.`personReceivesBenefit` as STRING)) as carerCircumstances_caringCircumstances_personReceivesBenefit, 
COALESCE(`_removed`.`carerCircumstances`.`claimantId`, `carerCircumstances`.`claimantId`) as carerCircumstances_claimantId, 
COALESCE(`_removed`.`carerCircumstances`.`contractId`, `carerCircumstances`.`contractId`) as carerCircumstances_contractId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`carerCircumstances`.`declaredDateTime`.`d_date`, LENGTH(`_removed`.`carerCircumstances`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`carerCircumstances`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`carerCircumstances`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`carerCircumstances`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`carerCircumstances`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`carerCircumstances`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`carerCircumstances`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`carerCircumstances`.`declaredDateTime`.`d_date`, LENGTH(`carerCircumstances`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`carerCircumstances`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`carerCircumstances`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`carerCircumstances`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`carerCircumstances`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`carerCircumstances`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`carerCircumstances`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as carerCircumstances_declaredDateTime_d_date, 
COALESCE(CAST(`_removed`.`carerCircumstances`.`effectiveDate`.`hasDate` as STRING), CAST(`carerCircumstances`.`effectiveDate`.`hasDate` as STRING)) as carerCircumstances_effectiveDate_hasDate, 
COALESCE(`_removed`.`carerCircumstances`.`effectiveDate`.`knownDate`, `carerCircumstances`.`effectiveDate`.`knownDate`) as carerCircumstances_effectiveDate_knownDate, 
COALESCE(`_removed`.`carerCircumstances`.`effectiveDate`.`type`, `carerCircumstances`.`effectiveDate`.`type`) as carerCircumstances_effectiveDate_type, 
COALESCE(`_removed`.`carerCircumstances`.`effectiveDate`.`date`, `carerCircumstances`.`effectiveDate`.`date`) as carerCircumstances_effectiveDate_date, 
COALESCE(CAST(`_removed`.`carerCircumstances`.`moreThan35Hours` as STRING), CAST(`carerCircumstances`.`moreThan35Hours` as STRING)) as carerCircumstances_moreThan35Hours, 
COALESCE(CAST(`_removed`.`carerCircumstances`.`otherCaringResponsibilities`.`caringForSomeoneElse` as STRING), CAST(`carerCircumstances`.`otherCaringResponsibilities`.`caringForSomeoneElse` as STRING)) as carerCircumstances_otherCaringResponsibilities_caringForSomeoneElse, 
COALESCE(CAST(`_removed`.`carerCircumstances`.`otherCaringResponsibilities`.`personReceivesBenefit` as STRING), CAST(`carerCircumstances`.`otherCaringResponsibilities`.`personReceivesBenefit` as STRING)) as carerCircumstances_otherCaringResponsibilities_personReceivesBenefit, 
COALESCE(CAST(`_removed`.`carerCircumstances`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`carerCircumstances`.`paymentEffectiveDate`.`hasDate` as STRING)) as carerCircumstances_paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`carerCircumstances`.`paymentEffectiveDate`.`knownDate`, `carerCircumstances`.`paymentEffectiveDate`.`knownDate`) as carerCircumstances_paymentEffectiveDate_knownDate, 
COALESCE(`_removed`.`carerCircumstances`.`paymentEffectiveDate`.`type`, `carerCircumstances`.`paymentEffectiveDate`.`type`) as carerCircumstances_paymentEffectiveDate_type, 
COALESCE(`_removed`.`carerCircumstances`.`paymentEffectiveDate`.`date`, `carerCircumstances`.`paymentEffectiveDate`.`date`) as carerCircumstances_paymentEffectiveDate_date, 
COALESCE(CAST(`_removed`.`carerCircumstances`.`stoppedCaringCircumstances`.`dueToDeath` as STRING), CAST(`carerCircumstances`.`stoppedCaringCircumstances`.`dueToDeath` as STRING)) as carerCircumstances_stoppedCaringCircumstances_dueToDeath, 
COALESCE(`_removed`.`carerCircumstances`.`stoppedCaringCircumstances`.`stoppedDate`, `carerCircumstances`.`stoppedCaringCircumstances`.`stoppedDate`) as carerCircumstances_stoppedCaringCircumstances_stoppedDate, 
COALESCE(`_removed`.`carerCircumstances`.`timeOffCircumstances`.`endDate`, `carerCircumstances`.`timeOffCircumstances`.`endDate`) as carerCircumstances_timeOffCircumstances_endDate, 
COALESCE(`_removed`.`carerCircumstances`.`timeOffCircumstances`.`startDate`, `carerCircumstances`.`timeOffCircumstances`.`startDate`) as carerCircumstances_timeOffCircumstances_startDate, 
COALESCE(`_removed`.`carerCircumstances`.`type`, `carerCircumstances`.`type`) as carerCircumstances_type, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(`_removed`.`processId`, `processId`) as processId, 
COALESCE(`_removed`.`type`, `type`) as type, 
COALESCE(`_removed`.`_id`.`declarationId`, `_id`.`declarationId`) as id FROM src_core_carerdeclaration_carerdeclaration
 src_core_carerdeclaration_carerdeclaration;


!echo ------------------------;
!echo ------------------------ systemWorkGroupAllocation;
!echo ------------------------;
DROP TABLE IF EXISTS src_agent_core_systemworkgroupallocation_systemworkgroupallocation
;

CREATE EXTERNAL TABLE src_agent_core_systemworkgroupallocation_systemworkgroupallocation(
`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`systemWorkGroupAllocationId`:STRING>
,`_version` STRING
,`calculatedWorkGroup` STRING
,`claimantId` STRING
,`contractId` STRING
,`effectiveDate` STRING
,`expectedWorkingHours` STRING
,`superseded` BOOLEAN
,`systemWorkGroupAllocationId` STRING
,`_removed` STRUCT<`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`systemWorkGroupAllocationId`:STRING>
,`_version`:STRING
,`calculatedWorkGroup`:STRING
,`claimantId`:STRING
,`contractId`:STRING
,`effectiveDate`:STRING
,`expectedWorkingHours`:STRING
,`superseded`:BOOLEAN
,`systemWorkGroupAllocationId`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/agent-core/systemWorkGroupAllocation';

DROP TABLE IF EXISTS systemWorkGroupAllocation;

CREATE TABLE systemworkgroupallocation STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(`_removed`.`calculatedWorkGroup`, `calculatedWorkGroup`) as calculatedWorkGroup, 
COALESCE(`_removed`.`claimantId`, `claimantId`) as claimantId, 
COALESCE(`_removed`.`contractId`, `contractId`) as contractId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`effectiveDate`, `effectiveDate`) as effectiveDate, 
COALESCE(`_removed`.`expectedWorkingHours`, `expectedWorkingHours`) as expectedWorkingHours, 
COALESCE(CAST(`_removed`.`superseded` as STRING), CAST(`superseded` as STRING)) as superseded, 
COALESCE(`_removed`.`systemWorkGroupAllocationId`, `systemWorkGroupAllocationId`) as systemWorkGroupAllocationId, 
COALESCE(`_removed`.`_id`.`systemWorkGroupAllocationId`, `_id`.`systemWorkGroupAllocationId`) as id FROM src_agent_core_systemworkgroupallocation_systemworkgroupallocation
 src_agent_core_systemworkgroupallocation_systemworkgroupallocation;


!echo ------------------------;
!echo ------------------------ team;
!echo ------------------------;
DROP TABLE IF EXISTS src_agent_core_team_team
;

CREATE EXTERNAL TABLE src_agent_core_team_team(
`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`teamId`:STRING>
,`teamMemberIds` ARRAY<STRING>
,`_version` STRING
,`locationId` STRING
,`name` STRING
,`otherTeamTypeDescription` STRING
,`teamId` STRING
,`teamLeaderId` STRING
,`teamType` STRING
,`_removed` STRUCT<`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`teamId`:STRING>
,`teamMemberIds`:ARRAY<STRING>
,`_version`:STRING
,`locationId`:STRING
,`name`:STRING
,`otherTeamTypeDescription`:STRING
,`teamId`:STRING
,`teamLeaderId`:STRING
,`teamType`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/agent-core/team';

DROP TABLE IF EXISTS team;

CREATE TABLE team STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`locationId`, `locationId`) as locationId, 
COALESCE(`_removed`.`name`, `name`) as name, 
COALESCE(`_removed`.`otherTeamTypeDescription`, `otherTeamTypeDescription`) as otherTeamTypeDescription, 
COALESCE(`_removed`.`teamId`, `teamId`) as teamId, 
COALESCE(`_removed`.`teamLeaderId`, `teamLeaderId`) as teamLeaderId, 
COALESCE(`removed_exploded_teammemberids`, `exploded_teammemberids`) as teamMemberIds, 
COALESCE(`_removed`.`teamType`, `teamType`) as teamType, 
COALESCE(`_removed`.`_id`.`teamId`, `_id`.`teamId`) as id FROM src_agent_core_team_team
 LATERAL VIEW OUTER EXPLODE(`teamMemberIds`) view_exploded_teammemberids AS exploded_teammemberids 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`teamMemberIds`) view_removed_exploded_teammemberids AS removed_exploded_teammemberids 
;


!echo ------------------------;
!echo ------------------------ advanceDebtPosition;
!echo ------------------------;
DROP TABLE IF EXISTS src_advances_advancedebtposition_advancedebtposition
;

CREATE EXTERNAL TABLE src_advances_advancedebtposition_advancedebtposition(
`advanceProgressEvents` ARRAY<STRUCT<`advanceProgressSource`:STRING
,`assessmentPeriodId`:STRING
,`debtRepaid`:STRING
,`eventDate`:STRING
>>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`advanceGroupId`:STRING>
,`_version` STRING
,`advanceGroupId` STRING
,`originalAmountOutstanding` STRING
,`_removed` STRUCT<`advanceProgressEvents`:ARRAY<STRUCT<`advanceProgressSource`:STRING
,`assessmentPeriodId`:STRING
,`debtRepaid`:STRING
,`eventDate`:STRING
>>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`advanceGroupId`:STRING>
,`_version`:STRING
,`advanceGroupId`:STRING
,`originalAmountOutstanding`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/advances/advanceDebtPosition';

DROP TABLE IF EXISTS advanceDebtPosition;

CREATE TABLE advancedebtposition STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(`_removed`.`advanceGroupId`, `advanceGroupId`) as advanceGroupId, 
COALESCE(`removed_exploded_advanceprogressevents`.`advanceProgressSource`, `exploded_advanceprogressevents`.`advanceProgressSource`) as advanceProgressEvents_advanceProgressSource, 
COALESCE(`removed_exploded_advanceprogressevents`.`assessmentPeriodId`, `exploded_advanceprogressevents`.`assessmentPeriodId`) as advanceProgressEvents_assessmentPeriodId, 
COALESCE(`removed_exploded_advanceprogressevents`.`debtRepaid`, `exploded_advanceprogressevents`.`debtRepaid`) as advanceProgressEvents_debtRepaid, 
COALESCE(`removed_exploded_advanceprogressevents`.`eventDate`, `exploded_advanceprogressevents`.`eventDate`) as advanceProgressEvents_eventDate, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`originalAmountOutstanding`, `originalAmountOutstanding`) as originalAmountOutstanding, 
COALESCE(`_removed`.`_id`.`advanceGroupId`, `_id`.`advanceGroupId`) as id FROM src_advances_advancedebtposition_advancedebtposition
 LATERAL VIEW OUTER EXPLODE(`advanceProgressEvents`) view_exploded_advanceprogressevents AS exploded_advanceprogressevents 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`advanceProgressEvents`) view_removed_exploded_advanceprogressevents AS removed_exploded_advanceprogressevents 
;


!echo ------------------------;
!echo ------------------------ healthDeclaration;
!echo ------------------------;
DROP TABLE IF EXISTS src_core_healthdeclaration_healthdeclaration
;

CREATE EXTERNAL TABLE src_core_healthdeclaration_healthdeclaration(
`healthCircumstances` STRUCT<`declaredDateTime`:STRUCT<`d_date`:STRING>
,`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`armedForcesIndependencePayment`:BOOLEAN
,`attendanceAllowance`:BOOLEAN
,`claimantId`:STRING
,`constantAttendanceAllowance`:BOOLEAN
,`contractId`:STRING
,`dlaAllowance`:BOOLEAN
,`healthCondition`:BOOLEAN
,`piPayment`:BOOLEAN
,`type`:STRING>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`declarationId`:STRING>
,`_version` STRING
,`declarationId` STRING
,`processId` STRING
,`type` STRING
,`_removed` STRUCT<`healthCircumstances`:STRUCT<`declaredDateTime`:STRUCT<`d_date`:STRING>
,`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`armedForcesIndependencePayment`:BOOLEAN
,`attendanceAllowance`:BOOLEAN
,`claimantId`:STRING
,`constantAttendanceAllowance`:BOOLEAN
,`contractId`:STRING
,`dlaAllowance`:BOOLEAN
,`healthCondition`:BOOLEAN
,`piPayment`:BOOLEAN
,`type`:STRING>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`declarationId`:STRING>
,`_version`:STRING
,`declarationId`:STRING
,`processId`:STRING
,`type`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/core/healthDeclaration';

DROP TABLE IF EXISTS healthDeclaration;

CREATE TABLE healthdeclaration STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CAST(`_removed`.`healthCircumstances`.`armedForcesIndependencePayment` as STRING), CAST(`healthCircumstances`.`armedForcesIndependencePayment` as STRING)) as healthCircumstances_armedForcesIndependencePayment, 
COALESCE(CAST(`_removed`.`healthCircumstances`.`attendanceAllowance` as STRING), CAST(`healthCircumstances`.`attendanceAllowance` as STRING)) as healthCircumstances_attendanceAllowance, 
COALESCE(`_removed`.`healthCircumstances`.`claimantId`, `healthCircumstances`.`claimantId`) as healthCircumstances_claimantId, 
COALESCE(CAST(`_removed`.`healthCircumstances`.`constantAttendanceAllowance` as STRING), CAST(`healthCircumstances`.`constantAttendanceAllowance` as STRING)) as healthCircumstances_constantAttendanceAllowance, 
COALESCE(`_removed`.`healthCircumstances`.`contractId`, `healthCircumstances`.`contractId`) as healthCircumstances_contractId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`healthCircumstances`.`declaredDateTime`.`d_date`, LENGTH(`_removed`.`healthCircumstances`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`healthCircumstances`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`healthCircumstances`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`healthCircumstances`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`healthCircumstances`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`healthCircumstances`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`healthCircumstances`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`healthCircumstances`.`declaredDateTime`.`d_date`, LENGTH(`healthCircumstances`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`healthCircumstances`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`healthCircumstances`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`healthCircumstances`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`healthCircumstances`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`healthCircumstances`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`healthCircumstances`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as healthCircumstances_declaredDateTime_d_date, 
COALESCE(CAST(`_removed`.`healthCircumstances`.`dlaAllowance` as STRING), CAST(`healthCircumstances`.`dlaAllowance` as STRING)) as healthCircumstances_dlaAllowance, 
COALESCE(CAST(`_removed`.`healthCircumstances`.`effectiveDate`.`hasDate` as STRING), CAST(`healthCircumstances`.`effectiveDate`.`hasDate` as STRING)) as healthCircumstances_effectiveDate_hasDate, 
COALESCE(`_removed`.`healthCircumstances`.`effectiveDate`.`knownDate`, `healthCircumstances`.`effectiveDate`.`knownDate`) as healthCircumstances_effectiveDate_knownDate, 
COALESCE(`_removed`.`healthCircumstances`.`effectiveDate`.`type`, `healthCircumstances`.`effectiveDate`.`type`) as healthCircumstances_effectiveDate_type, 
COALESCE(`_removed`.`healthCircumstances`.`effectiveDate`.`date`, `healthCircumstances`.`effectiveDate`.`date`) as healthCircumstances_effectiveDate_date, 
COALESCE(CAST(`_removed`.`healthCircumstances`.`healthCondition` as STRING), CAST(`healthCircumstances`.`healthCondition` as STRING)) as healthCircumstances_healthCondition, 
COALESCE(CAST(`_removed`.`healthCircumstances`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`healthCircumstances`.`paymentEffectiveDate`.`hasDate` as STRING)) as healthCircumstances_paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`healthCircumstances`.`paymentEffectiveDate`.`knownDate`, `healthCircumstances`.`paymentEffectiveDate`.`knownDate`) as healthCircumstances_paymentEffectiveDate_knownDate, 
COALESCE(`_removed`.`healthCircumstances`.`paymentEffectiveDate`.`type`, `healthCircumstances`.`paymentEffectiveDate`.`type`) as healthCircumstances_paymentEffectiveDate_type, 
COALESCE(`_removed`.`healthCircumstances`.`paymentEffectiveDate`.`date`, `healthCircumstances`.`paymentEffectiveDate`.`date`) as healthCircumstances_paymentEffectiveDate_date, 
COALESCE(CAST(`_removed`.`healthCircumstances`.`piPayment` as STRING), CAST(`healthCircumstances`.`piPayment` as STRING)) as healthCircumstances_piPayment, 
COALESCE(`_removed`.`healthCircumstances`.`type`, `healthCircumstances`.`type`) as healthCircumstances_type, 
COALESCE(`_removed`.`processId`, `processId`) as processId, 
COALESCE(`_removed`.`type`, `type`) as type, 
COALESCE(`_removed`.`_id`.`declarationId`, `_id`.`declarationId`) as id FROM src_core_healthdeclaration_healthdeclaration
 src_core_healthdeclaration_healthdeclaration;


!echo ------------------------;
!echo ------------------------ advanceGroup;
!echo ------------------------;
DROP TABLE IF EXISTS src_advances_advancegroup_advancegroup
;

CREATE EXTERNAL TABLE src_advances_advancegroup_advancegroup(
`advanceDeferral` STRUCT<`deferralPeriods`:ARRAY<STRUCT<`deferredFromDate`:STRING
,`monthsDeferred`:STRING
>>>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`advanceGroupId`:STRING>
,`claimants` ARRAY<STRING>
,`people` ARRAY<STRING>
,`_version` STRING
,`advanceAmount` STRING
,`advanceGroupId` STRING
,`advancePaidToClaimantDeadline` STRING
,`advanceType` STRING
,`contractId` STRING
,`isTransfer` BOOLEAN
,`monthlyRepaymentAmount` STRING
,`originalAdvanceGroupId` STRING
,`repaymentMonths` STRING
,`_removed` STRUCT<`advanceDeferral`:STRUCT<`deferralPeriods`:ARRAY<STRUCT<`deferredFromDate`:STRING
,`monthsDeferred`:STRING
>>>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`advanceGroupId`:STRING>
,`claimants`:ARRAY<STRING>
,`people`:ARRAY<STRING>
,`_version`:STRING
,`advanceAmount`:STRING
,`advanceGroupId`:STRING
,`advancePaidToClaimantDeadline`:STRING
,`advanceType`:STRING
,`contractId`:STRING
,`isTransfer`:BOOLEAN
,`monthlyRepaymentAmount`:STRING
,`originalAdvanceGroupId`:STRING
,`repaymentMonths`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/advances/advanceGroup';

DROP TABLE IF EXISTS advanceGroup;

CREATE TABLE advancegroup STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(`_removed`.`advanceAmount`, `advanceAmount`) as advanceAmount, 
COALESCE(`removed_exploded_advancedeferral_deferralperiods`.`deferredFromDate`, `exploded_advancedeferral_deferralperiods`.`deferredFromDate`) as advanceDeferral_deferralPeriods_deferredFromDate, 
COALESCE(`removed_exploded_advancedeferral_deferralperiods`.`monthsDeferred`, `exploded_advancedeferral_deferralperiods`.`monthsDeferred`) as advanceDeferral_deferralPeriods_monthsDeferred, 
COALESCE(`_removed`.`advanceGroupId`, `advanceGroupId`) as advanceGroupId, 
COALESCE(`_removed`.`advancePaidToClaimantDeadline`, `advancePaidToClaimantDeadline`) as advancePaidToClaimantDeadline, 
COALESCE(`_removed`.`advanceType`, `advanceType`) as advanceType, 
COALESCE(`removed_exploded_claimants`, `exploded_claimants`) as claimants, 
COALESCE(`_removed`.`contractId`, `contractId`) as contractId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(CAST(`_removed`.`isTransfer` as STRING), CAST(`isTransfer` as STRING)) as isTransfer, 
COALESCE(`_removed`.`monthlyRepaymentAmount`, `monthlyRepaymentAmount`) as monthlyRepaymentAmount, 
COALESCE(`_removed`.`originalAdvanceGroupId`, `originalAdvanceGroupId`) as originalAdvanceGroupId, 
COALESCE(`removed_exploded_people`, `exploded_people`) as people, 
COALESCE(`_removed`.`repaymentMonths`, `repaymentMonths`) as repaymentMonths, 
COALESCE(`_removed`.`_id`.`advanceGroupId`, `_id`.`advanceGroupId`) as id FROM src_advances_advancegroup_advancegroup
 LATERAL VIEW OUTER EXPLODE(`advanceDeferral`.`deferralPeriods`) view_exploded_advancedeferral_deferralperiods AS exploded_advancedeferral_deferralperiods 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`advanceDeferral`.`deferralPeriods`) view_removed_exploded_advancedeferral_deferralperiods AS removed_exploded_advancedeferral_deferralperiods 
 LATERAL VIEW OUTER EXPLODE(`claimants`) view_exploded_claimants AS exploded_claimants 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`claimants`) view_removed_exploded_claimants AS removed_exploded_claimants 
 LATERAL VIEW OUTER EXPLODE(`people`) view_exploded_people AS exploded_people 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`people`) view_removed_exploded_people AS removed_exploded_people 
;


!echo ------------------------;
!echo ------------------------ fraudPenalty;
!echo ------------------------;
DROP TABLE IF EXISTS src_penalties_and_deductions_fraudpenalty_fraudpenalty
;

CREATE EXTERNAL TABLE src_penalties_and_deductions_fraudpenalty_fraudpenalty(
`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`fraudPenaltyId`:STRING>
,`_version` STRING
,`contractId` STRING
,`dateOfConviction` STRING
,`deleteExplanation` STRING
,`deleted` BOOLEAN
,`determinationDate` STRING
,`durationInDays` STRING
,`fraudPenaltyId` STRING
,`personId` STRING
,`twoStrike` BOOLEAN
,`universalCreditFraudPenalty` BOOLEAN
,`migratedPercentageRate` STRING
,`offenceDate` STRING
,`originalBenefitType` STRING
,`transitionProtectionApplies` BOOLEAN
,`type` STRING
,`_removed` STRUCT<`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`fraudPenaltyId`:STRING>
,`_version`:STRING
,`contractId`:STRING
,`dateOfConviction`:STRING
,`deleteExplanation`:STRING
,`deleted`:BOOLEAN
,`determinationDate`:STRING
,`durationInDays`:STRING
,`fraudPenaltyId`:STRING
,`personId`:STRING
,`twoStrike`:BOOLEAN
,`universalCreditFraudPenalty`:BOOLEAN
,`migratedPercentageRate`:STRING
,`offenceDate`:STRING
,`originalBenefitType`:STRING
,`transitionProtectionApplies`:BOOLEAN
,`type`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/penalties-and-deductions/fraudPenalty';

DROP TABLE IF EXISTS fraudPenalty;

CREATE TABLE fraudpenalty STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(`_removed`.`contractId`, `contractId`) as contractId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`dateOfConviction`, `dateOfConviction`) as dateOfConviction, 
COALESCE(`_removed`.`deleteExplanation`, `deleteExplanation`) as deleteExplanation, 
COALESCE(CAST(`_removed`.`deleted` as STRING), CAST(`deleted` as STRING)) as deleted, 
COALESCE(`_removed`.`determinationDate`, `determinationDate`) as determinationDate, 
COALESCE(`_removed`.`durationInDays`, `durationInDays`) as durationInDays, 
COALESCE(`_removed`.`fraudPenaltyId`, `fraudPenaltyId`) as fraudPenaltyId, 
COALESCE(`_removed`.`personId`, `personId`) as personId, 
COALESCE(CAST(`_removed`.`twoStrike` as STRING), CAST(`twoStrike` as STRING)) as twoStrike, 
COALESCE(CAST(`_removed`.`universalCreditFraudPenalty` as STRING), CAST(`universalCreditFraudPenalty` as STRING)) as universalCreditFraudPenalty, 
COALESCE(`_removed`.`migratedPercentageRate`, `migratedPercentageRate`) as migratedPercentageRate, 
COALESCE(`_removed`.`offenceDate`, `offenceDate`) as offenceDate, 
COALESCE(`_removed`.`originalBenefitType`, `originalBenefitType`) as originalBenefitType, 
COALESCE(CAST(`_removed`.`transitionProtectionApplies` as STRING), CAST(`transitionProtectionApplies` as STRING)) as transitionProtectionApplies, 
COALESCE(`_removed`.`type`, `type`) as type, 
COALESCE(`_removed`.`_id`.`fraudPenaltyId`, `_id`.`fraudPenaltyId`) as id FROM src_penalties_and_deductions_fraudpenalty_fraudpenalty
 src_penalties_and_deductions_fraudpenalty_fraudpenalty;


!echo ------------------------;
!echo ------------------------ workAndEarningsDeclaration;
!echo ------------------------;
DROP TABLE IF EXISTS src_core_workandearningsdeclaration_workandearningsdeclaration
;

CREATE EXTERNAL TABLE src_core_workandearningsdeclaration_workandearningsdeclaration(
`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`childTaxCredit` STRUCT<`dateStopped`:STRING
,`receivedInLast5Weeks`:BOOLEAN>
,`earnings` STRUCT<`amount`:STRING
,`hoursPerWeek`:STRING
,`paymentFrequency`:STRING>
,`effectiveDate` STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`employerAndMaternityAllowance` STRUCT<`maternityAllowance`:BOOLEAN
,`maternityAllowanceAmount`:STRING
,`maternityAllowancePaymentFrequency`:STRING
,`otherEmployerPaymentFrequency`:STRING
,`otherPayFromEmployer`:BOOLEAN
,`otherPayFromEmployerAmount`:STRING>
,`futureEmployment` STRUCT<`expectedEarnings`:STRING
,`hoursPerWeek`:STRING
,`paymentFrequency`:STRING
,`startDate`:STRING
,`startingEmployment`:BOOLEAN>
,`paymentEffectiveDate` STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`workHistory` STRUCT<`dateStoppedWorking`:STRING
,`stoppedWorkingInLast9Months`:BOOLEAN>
,`workingTaxCredit` STRUCT<`dateStopped`:STRING
,`receivedInLast5Weeks`:BOOLEAN>
,`_id` STRUCT<`declarationId`:STRING>
,`_version` STRING
,`citizenId` STRING
,`contractId` STRING
,`declarationId` STRING
,`employmentStatus` STRING
,`expectedPreviousEarnings` STRING
,`processId` STRING
,`startingSelfEmployment` BOOLEAN
,`working` BOOLEAN
,`type` STRING
,`_removed` STRUCT<`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`childTaxCredit`:STRUCT<`dateStopped`:STRING
,`receivedInLast5Weeks`:BOOLEAN>
,`earnings`:STRUCT<`amount`:STRING
,`hoursPerWeek`:STRING
,`paymentFrequency`:STRING>
,`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
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
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`workHistory`:STRUCT<`dateStoppedWorking`:STRING
,`stoppedWorkingInLast9Months`:BOOLEAN>
,`workingTaxCredit`:STRUCT<`dateStopped`:STRING
,`receivedInLast5Weeks`:BOOLEAN>
,`_id`:STRUCT<`declarationId`:STRING>
,`_version`:STRING
,`citizenId`:STRING
,`contractId`:STRING
,`declarationId`:STRING
,`employmentStatus`:STRING
,`expectedPreviousEarnings`:STRING
,`processId`:STRING
,`startingSelfEmployment`:BOOLEAN
,`working`:BOOLEAN
,`type`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/core/workAndEarningsDeclaration';

DROP TABLE IF EXISTS workAndEarningsDeclaration;

CREATE TABLE workandearningsdeclaration STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(`_removed`.`childTaxCredit`.`dateStopped`, `childTaxCredit`.`dateStopped`) as childTaxCredit_dateStopped, 
COALESCE(CAST(`_removed`.`childTaxCredit`.`receivedInLast5Weeks` as STRING), CAST(`childTaxCredit`.`receivedInLast5Weeks` as STRING)) as childTaxCredit_receivedInLast5Weeks, 
COALESCE(`_removed`.`citizenId`, `citizenId`) as citizenId, 
COALESCE(`_removed`.`contractId`, `contractId`) as contractId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(`_removed`.`earnings`.`amount`, `earnings`.`amount`) as earnings_amount, 
COALESCE(`_removed`.`earnings`.`hoursPerWeek`, `earnings`.`hoursPerWeek`) as earnings_hoursPerWeek, 
COALESCE(`_removed`.`earnings`.`paymentFrequency`, `earnings`.`paymentFrequency`) as earnings_paymentFrequency, 
COALESCE(CAST(`_removed`.`effectiveDate`.`hasDate` as STRING), CAST(`effectiveDate`.`hasDate` as STRING)) as effectiveDate_hasDate, 
COALESCE(`_removed`.`effectiveDate`.`knownDate`, `effectiveDate`.`knownDate`) as effectiveDate_knownDate, 
COALESCE(`_removed`.`effectiveDate`.`type`, `effectiveDate`.`type`) as effectiveDate_type, 
COALESCE(`_removed`.`effectiveDate`.`date`, `effectiveDate`.`date`) as effectiveDate_date, 
COALESCE(CAST(`_removed`.`employerAndMaternityAllowance`.`maternityAllowance` as STRING), CAST(`employerAndMaternityAllowance`.`maternityAllowance` as STRING)) as employerAndMaternityAllowance_maternityAllowance, 
COALESCE(`_removed`.`employerAndMaternityAllowance`.`maternityAllowanceAmount`, `employerAndMaternityAllowance`.`maternityAllowanceAmount`) as employerAndMaternityAllowance_maternityAllowanceAmount, 
COALESCE(`_removed`.`employerAndMaternityAllowance`.`maternityAllowancePaymentFrequency`, `employerAndMaternityAllowance`.`maternityAllowancePaymentFrequency`) as employerAndMaternityAllowance_maternityAllowancePaymentFrequency, 
COALESCE(`_removed`.`employerAndMaternityAllowance`.`otherEmployerPaymentFrequency`, `employerAndMaternityAllowance`.`otherEmployerPaymentFrequency`) as employerAndMaternityAllowance_otherEmployerPaymentFrequency, 
COALESCE(CAST(`_removed`.`employerAndMaternityAllowance`.`otherPayFromEmployer` as STRING), CAST(`employerAndMaternityAllowance`.`otherPayFromEmployer` as STRING)) as employerAndMaternityAllowance_otherPayFromEmployer, 
COALESCE(`_removed`.`employerAndMaternityAllowance`.`otherPayFromEmployerAmount`, `employerAndMaternityAllowance`.`otherPayFromEmployerAmount`) as employerAndMaternityAllowance_otherPayFromEmployerAmount, 
COALESCE(`_removed`.`employmentStatus`, `employmentStatus`) as employmentStatus, 
COALESCE(`_removed`.`expectedPreviousEarnings`, `expectedPreviousEarnings`) as expectedPreviousEarnings, 
COALESCE(`_removed`.`futureEmployment`.`expectedEarnings`, `futureEmployment`.`expectedEarnings`) as futureEmployment_expectedEarnings, 
COALESCE(`_removed`.`futureEmployment`.`hoursPerWeek`, `futureEmployment`.`hoursPerWeek`) as futureEmployment_hoursPerWeek, 
COALESCE(`_removed`.`futureEmployment`.`paymentFrequency`, `futureEmployment`.`paymentFrequency`) as futureEmployment_paymentFrequency, 
COALESCE(`_removed`.`futureEmployment`.`startDate`, `futureEmployment`.`startDate`) as futureEmployment_startDate, 
COALESCE(CAST(`_removed`.`futureEmployment`.`startingEmployment` as STRING), CAST(`futureEmployment`.`startingEmployment` as STRING)) as futureEmployment_startingEmployment, 
COALESCE(CAST(`_removed`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`paymentEffectiveDate`.`hasDate` as STRING)) as paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`paymentEffectiveDate`.`knownDate`, `paymentEffectiveDate`.`knownDate`) as paymentEffectiveDate_knownDate, 
COALESCE(`_removed`.`paymentEffectiveDate`.`type`, `paymentEffectiveDate`.`type`) as paymentEffectiveDate_type, 
COALESCE(`_removed`.`paymentEffectiveDate`.`date`, `paymentEffectiveDate`.`date`) as paymentEffectiveDate_date, 
COALESCE(`_removed`.`processId`, `processId`) as processId, 
COALESCE(CAST(`_removed`.`startingSelfEmployment` as STRING), CAST(`startingSelfEmployment` as STRING)) as startingSelfEmployment, 
COALESCE(`_removed`.`workHistory`.`dateStoppedWorking`, `workHistory`.`dateStoppedWorking`) as workHistory_dateStoppedWorking, 
COALESCE(CAST(`_removed`.`workHistory`.`stoppedWorkingInLast9Months` as STRING), CAST(`workHistory`.`stoppedWorkingInLast9Months` as STRING)) as workHistory_stoppedWorkingInLast9Months, 
COALESCE(CAST(`_removed`.`working` as STRING), CAST(`working` as STRING)) as working, 
COALESCE(`_removed`.`workingTaxCredit`.`dateStopped`, `workingTaxCredit`.`dateStopped`) as workingTaxCredit_dateStopped, 
COALESCE(CAST(`_removed`.`workingTaxCredit`.`receivedInLast5Weeks` as STRING), CAST(`workingTaxCredit`.`receivedInLast5Weeks` as STRING)) as workingTaxCredit_receivedInLast5Weeks, 
COALESCE(`_removed`.`type`, `type`) as type, 
COALESCE(`_removed`.`_id`.`declarationId`, `_id`.`declarationId`) as id FROM src_core_workandearningsdeclaration_workandearningsdeclaration
 src_core_workandearningsdeclaration_workandearningsdeclaration;


!echo ------------------------;
!echo ------------------------ debtPosition;
!echo ------------------------;
DROP TABLE IF EXISTS src_deductions_debtposition_debtposition
;

CREATE EXTERNAL TABLE src_deductions_debtposition_debtposition(
`details` ARRAY<STRUCT<`amountOutstanding`:STRING
,`analysisCode`:STRING
,`debtType`:STRING
,`dmsDebtType`:STRING
,`recoveryRate`:STRING
,`universalCreditId`:STRING
>>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`responseDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`debtPositionId`:STRING>
,`_version` STRING
,`affordabilityRate` STRING
,`contractId` STRING
,`lowRecoveryPercentage` STRING
,`hasDebt` BOOLEAN
,`_removed` STRUCT<`details`:ARRAY<STRUCT<`amountOutstanding`:STRING
,`analysisCode`:STRING
,`debtType`:STRING
,`dmsDebtType`:STRING
,`recoveryRate`:STRING
,`universalCreditId`:STRING
>>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`responseDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`debtPositionId`:STRING>
,`_version`:STRING
,`affordabilityRate`:STRING
,`contractId`:STRING
,`lowRecoveryPercentage`:STRING
,`hasDebt`:BOOLEAN
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/deductions/debtPosition';

DROP TABLE IF EXISTS debtPosition;

CREATE TABLE debtposition STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(`_removed`.`affordabilityRate`, `affordabilityRate`) as affordabilityRate, 
COALESCE(`_removed`.`contractId`, `contractId`) as contractId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`removed_exploded_details`.`amountOutstanding`, `exploded_details`.`amountOutstanding`) as details_amountOutstanding, 
COALESCE(`removed_exploded_details`.`analysisCode`, `exploded_details`.`analysisCode`) as details_analysisCode, 
COALESCE(`removed_exploded_details`.`debtType`, `exploded_details`.`debtType`) as details_debtType, 
COALESCE(`removed_exploded_details`.`dmsDebtType`, `exploded_details`.`dmsDebtType`) as details_dmsDebtType, 
COALESCE(`removed_exploded_details`.`recoveryRate`, `exploded_details`.`recoveryRate`) as details_recoveryRate, 
COALESCE(`removed_exploded_details`.`universalCreditId`, `exploded_details`.`universalCreditId`) as details_universalCreditId, 
COALESCE(`_removed`.`lowRecoveryPercentage`, `lowRecoveryPercentage`) as lowRecoveryPercentage, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`responseDateTime`.`d_date`, LENGTH(`_removed`.`responseDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`responseDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`responseDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`responseDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`responseDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`responseDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`responseDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`responseDateTime`.`d_date`, LENGTH(`responseDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`responseDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`responseDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`responseDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`responseDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`responseDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`responseDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as responseDateTime_d_date, 
COALESCE(CAST(`_removed`.`hasDebt` as STRING), CAST(`hasDebt` as STRING)) as hasDebt, 
COALESCE(`_removed`.`_id`.`debtPositionId`, `_id`.`debtPositionId`) as id FROM src_deductions_debtposition_debtposition
 LATERAL VIEW OUTER EXPLODE(`details`) view_exploded_details AS exploded_details 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`details`) view_removed_exploded_details AS removed_exploded_details 
;


!echo ------------------------;
!echo ------------------------ housingDeclaration;
!echo ------------------------;
DROP TABLE IF EXISTS src_core_housingdeclaration_housingdeclaration
;

CREATE EXTERNAL TABLE src_core_housingdeclaration_housingdeclaration(
`privateRentedPropertyDeclarationDetails` STRUCT<`jointTenancy`:STRUCT<`rent`:STRUCT<`amount`:STRING
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
,`socialAccommodationDeclarationDetails` STRUCT<`jointTenancy`:STRUCT<`rent`:STRUCT<`amount`:STRING
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
,`ownPropertyDeclarationDetails` STRUCT<`mortgageDetailsList`:ARRAY<STRUCT<`amount`:STRING
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
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`councilTax` STRUCT<`appliedToCouncilTaxReduction`:STRING
,`councilTaxInClaimantName`:BOOLEAN>
,`declaredDateTime` STRUCT<`d_date`:STRING>
,`effectiveDate` STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`noApplicableHousingCostsDeclarationDetails` STRUCT<`noApplicableHousingCostsType`:STRING>
,`otherHousingDeclarationDetails` STRUCT<`furtherDetails`:STRING
,`otherAccommodationType`:STRING>
,`paymentEffectiveDate` STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`_id` STRUCT<`declarationId`:STRING>
,`_version` STRING
,`contractId` STRING
,`declarationId` STRING
,`livingDetails` STRING
,`processId` STRING
,`type` STRING
,`_removed` STRUCT<`privateRentedPropertyDeclarationDetails`:STRUCT<`jointTenancy`:STRUCT<`rent`:STRUCT<`amount`:STRING
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
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`councilTax`:STRUCT<`appliedToCouncilTaxReduction`:STRING
,`councilTaxInClaimantName`:BOOLEAN>
,`declaredDateTime`:STRUCT<`d_date`:STRING>
,`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`noApplicableHousingCostsDeclarationDetails`:STRUCT<`noApplicableHousingCostsType`:STRING>
,`otherHousingDeclarationDetails`:STRUCT<`furtherDetails`:STRING
,`otherAccommodationType`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`_id`:STRUCT<`declarationId`:STRING>
,`_version`:STRING
,`contractId`:STRING
,`declarationId`:STRING
,`livingDetails`:STRING
,`processId`:STRING
,`type`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/core/housingDeclaration';

DROP TABLE IF EXISTS housingDeclaration;

CREATE TABLE housingdeclaration STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(`_removed`.`contractId`, `contractId`) as contractId, 
COALESCE(`_removed`.`councilTax`.`appliedToCouncilTaxReduction`, `councilTax`.`appliedToCouncilTaxReduction`) as councilTax_appliedToCouncilTaxReduction, 
COALESCE(CAST(`_removed`.`councilTax`.`councilTaxInClaimantName` as STRING), CAST(`councilTax`.`councilTaxInClaimantName` as STRING)) as councilTax_councilTaxInClaimantName, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`declaredDateTime`.`d_date`, LENGTH(`_removed`.`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`declaredDateTime`.`d_date`, LENGTH(`declaredDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`declaredDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`declaredDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`declaredDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`declaredDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`declaredDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as declaredDateTime_d_date, 
COALESCE(CAST(`_removed`.`effectiveDate`.`hasDate` as STRING), CAST(`effectiveDate`.`hasDate` as STRING)) as effectiveDate_hasDate, 
COALESCE(`_removed`.`effectiveDate`.`knownDate`, `effectiveDate`.`knownDate`) as effectiveDate_knownDate, 
COALESCE(`_removed`.`effectiveDate`.`type`, `effectiveDate`.`type`) as effectiveDate_type, 
COALESCE(`_removed`.`effectiveDate`.`date`, `effectiveDate`.`date`) as effectiveDate_date, 
COALESCE(`_removed`.`livingDetails`, `livingDetails`) as livingDetails, 
COALESCE(`_removed`.`noApplicableHousingCostsDeclarationDetails`.`noApplicableHousingCostsType`, `noApplicableHousingCostsDeclarationDetails`.`noApplicableHousingCostsType`) as noApplicableHousingCostsDeclarationDetails_noApplicableHousingCostsType, 
COALESCE(`_removed`.`otherHousingDeclarationDetails`.`furtherDetails`, `otherHousingDeclarationDetails`.`furtherDetails`) as otherHousingDeclarationDetails_furtherDetails, 
COALESCE(`_removed`.`otherHousingDeclarationDetails`.`otherAccommodationType`, `otherHousingDeclarationDetails`.`otherAccommodationType`) as otherHousingDeclarationDetails_otherAccommodationType, 
COALESCE(CAST(`_removed`.`ownPropertyDeclarationDetails`.`haveMortgage` as STRING), CAST(`ownPropertyDeclarationDetails`.`haveMortgage` as STRING)) as ownPropertyDeclarationDetails_haveMortgage, 
COALESCE(CAST(`_removed`.`ownPropertyDeclarationDetails`.`payServiceCharges` as STRING), CAST(`ownPropertyDeclarationDetails`.`payServiceCharges` as STRING)) as ownPropertyDeclarationDetails_payServiceCharges, 
COALESCE(`removed_exploded_ownpropertydeclarationdetails_mortgagedetailslist`.`amount`, `exploded_ownpropertydeclarationdetails_mortgagedetailslist`.`amount`) as ownPropertyDeclarationDetails_mortgageDetailsList_amount, 
COALESCE(CAST(`removed_exploded_ownpropertydeclarationdetails_mortgagedetailslist`.`forDisabilityAdaptation` as STRING), CAST(`exploded_ownpropertydeclarationdetails_mortgagedetailslist`.`forDisabilityAdaptation` as STRING)) as ownPropertyDeclarationDetails_mortgageDetailsList_forDisabilityAdaptation, 
COALESCE(`removed_exploded_ownpropertydeclarationdetails_mortgagedetailslist`.`lender`, `exploded_ownpropertydeclarationdetails_mortgagedetailslist`.`lender`) as ownPropertyDeclarationDetails_mortgageDetailsList_lender, 
COALESCE(`removed_exploded_ownpropertydeclarationdetails_mortgagedetailslist`.`lenderCode`, `exploded_ownpropertydeclarationdetails_mortgagedetailslist`.`lenderCode`) as ownPropertyDeclarationDetails_mortgageDetailsList_lenderCode, 
COALESCE(`removed_exploded_ownpropertydeclarationdetails_mortgagedetailslist`.`loanReferenceNumber`, `exploded_ownpropertydeclarationdetails_mortgagedetailslist`.`loanReferenceNumber`) as ownPropertyDeclarationDetails_mortgageDetailsList_loanReferenceNumber, 
COALESCE(`removed_exploded_ownpropertydeclarationdetails_mortgagedetailslist`.`mortgageDetailsId`, `exploded_ownpropertydeclarationdetails_mortgagedetailslist`.`mortgageDetailsId`) as ownPropertyDeclarationDetails_mortgageDetailsList_mortgageDetailsId, 
COALESCE(`_removed`.`ownPropertyDeclarationDetails`.`serviceChargeDetails`.`amount`, `ownPropertyDeclarationDetails`.`serviceChargeDetails`.`amount`) as ownPropertyDeclarationDetails_serviceChargeDetails_amount, 
COALESCE(`_removed`.`ownPropertyDeclarationDetails`.`serviceChargeDetails`.`frequency`, `ownPropertyDeclarationDetails`.`serviceChargeDetails`.`frequency`) as ownPropertyDeclarationDetails_serviceChargeDetails_frequency, 
COALESCE(CAST(`_removed`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`paymentEffectiveDate`.`hasDate` as STRING)) as paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`paymentEffectiveDate`.`knownDate`, `paymentEffectiveDate`.`knownDate`) as paymentEffectiveDate_knownDate, 
COALESCE(`_removed`.`paymentEffectiveDate`.`type`, `paymentEffectiveDate`.`type`) as paymentEffectiveDate_type, 
COALESCE(`_removed`.`paymentEffectiveDate`.`date`, `paymentEffectiveDate`.`date`) as paymentEffectiveDate_date, 
COALESCE(CAST(`_removed`.`privateRentedPropertyDeclarationDetails`.`additionalRentedHousingInformation`.`exemptFromTheSharedRoomRate` as STRING), CAST(`privateRentedPropertyDeclarationDetails`.`additionalRentedHousingInformation`.`exemptFromTheSharedRoomRate` as STRING)) as privateRentedPropertyDeclarationDetails_additionalRentedHousingInformation_exemptFromTheSharedRoomRate, 
COALESCE(CAST(`_removed`.`privateRentedPropertyDeclarationDetails`.`additionalRentedHousingInformation`.`nonCommercialRelationshipWithLandlord` as STRING), CAST(`privateRentedPropertyDeclarationDetails`.`additionalRentedHousingInformation`.`nonCommercialRelationshipWithLandlord` as STRING)) as privateRentedPropertyDeclarationDetails_additionalRentedHousingInformation_nonCommercialRelationshipWithLandlord, 
COALESCE(CAST(`_removed`.`privateRentedPropertyDeclarationDetails`.`additionalRentedHousingInformation`.`supportedExemptAccommodation` as STRING), CAST(`privateRentedPropertyDeclarationDetails`.`additionalRentedHousingInformation`.`supportedExemptAccommodation` as STRING)) as privateRentedPropertyDeclarationDetails_additionalRentedHousingInformation_supportedExemptAccommodation, 
COALESCE(CAST(`_removed`.`privateRentedPropertyDeclarationDetails`.`bedroomAndTenancy`.`bothYouAndPartnerNamedOnTenancyOrRentalAgreement` as STRING), CAST(`privateRentedPropertyDeclarationDetails`.`bedroomAndTenancy`.`bothYouAndPartnerNamedOnTenancyOrRentalAgreement` as STRING)) as privateRentedPropertyDeclarationDetails_bedroomAndTenancy_bothYouAndPartnerNamedOnTenancyOrRentalAgreement, 
COALESCE(CAST(`_removed`.`privateRentedPropertyDeclarationDetails`.`bedroomAndTenancy`.`jointTenancyWithSomeoneOtherThanPartner` as STRING), CAST(`privateRentedPropertyDeclarationDetails`.`bedroomAndTenancy`.`jointTenancyWithSomeoneOtherThanPartner` as STRING)) as privateRentedPropertyDeclarationDetails_bedroomAndTenancy_jointTenancyWithSomeoneOtherThanPartner, 
COALESCE(`_removed`.`privateRentedPropertyDeclarationDetails`.`bedroomAndTenancy`.`numberOfBedrooms`, `privateRentedPropertyDeclarationDetails`.`bedroomAndTenancy`.`numberOfBedrooms`) as privateRentedPropertyDeclarationDetails_bedroomAndTenancy_numberOfBedrooms, 
COALESCE(`_removed`.`privateRentedPropertyDeclarationDetails`.`jointTenancy`.`numberOfTenants`, `privateRentedPropertyDeclarationDetails`.`jointTenancy`.`numberOfTenants`) as privateRentedPropertyDeclarationDetails_jointTenancy_numberOfTenants, 
COALESCE(`_removed`.`privateRentedPropertyDeclarationDetails`.`jointTenancy`.`rent`.`amount`, `privateRentedPropertyDeclarationDetails`.`jointTenancy`.`rent`.`amount`) as privateRentedPropertyDeclarationDetails_jointTenancy_rent_amount, 
COALESCE(`_removed`.`privateRentedPropertyDeclarationDetails`.`jointTenancy`.`rent`.`paymentFrequency`, `privateRentedPropertyDeclarationDetails`.`jointTenancy`.`rent`.`paymentFrequency`) as privateRentedPropertyDeclarationDetails_jointTenancy_rent_paymentFrequency, 
COALESCE(`_removed`.`privateRentedPropertyDeclarationDetails`.`jointTenancy`.`rent`.`type`, `privateRentedPropertyDeclarationDetails`.`jointTenancy`.`rent`.`type`) as privateRentedPropertyDeclarationDetails_jointTenancy_rent_type, 
COALESCE(`_removed`.`privateRentedPropertyDeclarationDetails`.`jointTenancy`.`serviceCharges`.`amount`, `privateRentedPropertyDeclarationDetails`.`jointTenancy`.`serviceCharges`.`amount`) as privateRentedPropertyDeclarationDetails_jointTenancy_serviceCharges_amount, 
COALESCE(`_removed`.`privateRentedPropertyDeclarationDetails`.`jointTenancy`.`serviceCharges`.`paymentFrequency`, `privateRentedPropertyDeclarationDetails`.`jointTenancy`.`serviceCharges`.`paymentFrequency`) as privateRentedPropertyDeclarationDetails_jointTenancy_serviceCharges_paymentFrequency, 
COALESCE(`_removed`.`privateRentedPropertyDeclarationDetails`.`jointTenancy`.`serviceCharges`.`type`, `privateRentedPropertyDeclarationDetails`.`jointTenancy`.`serviceCharges`.`type`) as privateRentedPropertyDeclarationDetails_jointTenancy_serviceCharges_type, 
COALESCE(`_removed`.`privateRentedPropertyDeclarationDetails`.`landlordDetails`.`addressNumber`, `privateRentedPropertyDeclarationDetails`.`landlordDetails`.`addressNumber`) as privateRentedPropertyDeclarationDetails_landlordDetails_addressNumber, 
COALESCE(`_removed`.`privateRentedPropertyDeclarationDetails`.`landlordDetails`.`email`, `privateRentedPropertyDeclarationDetails`.`landlordDetails`.`email`) as privateRentedPropertyDeclarationDetails_landlordDetails_email, 
COALESCE(`_removed`.`privateRentedPropertyDeclarationDetails`.`landlordDetails`.`name`, `privateRentedPropertyDeclarationDetails`.`landlordDetails`.`name`) as privateRentedPropertyDeclarationDetails_landlordDetails_name, 
COALESCE(`_removed`.`privateRentedPropertyDeclarationDetails`.`landlordDetails`.`payRentTo`, `privateRentedPropertyDeclarationDetails`.`landlordDetails`.`payRentTo`) as privateRentedPropertyDeclarationDetails_landlordDetails_payRentTo, 
COALESCE(`_removed`.`privateRentedPropertyDeclarationDetails`.`landlordDetails`.`phoneNumber`, `privateRentedPropertyDeclarationDetails`.`landlordDetails`.`phoneNumber`) as privateRentedPropertyDeclarationDetails_landlordDetails_phoneNumber, 
COALESCE(`_removed`.`privateRentedPropertyDeclarationDetails`.`landlordDetails`.`postcode`, `privateRentedPropertyDeclarationDetails`.`landlordDetails`.`postcode`) as privateRentedPropertyDeclarationDetails_landlordDetails_postcode, 
COALESCE(`_removed`.`privateRentedPropertyDeclarationDetails`.`landlordDetails`.`townCity`, `privateRentedPropertyDeclarationDetails`.`landlordDetails`.`townCity`) as privateRentedPropertyDeclarationDetails_landlordDetails_townCity, 
COALESCE(CAST(`_removed`.`privateRentedPropertyDeclarationDetails`.`landlordRelationshipDetails`.`anyoneCloseRelative` as STRING), CAST(`privateRentedPropertyDeclarationDetails`.`landlordRelationshipDetails`.`anyoneCloseRelative` as STRING)) as privateRentedPropertyDeclarationDetails_landlordRelationshipDetails_anyoneCloseRelative, 
COALESCE(CAST(`_removed`.`privateRentedPropertyDeclarationDetails`.`landlordRelationshipDetails`.`anyoneFinancialInterest` as STRING), CAST(`privateRentedPropertyDeclarationDetails`.`landlordRelationshipDetails`.`anyoneFinancialInterest` as STRING)) as privateRentedPropertyDeclarationDetails_landlordRelationshipDetails_anyoneFinancialInterest, 
COALESCE(CAST(`_removed`.`privateRentedPropertyDeclarationDetails`.`landlordRelationshipDetails`.`liveAtSameProperty` as STRING), CAST(`privateRentedPropertyDeclarationDetails`.`landlordRelationshipDetails`.`liveAtSameProperty` as STRING)) as privateRentedPropertyDeclarationDetails_landlordRelationshipDetails_liveAtSameProperty, 
COALESCE(`_removed`.`privateRentedPropertyDeclarationDetails`.`rentDetails`.`regularPayment`.`amount`, `privateRentedPropertyDeclarationDetails`.`rentDetails`.`regularPayment`.`amount`) as privateRentedPropertyDeclarationDetails_rentDetails_regularPayment_amount, 
COALESCE(`_removed`.`privateRentedPropertyDeclarationDetails`.`rentDetails`.`regularPayment`.`paymentFrequency`, `privateRentedPropertyDeclarationDetails`.`rentDetails`.`regularPayment`.`paymentFrequency`) as privateRentedPropertyDeclarationDetails_rentDetails_regularPayment_paymentFrequency, 
COALESCE(`_removed`.`privateRentedPropertyDeclarationDetails`.`rentDetails`.`regularPayment`.`type`, `privateRentedPropertyDeclarationDetails`.`rentDetails`.`regularPayment`.`type`) as privateRentedPropertyDeclarationDetails_rentDetails_regularPayment_type, 
COALESCE(`_removed`.`privateRentedPropertyDeclarationDetails`.`rentDetails`.`startDate`, `privateRentedPropertyDeclarationDetails`.`rentDetails`.`startDate`) as privateRentedPropertyDeclarationDetails_rentDetails_startDate, 
COALESCE(`_removed`.`processId`, `processId`) as processId, 
COALESCE(CAST(`_removed`.`socialAccommodationDeclarationDetails`.`additionalRentedHousingInformation`.`exemptFromTheSharedRoomRate` as STRING), CAST(`socialAccommodationDeclarationDetails`.`additionalRentedHousingInformation`.`exemptFromTheSharedRoomRate` as STRING)) as socialAccommodationDeclarationDetails_additionalRentedHousingInformation_exemptFromTheSharedRoomRate, 
COALESCE(CAST(`_removed`.`socialAccommodationDeclarationDetails`.`additionalRentedHousingInformation`.`nonCommercialRelationshipWithLandlord` as STRING), CAST(`socialAccommodationDeclarationDetails`.`additionalRentedHousingInformation`.`nonCommercialRelationshipWithLandlord` as STRING)) as socialAccommodationDeclarationDetails_additionalRentedHousingInformation_nonCommercialRelationshipWithLandlord, 
COALESCE(CAST(`_removed`.`socialAccommodationDeclarationDetails`.`additionalRentedHousingInformation`.`supportedExemptAccommodation` as STRING), CAST(`socialAccommodationDeclarationDetails`.`additionalRentedHousingInformation`.`supportedExemptAccommodation` as STRING)) as socialAccommodationDeclarationDetails_additionalRentedHousingInformation_supportedExemptAccommodation, 
COALESCE(CAST(`_removed`.`socialAccommodationDeclarationDetails`.`bedroomAndTenancy`.`bothYouAndPartnerNamedOnTenancyOrRentalAgreement` as STRING), CAST(`socialAccommodationDeclarationDetails`.`bedroomAndTenancy`.`bothYouAndPartnerNamedOnTenancyOrRentalAgreement` as STRING)) as socialAccommodationDeclarationDetails_bedroomAndTenancy_bothYouAndPartnerNamedOnTenancyOrRentalAgreement, 
COALESCE(CAST(`_removed`.`socialAccommodationDeclarationDetails`.`bedroomAndTenancy`.`jointTenancyWithSomeoneOtherThanPartner` as STRING), CAST(`socialAccommodationDeclarationDetails`.`bedroomAndTenancy`.`jointTenancyWithSomeoneOtherThanPartner` as STRING)) as socialAccommodationDeclarationDetails_bedroomAndTenancy_jointTenancyWithSomeoneOtherThanPartner, 
COALESCE(`_removed`.`socialAccommodationDeclarationDetails`.`bedroomAndTenancy`.`numberOfBedrooms`, `socialAccommodationDeclarationDetails`.`bedroomAndTenancy`.`numberOfBedrooms`) as socialAccommodationDeclarationDetails_bedroomAndTenancy_numberOfBedrooms, 
COALESCE(`_removed`.`socialAccommodationDeclarationDetails`.`jointTenancy`.`numberOfTenants`, `socialAccommodationDeclarationDetails`.`jointTenancy`.`numberOfTenants`) as socialAccommodationDeclarationDetails_jointTenancy_numberOfTenants, 
COALESCE(`_removed`.`socialAccommodationDeclarationDetails`.`jointTenancy`.`rent`.`amount`, `socialAccommodationDeclarationDetails`.`jointTenancy`.`rent`.`amount`) as socialAccommodationDeclarationDetails_jointTenancy_rent_amount, 
COALESCE(`_removed`.`socialAccommodationDeclarationDetails`.`jointTenancy`.`rent`.`paymentFrequency`, `socialAccommodationDeclarationDetails`.`jointTenancy`.`rent`.`paymentFrequency`) as socialAccommodationDeclarationDetails_jointTenancy_rent_paymentFrequency, 
COALESCE(`_removed`.`socialAccommodationDeclarationDetails`.`jointTenancy`.`rent`.`type`, `socialAccommodationDeclarationDetails`.`jointTenancy`.`rent`.`type`) as socialAccommodationDeclarationDetails_jointTenancy_rent_type, 
COALESCE(`_removed`.`socialAccommodationDeclarationDetails`.`jointTenancy`.`serviceCharges`.`amount`, `socialAccommodationDeclarationDetails`.`jointTenancy`.`serviceCharges`.`amount`) as socialAccommodationDeclarationDetails_jointTenancy_serviceCharges_amount, 
COALESCE(`_removed`.`socialAccommodationDeclarationDetails`.`jointTenancy`.`serviceCharges`.`paymentFrequency`, `socialAccommodationDeclarationDetails`.`jointTenancy`.`serviceCharges`.`paymentFrequency`) as socialAccommodationDeclarationDetails_jointTenancy_serviceCharges_paymentFrequency, 
COALESCE(`_removed`.`socialAccommodationDeclarationDetails`.`jointTenancy`.`serviceCharges`.`type`, `socialAccommodationDeclarationDetails`.`jointTenancy`.`serviceCharges`.`type`) as socialAccommodationDeclarationDetails_jointTenancy_serviceCharges_type, 
COALESCE(`_removed`.`socialAccommodationDeclarationDetails`.`landlordDetails`.`addressNumber`, `socialAccommodationDeclarationDetails`.`landlordDetails`.`addressNumber`) as socialAccommodationDeclarationDetails_landlordDetails_addressNumber, 
COALESCE(`_removed`.`socialAccommodationDeclarationDetails`.`landlordDetails`.`email`, `socialAccommodationDeclarationDetails`.`landlordDetails`.`email`) as socialAccommodationDeclarationDetails_landlordDetails_email, 
COALESCE(`_removed`.`socialAccommodationDeclarationDetails`.`landlordDetails`.`name`, `socialAccommodationDeclarationDetails`.`landlordDetails`.`name`) as socialAccommodationDeclarationDetails_landlordDetails_name, 
COALESCE(`_removed`.`socialAccommodationDeclarationDetails`.`landlordDetails`.`payRentTo`, `socialAccommodationDeclarationDetails`.`landlordDetails`.`payRentTo`) as socialAccommodationDeclarationDetails_landlordDetails_payRentTo, 
COALESCE(`_removed`.`socialAccommodationDeclarationDetails`.`landlordDetails`.`phoneNumber`, `socialAccommodationDeclarationDetails`.`landlordDetails`.`phoneNumber`) as socialAccommodationDeclarationDetails_landlordDetails_phoneNumber, 
COALESCE(`_removed`.`socialAccommodationDeclarationDetails`.`landlordDetails`.`postcode`, `socialAccommodationDeclarationDetails`.`landlordDetails`.`postcode`) as socialAccommodationDeclarationDetails_landlordDetails_postcode, 
COALESCE(`_removed`.`socialAccommodationDeclarationDetails`.`landlordDetails`.`townCity`, `socialAccommodationDeclarationDetails`.`landlordDetails`.`townCity`) as socialAccommodationDeclarationDetails_landlordDetails_townCity, 
COALESCE(`_removed`.`socialAccommodationDeclarationDetails`.`rentDetails`.`regularPayment`.`amount`, `socialAccommodationDeclarationDetails`.`rentDetails`.`regularPayment`.`amount`) as socialAccommodationDeclarationDetails_rentDetails_regularPayment_amount, 
COALESCE(`_removed`.`socialAccommodationDeclarationDetails`.`rentDetails`.`regularPayment`.`paymentFrequency`, `socialAccommodationDeclarationDetails`.`rentDetails`.`regularPayment`.`paymentFrequency`) as socialAccommodationDeclarationDetails_rentDetails_regularPayment_paymentFrequency, 
COALESCE(`_removed`.`socialAccommodationDeclarationDetails`.`rentDetails`.`regularPayment`.`type`, `socialAccommodationDeclarationDetails`.`rentDetails`.`regularPayment`.`type`) as socialAccommodationDeclarationDetails_rentDetails_regularPayment_type, 
COALESCE(`_removed`.`socialAccommodationDeclarationDetails`.`rentDetails`.`startDate`, `socialAccommodationDeclarationDetails`.`rentDetails`.`startDate`) as socialAccommodationDeclarationDetails_rentDetails_startDate, 
COALESCE(`_removed`.`socialAccommodationDeclarationDetails`.`socialHousing`.`rentFreeWeeks`, `socialAccommodationDeclarationDetails`.`socialHousing`.`rentFreeWeeks`) as socialAccommodationDeclarationDetails_socialHousing_rentFreeWeeks, 
COALESCE(`_removed`.`socialAccommodationDeclarationDetails`.`socialHousing`.`serviceCharges`.`amount`, `socialAccommodationDeclarationDetails`.`socialHousing`.`serviceCharges`.`amount`) as socialAccommodationDeclarationDetails_socialHousing_serviceCharges_amount, 
COALESCE(`_removed`.`socialAccommodationDeclarationDetails`.`socialHousing`.`serviceCharges`.`paymentFrequency`, `socialAccommodationDeclarationDetails`.`socialHousing`.`serviceCharges`.`paymentFrequency`) as socialAccommodationDeclarationDetails_socialHousing_serviceCharges_paymentFrequency, 
COALESCE(`_removed`.`socialAccommodationDeclarationDetails`.`socialHousing`.`serviceCharges`.`type`, `socialAccommodationDeclarationDetails`.`socialHousing`.`serviceCharges`.`type`) as socialAccommodationDeclarationDetails_socialHousing_serviceCharges_type, 
COALESCE(`_removed`.`type`, `type`) as type, 
COALESCE(`_removed`.`_id`.`declarationId`, `_id`.`declarationId`) as id FROM src_core_housingdeclaration_housingdeclaration
 LATERAL VIEW OUTER EXPLODE(`ownPropertyDeclarationDetails`.`mortgageDetailsList`) view_exploded_ownpropertydeclarationdetails_mortgagedetailslist AS exploded_ownpropertydeclarationdetails_mortgagedetailslist 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`ownPropertyDeclarationDetails`.`mortgageDetailsList`) view_removed_exploded_ownpropertydeclarationdetails_mortgagedetailslist AS removed_exploded_ownpropertydeclarationdetails_mortgagedetailslist 
;


!echo ------------------------;
!echo ------------------------ personalDetails;
!echo ------------------------;
DROP TABLE IF EXISTS src_core_personaldetails_personaldetails
;

CREATE EXTERNAL TABLE src_core_personaldetails_personaldetails(
`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`effectiveDate` STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate` STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`_id` STRUCT<`declarationId`:STRING>
,`_version` STRING
,`citizenId` STRING
,`contactPreference` STRING
,`contractId` STRING
,`declarationId` STRING
,`email` STRING
,`firstName` STRING
,`gender` STRING
,`lastName` STRING
,`middleName` STRING
,`mobileNumber` STRING
,`processId` STRING
,`sanitisedFirstName` STRING
,`sanitisedLastName` STRING
,`sanitisedMiddleName` STRING
,`verifiedUsingBioQuestionsOrThirdParty` BOOLEAN
,`verifiedWithNameDateOfBirthOrAddressChange` BOOLEAN
,`type` STRING
,`_removed` STRUCT<`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`effectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`paymentEffectiveDate`:STRUCT<`hasDate`:BOOLEAN
,`knownDate`:STRING
,`type`:STRING
,`date`:STRING>
,`_id`:STRUCT<`declarationId`:STRING>
,`_version`:STRING
,`citizenId`:STRING
,`contactPreference`:STRING
,`contractId`:STRING
,`declarationId`:STRING
,`email`:STRING
,`firstName`:STRING
,`gender`:STRING
,`lastName`:STRING
,`middleName`:STRING
,`mobileNumber`:STRING
,`processId`:STRING
,`sanitisedFirstName`:STRING
,`sanitisedLastName`:STRING
,`sanitisedMiddleName`:STRING
,`verifiedUsingBioQuestionsOrThirdParty`:BOOLEAN
,`verifiedWithNameDateOfBirthOrAddressChange`:BOOLEAN
,`type`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/core/personalDetails';

DROP TABLE IF EXISTS personalDetails;

CREATE TABLE personaldetails STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(`_removed`.`citizenId`, `citizenId`) as citizenId, 
COALESCE(`_removed`.`contactPreference`, `contactPreference`) as contactPreference, 
COALESCE(`_removed`.`contractId`, `contractId`) as contractId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`declarationId`, `declarationId`) as declarationId, 
COALESCE(CAST(`_removed`.`effectiveDate`.`hasDate` as STRING), CAST(`effectiveDate`.`hasDate` as STRING)) as effectiveDate_hasDate, 
COALESCE(`_removed`.`effectiveDate`.`knownDate`, `effectiveDate`.`knownDate`) as effectiveDate_knownDate, 
COALESCE(`_removed`.`effectiveDate`.`type`, `effectiveDate`.`type`) as effectiveDate_type, 
COALESCE(`_removed`.`effectiveDate`.`date`, `effectiveDate`.`date`) as effectiveDate_date, 
COALESCE(`_removed`.`email`, `email`) as email, 
COALESCE(`_removed`.`firstName`, `firstName`) as firstName, 
COALESCE(`_removed`.`gender`, `gender`) as gender, 
COALESCE(`_removed`.`lastName`, `lastName`) as lastName, 
COALESCE(`_removed`.`middleName`, `middleName`) as middleName, 
COALESCE(`_removed`.`mobileNumber`, `mobileNumber`) as mobileNumber, 
COALESCE(CAST(`_removed`.`paymentEffectiveDate`.`hasDate` as STRING), CAST(`paymentEffectiveDate`.`hasDate` as STRING)) as paymentEffectiveDate_hasDate, 
COALESCE(`_removed`.`paymentEffectiveDate`.`knownDate`, `paymentEffectiveDate`.`knownDate`) as paymentEffectiveDate_knownDate, 
COALESCE(`_removed`.`paymentEffectiveDate`.`type`, `paymentEffectiveDate`.`type`) as paymentEffectiveDate_type, 
COALESCE(`_removed`.`paymentEffectiveDate`.`date`, `paymentEffectiveDate`.`date`) as paymentEffectiveDate_date, 
COALESCE(`_removed`.`processId`, `processId`) as processId, 
COALESCE(`_removed`.`sanitisedFirstName`, `sanitisedFirstName`) as sanitisedFirstName, 
COALESCE(`_removed`.`sanitisedLastName`, `sanitisedLastName`) as sanitisedLastName, 
COALESCE(`_removed`.`sanitisedMiddleName`, `sanitisedMiddleName`) as sanitisedMiddleName, 
COALESCE(CAST(`_removed`.`verifiedUsingBioQuestionsOrThirdParty` as STRING), CAST(`verifiedUsingBioQuestionsOrThirdParty` as STRING)) as verifiedUsingBioQuestionsOrThirdParty, 
COALESCE(CAST(`_removed`.`verifiedWithNameDateOfBirthOrAddressChange` as STRING), CAST(`verifiedWithNameDateOfBirthOrAddressChange` as STRING)) as verifiedWithNameDateOfBirthOrAddressChange, 
COALESCE(`_removed`.`type`, `type`) as type, 
COALESCE(`_removed`.`_id`.`declarationId`, `_id`.`declarationId`) as id FROM src_core_personaldetails_personaldetails
 src_core_personaldetails_personaldetails;


!echo ------------------------;
!echo ------------------------ survey;
!echo ------------------------;
DROP TABLE IF EXISTS src_core_survey_survey
;

CREATE EXTERNAL TABLE src_core_survey_survey(
`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`d_oid`:STRING>
,`_version` STRING
,`claimantId` STRING
,`contractId` STRING
,`skipped` BOOLEAN
,`_removed` STRUCT<`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`d_oid`:STRING>
,`_version`:STRING
,`claimantId`:STRING
,`contractId`:STRING
,`skipped`:BOOLEAN
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/core/survey';

DROP TABLE IF EXISTS survey;

CREATE TABLE survey STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(`_removed`.`claimantId`, `claimantId`) as claimantId, 
COALESCE(`_removed`.`contractId`, `contractId`) as contractId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(CAST(`_removed`.`skipped` as STRING), CAST(`skipped` as STRING)) as skipped, 
COALESCE(`_removed`.`_id`.`d_oid`, `_id`.`d_oid`) as id FROM src_core_survey_survey
 src_core_survey_survey;


!echo ------------------------;
!echo ------------------------ sanctionProgress;
!echo ------------------------;
DROP TABLE IF EXISTS src_penalties_and_deductions_sanctionprogress_sanctionprogress
;

CREATE EXTERNAL TABLE src_penalties_and_deductions_sanctionprogress_sanctionprogress(
`sanctionProgressEvents` ARRAY<STRUCT<`trace`:STRUCT<`elements`:ARRAY<STRUCT<`code`:STRING
,`description`:STRING
,`elementType`:STRING
>>>
,`value`:STRUCT<`assessmentPeriodId`:STRING
,`deductedFixedElement`:STRING
,`deductedOpenEndedElement`:STRING
,`effectiveDate`:STRING
,`eventDate`:STRING
,`openEndedAdditionToTorp`:STRING
,`remainingFixedDuration`:STRING
,`remainingOpenEndedDuration`:STRING
,`sanctionId`:STRING
,`usedOutOfClaimDaysFixed`:STRING
,`usedOutOfClaimDaysOpenEnded`:STRING
,`daysDeducted`:STRING
,`daysRemaining`:STRING>
>>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`sanctionId`:STRING>
,`_version` STRING
,`dateOfFailure` STRING
,`decisionDate` STRING
,`personId` STRING
,`sanctionId` STRING
,`_removed` STRUCT<`sanctionProgressEvents`:ARRAY<STRUCT<`trace`:STRUCT<`elements`:ARRAY<STRUCT<`code`:STRING
,`description`:STRING
,`elementType`:STRING
>>>
,`value`:STRUCT<`assessmentPeriodId`:STRING
,`deductedFixedElement`:STRING
,`deductedOpenEndedElement`:STRING
,`effectiveDate`:STRING
,`eventDate`:STRING
,`openEndedAdditionToTorp`:STRING
,`remainingFixedDuration`:STRING
,`remainingOpenEndedDuration`:STRING
,`sanctionId`:STRING
,`usedOutOfClaimDaysFixed`:STRING
,`usedOutOfClaimDaysOpenEnded`:STRING
,`daysDeducted`:STRING
,`daysRemaining`:STRING>
>>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`sanctionId`:STRING>
,`_version`:STRING
,`dateOfFailure`:STRING
,`decisionDate`:STRING
,`personId`:STRING
,`sanctionId`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/penalties-and-deductions/sanctionProgress';

DROP TABLE IF EXISTS sanctionProgress;

CREATE TABLE sanctionprogress STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`dateOfFailure`, `dateOfFailure`) as dateOfFailure, 
COALESCE(`_removed`.`decisionDate`, `decisionDate`) as decisionDate, 
COALESCE(`_removed`.`personId`, `personId`) as personId, 
COALESCE(`_removed`.`sanctionId`, `sanctionId`) as sanctionId, 
COALESCE(`removed_exploded_sanctionprogressevents_exploded_trace_elements`.`code`, `exploded_sanctionprogressevents_exploded_trace_elements`.`code`) as sanctionProgressEvents_trace_elements_code, 
COALESCE(`removed_exploded_sanctionprogressevents_exploded_trace_elements`.`description`, `exploded_sanctionprogressevents_exploded_trace_elements`.`description`) as sanctionProgressEvents_trace_elements_description, 
COALESCE(`removed_exploded_sanctionprogressevents_exploded_trace_elements`.`elementType`, `exploded_sanctionprogressevents_exploded_trace_elements`.`elementType`) as sanctionProgressEvents_trace_elements_elementType, 
COALESCE(`removed_exploded_sanctionprogressevents`.`value`.`assessmentPeriodId`, `exploded_sanctionprogressevents`.`value`.`assessmentPeriodId`) as sanctionProgressEvents_value_assessmentPeriodId, 
COALESCE(`removed_exploded_sanctionprogressevents`.`value`.`deductedFixedElement`, `exploded_sanctionprogressevents`.`value`.`deductedFixedElement`) as sanctionProgressEvents_value_deductedFixedElement, 
COALESCE(`removed_exploded_sanctionprogressevents`.`value`.`deductedOpenEndedElement`, `exploded_sanctionprogressevents`.`value`.`deductedOpenEndedElement`) as sanctionProgressEvents_value_deductedOpenEndedElement, 
COALESCE(`removed_exploded_sanctionprogressevents`.`value`.`effectiveDate`, `exploded_sanctionprogressevents`.`value`.`effectiveDate`) as sanctionProgressEvents_value_effectiveDate, 
COALESCE(`removed_exploded_sanctionprogressevents`.`value`.`eventDate`, `exploded_sanctionprogressevents`.`value`.`eventDate`) as sanctionProgressEvents_value_eventDate, 
COALESCE(`removed_exploded_sanctionprogressevents`.`value`.`openEndedAdditionToTorp`, `exploded_sanctionprogressevents`.`value`.`openEndedAdditionToTorp`) as sanctionProgressEvents_value_openEndedAdditionToTorp, 
COALESCE(`removed_exploded_sanctionprogressevents`.`value`.`remainingFixedDuration`, `exploded_sanctionprogressevents`.`value`.`remainingFixedDuration`) as sanctionProgressEvents_value_remainingFixedDuration, 
COALESCE(`removed_exploded_sanctionprogressevents`.`value`.`remainingOpenEndedDuration`, `exploded_sanctionprogressevents`.`value`.`remainingOpenEndedDuration`) as sanctionProgressEvents_value_remainingOpenEndedDuration, 
COALESCE(`removed_exploded_sanctionprogressevents`.`value`.`sanctionId`, `exploded_sanctionprogressevents`.`value`.`sanctionId`) as sanctionProgressEvents_value_sanctionId, 
COALESCE(`removed_exploded_sanctionprogressevents`.`value`.`usedOutOfClaimDaysFixed`, `exploded_sanctionprogressevents`.`value`.`usedOutOfClaimDaysFixed`) as sanctionProgressEvents_value_usedOutOfClaimDaysFixed, 
COALESCE(`removed_exploded_sanctionprogressevents`.`value`.`usedOutOfClaimDaysOpenEnded`, `exploded_sanctionprogressevents`.`value`.`usedOutOfClaimDaysOpenEnded`) as sanctionProgressEvents_value_usedOutOfClaimDaysOpenEnded, 
COALESCE(`removed_exploded_sanctionprogressevents`.`value`.`daysDeducted`, `exploded_sanctionprogressevents`.`value`.`daysDeducted`) as sanctionProgressEvents_value_daysDeducted, 
COALESCE(`removed_exploded_sanctionprogressevents`.`value`.`daysRemaining`, `exploded_sanctionprogressevents`.`value`.`daysRemaining`) as sanctionProgressEvents_value_daysRemaining, 
COALESCE(`_removed`.`_id`.`sanctionId`, `_id`.`sanctionId`) as id FROM src_penalties_and_deductions_sanctionprogress_sanctionprogress
 LATERAL VIEW OUTER EXPLODE(`sanctionProgressEvents`) view_exploded_sanctionprogressevents AS exploded_sanctionprogressevents 
 LATERAL VIEW OUTER EXPLODE(`exploded_sanctionprogressevents`.`trace`.`elements`) view_exploded_sanctionprogressevents_exploded_trace_elements AS exploded_sanctionprogressevents_exploded_trace_elements 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`sanctionProgressEvents`) view_removed_exploded_sanctionprogressevents AS removed_exploded_sanctionprogressevents 
 LATERAL VIEW OUTER EXPLODE(`removed_exploded_sanctionprogressevents`.`trace`.`elements`) view_removed_exploded_sanctionprogressevents_exploded_trace_elements AS removed_exploded_sanctionprogressevents_exploded_trace_elements 
;


!echo ------------------------;
!echo ------------------------ sanctionTerminationDecision;
!echo ------------------------;
DROP TABLE IF EXISTS src_penalties_and_deductions_sanctionterminationdecision_sanctionterminationdecision
;

CREATE EXTERNAL TABLE src_penalties_and_deductions_sanctionterminationdecision_sanctionterminationdecision(
`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`sanctionTerminationDecisionId`:STRING>
,`_version` STRING
,`applicationDate` STRING
,`personId` STRING
,`sanctionTerminationDecisionId` STRING
,`status` STRING
,`_removed` STRUCT<`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`sanctionTerminationDecisionId`:STRING>
,`_version`:STRING
,`applicationDate`:STRING
,`personId`:STRING
,`sanctionTerminationDecisionId`:STRING
,`status`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/penalties-and-deductions/sanctionTerminationDecision';

DROP TABLE IF EXISTS sanctionTerminationDecision;

CREATE TABLE sanctionterminationdecision STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(`_removed`.`applicationDate`, `applicationDate`) as applicationDate, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`personId`, `personId`) as personId, 
COALESCE(`_removed`.`sanctionTerminationDecisionId`, `sanctionTerminationDecisionId`) as sanctionTerminationDecisionId, 
COALESCE(`_removed`.`status`, `status`) as status, 
COALESCE(`_removed`.`_id`.`sanctionTerminationDecisionId`, `_id`.`sanctionTerminationDecisionId`) as id FROM src_penalties_and_deductions_sanctionterminationdecision_sanctionterminationdecision
 src_penalties_and_deductions_sanctionterminationdecision_sanctionterminationdecision;


!echo ------------------------;
!echo ------------------------ workGroupAllocation;
!echo ------------------------;
DROP TABLE IF EXISTS src_core_workgroupallocation_workgroupallocation
;

CREATE EXTERNAL TABLE src_core_workgroupallocation_workgroupallocation(
`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`d_oid`:STRING>
,`_version` STRING
,`citizenId` STRING
,`contractId` STRING
,`currentWorkGroup` STRING
,`expectedWorkingHours` STRING
,`workGroupAllocationId` STRING
,`_removed` STRUCT<`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`d_oid`:STRING>
,`_version`:STRING
,`citizenId`:STRING
,`contractId`:STRING
,`currentWorkGroup`:STRING
,`expectedWorkingHours`:STRING
,`workGroupAllocationId`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/core/workGroupAllocation';

DROP TABLE IF EXISTS workGroupAllocation;

CREATE TABLE workgroupallocation STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(`_removed`.`citizenId`, `citizenId`) as citizenId, 
COALESCE(`_removed`.`contractId`, `contractId`) as contractId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`currentWorkGroup`, `currentWorkGroup`) as currentWorkGroup, 
COALESCE(`_removed`.`expectedWorkingHours`, `expectedWorkingHours`) as expectedWorkingHours, 
COALESCE(`_removed`.`workGroupAllocationId`, `workGroupAllocationId`) as workGroupAllocationId, 
COALESCE(`_removed`.`_id`.`d_oid`, `_id`.`d_oid`) as id FROM src_core_workgroupallocation_workgroupallocation
 src_core_workgroupallocation_workgroupallocation;


!echo ------------------------;
!echo ------------------------ outOfClaimDays;
!echo ------------------------;
DROP TABLE IF EXISTS src_penalties_and_deductions_outofclaimdays_outofclaimdays
;

CREATE EXTERNAL TABLE src_penalties_and_deductions_outofclaimdays_outofclaimdays(
`usedOutOfClaimDays` ARRAY<STRUCT<`trace`:STRUCT<`elements`:ARRAY<STRUCT<`code`:STRING
,`description`:STRING
,`elementType`:STRING
>>>
,`value`:STRUCT<`usedPeriods`:ARRAY<STRUCT<`endDate`:STRING
,`idString`:STRING
,`startDate`:STRING
,`typeName`:STRING
,`fraudPenaltyId`:STRING
,`type`:STRING
,`sanctionId`:STRING
>>
,`effectiveDate`:STRING>
>>
,`_removedDateTime` STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime` STRUCT<`d_date`:STRING>
,`createdDateTime` STRUCT<`d_date`:STRING>
,`updateDateTime` STRUCT<`d_date`:STRING>
,`_id` STRUCT<`personId`:STRING>
,`_version` STRING
,`personId` STRING
,`_removed` STRUCT<`usedOutOfClaimDays`:ARRAY<STRUCT<`trace`:STRUCT<`elements`:ARRAY<STRUCT<`code`:STRING
,`description`:STRING
,`elementType`:STRING
>>>
,`value`:STRUCT<`usedPeriods`:ARRAY<STRUCT<`endDate`:STRING
,`idString`:STRING
,`startDate`:STRING
,`typeName`:STRING
,`fraudPenaltyId`:STRING
,`type`:STRING
,`sanctionId`:STRING
>>
,`effectiveDate`:STRING>
>>
,`_removedDateTime`:STRUCT<`d_date`:STRING>
,`_lastModifiedDateTime`:STRUCT<`d_date`:STRING>
,`createdDateTime`:STRUCT<`d_date`:STRING>
,`updateDateTime`:STRUCT<`d_date`:STRING>
,`_id`:STRUCT<`personId`:STRING>
,`_version`:STRING
,`personId`:STRING
>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/etl/uc/mongo/${hiveconf:BATCH_DATE}/penalties-and-deductions/outOfClaimDays';

DROP TABLE IF EXISTS outOfClaimDays;

CREATE TABLE outofclaimdays STORED AS ORC AS SELECT 
 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_removedDateTime`.`d_date`, LENGTH(`_removed`.`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_removedDateTime`.`d_date`, LENGTH(`_removedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as removed_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`_lastModifiedDateTime`.`d_date`, LENGTH(`_removed`.`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`_lastModifiedDateTime`.`d_date`, LENGTH(`_lastModifiedDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_lastModifiedDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_lastModifiedDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_lastModifiedDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_lastModifiedDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as last_modified_ts, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as created_ts, 
COALESCE(CAST(`_removed`.`_version` as INT), CAST(`_version` as INT)) as version, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`createdDateTime`.`d_date`, LENGTH(`_removed`.`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`createdDateTime`.`d_date`, LENGTH(`createdDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`createdDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`createdDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`createdDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`createdDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as createdDateTime_d_date, 
COALESCE(`_removed`.`personId`, `personId`) as personId, 
COALESCE(`removed_exploded_usedoutofclaimdays_exploded_trace_elements`.`code`, `exploded_usedoutofclaimdays_exploded_trace_elements`.`code`) as usedOutOfClaimDays_trace_elements_code, 
COALESCE(`removed_exploded_usedoutofclaimdays_exploded_trace_elements`.`description`, `exploded_usedoutofclaimdays_exploded_trace_elements`.`description`) as usedOutOfClaimDays_trace_elements_description, 
COALESCE(`removed_exploded_usedoutofclaimdays_exploded_trace_elements`.`elementType`, `exploded_usedoutofclaimdays_exploded_trace_elements`.`elementType`) as usedOutOfClaimDays_trace_elements_elementType, 
COALESCE(`removed_exploded_usedoutofclaimdays`.`value`.`effectiveDate`, `exploded_usedoutofclaimdays`.`value`.`effectiveDate`) as usedOutOfClaimDays_value_effectiveDate, 
COALESCE(`removed_exploded_usedoutofclaimdays_exploded_value_usedperiods`.`endDate`, `exploded_usedoutofclaimdays_exploded_value_usedperiods`.`endDate`) as usedOutOfClaimDays_value_usedPeriods_endDate, 
COALESCE(`removed_exploded_usedoutofclaimdays_exploded_value_usedperiods`.`idString`, `exploded_usedoutofclaimdays_exploded_value_usedperiods`.`idString`) as usedOutOfClaimDays_value_usedPeriods_idString, 
COALESCE(`removed_exploded_usedoutofclaimdays_exploded_value_usedperiods`.`startDate`, `exploded_usedoutofclaimdays_exploded_value_usedperiods`.`startDate`) as usedOutOfClaimDays_value_usedPeriods_startDate, 
COALESCE(`removed_exploded_usedoutofclaimdays_exploded_value_usedperiods`.`typeName`, `exploded_usedoutofclaimdays_exploded_value_usedperiods`.`typeName`) as usedOutOfClaimDays_value_usedPeriods_typeName, 
COALESCE(`removed_exploded_usedoutofclaimdays_exploded_value_usedperiods`.`fraudPenaltyId`, `exploded_usedoutofclaimdays_exploded_value_usedperiods`.`fraudPenaltyId`) as usedOutOfClaimDays_value_usedPeriods_fraudPenaltyId, 
COALESCE(`removed_exploded_usedoutofclaimdays_exploded_value_usedperiods`.`type`, `exploded_usedoutofclaimdays_exploded_value_usedperiods`.`type`) as usedOutOfClaimDays_value_usedPeriods_type, 
COALESCE(`removed_exploded_usedoutofclaimdays_exploded_value_usedperiods`.`sanctionId`, `exploded_usedoutofclaimdays_exploded_value_usedperiods`.`sanctionId`) as usedOutOfClaimDays_value_usedPeriods_sanctionId, 
COALESCE(CASE WHEN SUBSTRING(`_removed`.`updateDateTime`.`d_date`, LENGTH(`_removed`.`updateDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`_removed`.`updateDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`updateDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`_removed`.`updateDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`_removed`.`updateDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`_removed`.`updateDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`_removed`.`updateDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END, CASE WHEN SUBSTRING(`updateDateTime`.`d_date`, LENGTH(`updateDateTime`.`d_date`), 1) = 'Z' THEN CAST(CONCAT(SUBSTR(`updateDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`updateDateTime`.`d_date`, 12, 12)) AS TIMESTAMP) ELSE CAST(CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(CONCAT(SUBSTR(`updateDateTime`.`d_date`, 1, 10), ' ', SUBSTR(`updateDateTime`.`d_date`, 12, 8)) AS TIMESTAMP)) - (CAST(SUBSTR(`updateDateTime`.`d_date`, 25, 2) AS BIGINT) * 3600),'yyyy-MM-dd HH:mm:ss'),'.', SUBSTR(`updateDateTime`.`d_date`, 21, 3)) AS TIMESTAMP) END) as updateDateTime_d_date, 
COALESCE(`_removed`.`_id`.`personId`, `_id`.`personId`) as id FROM src_penalties_and_deductions_outofclaimdays_outofclaimdays
 LATERAL VIEW OUTER EXPLODE(`usedOutOfClaimDays`) view_exploded_usedoutofclaimdays AS exploded_usedoutofclaimdays 
 LATERAL VIEW OUTER EXPLODE(`exploded_usedoutofclaimdays`.`trace`.`elements`) view_exploded_usedoutofclaimdays_exploded_trace_elements AS exploded_usedoutofclaimdays_exploded_trace_elements 
 LATERAL VIEW OUTER EXPLODE(`_removed`.`usedOutOfClaimDays`) view_removed_exploded_usedoutofclaimdays AS removed_exploded_usedoutofclaimdays 
 LATERAL VIEW OUTER EXPLODE(`removed_exploded_usedoutofclaimdays`.`trace`.`elements`) view_removed_exploded_usedoutofclaimdays_exploded_trace_elements AS removed_exploded_usedoutofclaimdays_exploded_trace_elements 
 LATERAL VIEW OUTER EXPLODE(`exploded_usedoutofclaimdays`.`value`.`usedPeriods`) view_exploded_usedoutofclaimdays_exploded_value_usedperiods AS exploded_usedoutofclaimdays_exploded_value_usedperiods 
 LATERAL VIEW OUTER EXPLODE(`removed_exploded_usedoutofclaimdays`.`value`.`usedPeriods`) view_removed_exploded_usedoutofclaimdays_exploded_value_usedperiods AS removed_exploded_usedoutofclaimdays_exploded_value_usedperiods 
;


