!echo ------------------------;
!echo ------------------------ interactions source;
!echo ------------------------;
DROP TABLE IF EXISTS src_analytics_interactions_interactions
;

CREATE EXTERNAL TABLE src_analytics_interactions_interactions(
`Pages` ARRAY<STRUCT<`Item`:STRUCT<`_id`:STRUCT<`binary`:STRING
,`type`:STRING>
,`Language`:STRING
,`Version`:STRING>
,`SitecoreDevice`:STRUCT<`_id`:STRUCT<`binary`:STRING
,`type`:STRING>
,`Name`:STRING>
,`DateTime`:STRUCT<`date`:STRING>
,`MvTest`:STRUCT<`ValueAtExposure`:STRING>
,`Url`:STRUCT<`Path`:STRING>
,`Duration`:STRING
,`VisitPageIndex`:STRING
>>
,`Profiles` STRUCT<`Legal Persona`:STRUCT<`Values`:STRUCT<`b2b`:STRING
,`selfemployed`:STRING
,`legal trade`:STRING
,`personalcivil`:STRING>
,`PatternId`:STRUCT<`binary`:STRING
,`type`:STRING>
,`Count`:STRING
,`Total`:STRING
,`ProfileName`:STRING
,`PatternLabel`:STRING>>
,`ContactId` STRUCT<`binary`:STRING
,`type`:STRING>
,`StartDateTime` STRUCT<`date`:STRING>
,`EndDateTime` STRUCT<`date`:STRING>
,`SaveDateTime` STRUCT<`date`:STRING>
,`ChannelId` STRUCT<`binary`:STRING
,`type`:STRING>
,`Browser` STRUCT<`BrowserVersion`:STRING
,`BrowserMajorName`:STRING
,`BrowserMinorName`:STRING>
,`Screen` STRUCT<`ScreenHeight`:STRING
,`ScreenWidth`:STRING>
,`DeviceId` STRUCT<`binary`:STRING
,`type`:STRING>
,`GeoData` STRUCT<`AreaCode`:STRING
,`BusinessName`:STRING
,`City`:STRING
,`Country`:STRING
,`Dns`:STRING
,`Isp`:STRING
,`MetroCode`:STRING
,`PostalCode`:STRING
,`Region`:STRING
,`Url`:STRING>
,`Ip` STRUCT<`binary`:STRING
,`type`:STRING>
,`LocationId` STRUCT<`binary`:STRING
,`type`:STRING>
,`OperatingSystem` STRUCT<`_id`:STRING>
,`_t` STRING
,`ContactVisitIndex` STRING
,`Language` STRING
,`Referrer` STRING
,`ReferringSite` STRING
,`SiteName` STRING
,`TrafficType` STRING
,`UserAgent` STRING
,`Value` STRING
,`VisitPageCount` STRING
,`_id` STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION 's3://villani/raw/interactions/';

DROP TABLE IF EXISTS interactions;

!echo ------------------------;
!echo ------------------------ interactions transform;
!echo ------------------------;

CREATE TABLE interactions STORED AS ORC AS SELECT
`_t` as `_t`,
`ContactId`.`binary` as `contactid_binary`,
`ContactId`.`type` as `contactid_type`,
`StartDateTime`.`date` as `startdatetime_date`,
`EndDateTime`.`date` as `enddatetime_date`,
`SaveDateTime`.`date` as `savedatetime_date`,
`ChannelId`.`binary` as `channelid_binary`,
`ChannelId`.`type` as `channelid_type`,
`Browser`.`BrowserVersion` as `browser_browserversion`,
`Browser`.`BrowserMajorName` as `browser_browsermajorname`,
`Browser`.`BrowserMinorName` as `browser_browserminorname`,
`Screen`.`ScreenHeight` as `screen_screenheight`,
`Screen`.`ScreenWidth` as `screen_screenwidth`,
`ContactVisitIndex` as `contactvisitindex`,
`DeviceId`.`binary` as `deviceid_binary`,
`DeviceId`.`type` as `deviceid_type`,
`GeoData`.`AreaCode` as `geodata_areacode`,
`GeoData`.`BusinessName` as `geodata_businessname`,
`GeoData`.`City` as `geodata_city`,
`GeoData`.`Country` as `geodata_country`,
`GeoData`.`Dns` as `geodata_dns`,
`GeoData`.`Isp` as `geodata_isp`,
`GeoData`.`MetroCode` as `geodata_metrocode`,
`GeoData`.`PostalCode` as `geodata_postalcode`,
`GeoData`.`Region` as `geodata_region`,
`GeoData`.`Url` as `geodata_url`,
`Ip`.`binary` as `ip_binary`,
`Ip`.`type` as `ip_type`,
`Language` as `language`,
`LocationId`.`binary` as `locationid_binary`,
`LocationId`.`type` as `locationid_type`,
`OperatingSystem`.`_id` as `operatingsystem__id`,
`exploded_pages`.`DateTime`.`date` as `pages_datetime_date`,
`exploded_pages`.`Duration` as `pages_duration`,
`exploded_pages`.`Item`.`_id`.`binary` as `pages_item__id_binary`,
`exploded_pages`.`Item`.`_id`.`type` as `pages_item__id_type`,
`exploded_pages`.`Item`.`Language` as `pages_item_language`,
`exploded_pages`.`Item`.`Version` as `pages_item_version`,
`exploded_pages`.`SitecoreDevice`.`_id`.`binary` as `pages_sitecoredevice__id_binary`,
`exploded_pages`.`SitecoreDevice`.`_id`.`type` as `pages_sitecoredevice__id_type`,
`exploded_pages`.`SitecoreDevice`.`Name` as `pages_sitecoredevice_name`,
`exploded_pages`.`MvTest`.`ValueAtExposure` as `pages_mvtest_valueatexposure`,
`exploded_pages`.`Url`.`Path` as `pages_url_path`,
`exploded_pages`.`VisitPageIndex` as `pages_visitpageindex`,
`Profiles`.`Legal Persona`.`Count` as `profiles_legal_persona_count`,
`Profiles`.`Legal Persona`.`Total` as `profiles_legal_persona_total`,
`Profiles`.`Legal Persona`.`ProfileName` as `profiles_legal_persona_profilename`,
`Profiles`.`Legal Persona`.`Values`.`b2b` as `profiles_legal_persona_values_b2b`,
`Profiles`.`Legal Persona`.`Values`.`selfemployed` as `profiles_legal_persona_values_selfemployed`,
`Profiles`.`Legal Persona`.`Values`.`legal trade` as `profiles_legal_persona_values_legal_trade`,
`Profiles`.`Legal Persona`.`Values`.`personalcivil` as `profiles_legal_persona_values_personalcivil`,
`Profiles`.`Legal Persona`.`PatternId`.`binary` as `profiles_legal_persona_patternid_binary`,
`Profiles`.`Legal Persona`.`PatternId`.`type` as `profiles_legal_persona_patternid_type`,
`Profiles`.`Legal Persona`.`PatternLabel` as `profiles_legal_persona_patternlabel`,
`Referrer` as `referrer`,
`ReferringSite` as `referringsite`,
`SiteName` as `sitename`,
`TrafficType` as `traffictype`,
`UserAgent` as `useragent`,
`Value` as `value`,
`VisitPageCount` as `visitpagecount`,
`_id` as `id` FROM src_analytics_interactions_interactions
 LATERAL VIEW OUTER EXPLODE(`Pages`) view_exploded_pages AS exploded_pages
;

!echo ------------------------;
!echo ------------------------ copy data to s3;
!echo ------------------------;

insert overwrite directory 's3://villani/data/interactions/' stored as orc select * from interactions;


!echo ------------------------;
!echo ------------------------ contacts;
!echo ------------------------;
DROP TABLE IF EXISTS src_analytics_contacts_contacts
;

CREATE EXTERNAL TABLE src_analytics_contacts_contacts(
`System` STRUCT<`Classification`:STRING
,`OverrideClassification`:STRING
,`VisitCount`:STRING
,`Value`:STRING>
,`Lease` STRING
,`_id` STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION 's3://villani/raw/contacts/';

DROP TABLE IF EXISTS contacts;

CREATE TABLE contacts STORED AS ORC AS SELECT
`System`.`Classification` as `system_classification`,
`System`.`OverrideClassification` as `system_overrideclassification`,
`System`.`VisitCount` as `system_visitcount`,
`System`.`Value` as `system_value`,
`Lease` as `lease`,
`_id` as `id` FROM src_analytics_contacts_contacts;

!echo ------------------------;
!echo ------------------------ copy data to s3;
!echo ------------------------;

insert overwrite directory 's3://villani/data/contacts/' stored as orc select * from contacts;


!echo ------------------------;
!echo ------------------------ devices;
!echo ------------------------;
DROP TABLE IF EXISTS src_villani_devices_devices
;

CREATE EXTERNAL TABLE src_villani_devices_devices(
`LastKnownContactId` STRUCT<`binary`:STRING
,`type`:STRING>
,`_t` STRING
,`_id` STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION 's3://villani/raw/devices/';

DROP TABLE IF EXISTS devices;

CREATE TABLE devices STORED AS ORC AS SELECT

`_t` as `_t`,
`LastKnownContactId`.`binary` as `lastknowncontactid_binary`,
`LastKnownContactId`.`type` as `lastknowncontactid_type`,
`_id` as `id` FROM src_villani_devices_devices
 src_villani_devices_devices;

echo ------------------------;
!echo ------------------------ copy data to s3;
!echo ------------------------;

insert overwrite directory 's3://villani/data/devices/' stored as orc select * from devices;


!echo ------------------------;
!echo ------------------------ contacts;
!echo ------------------------;
DROP TABLE IF EXISTS src_analytics_contacts_contacts
;

CREATE EXTERNAL TABLE src_analytics_contacts_contacts(
`System` STRUCT<`Classification`:STRING
,`OverrideClassification`:STRING
,`VisitCount`:STRING
,`Value`:STRING>
,`Lease` STRING
,`_id` STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION 's3://villani/raw/contacts/';

DROP TABLE IF EXISTS contacts;

CREATE TABLE contacts STORED AS ORC AS SELECT
`System`.`Classification` as `system_classification`,
`System`.`OverrideClassification` as `system_overrideclassification`,
`System`.`VisitCount` as `system_visitcount`,
`System`.`Value` as `system_value`,
`Lease` as `lease`,
`_id` as `id` FROM src_analytics_contacts_contacts;

!echo ------------------------;
!echo ------------------------ copy data to s3;
!echo ------------------------;

insert overwrite directory 's3://villani/data/contacts/' stored as orc select * from contacts;