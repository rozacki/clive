!echo ------------------------;
!echo ------------------------ contacts;
!echo ------------------------;

CREATE EXTERNAL TABLE `contacts`(
  `system_classification` string,
  `system_overrideclassification` string,
  `system_visitcount` string,
  `system_value` string,
  `lease` string,
  `id` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
    "s3://villani/data/contacts"


!echo ------------------------;
!echo ------------------------ interactions;
!echo ------------------------;
CREATE EXTERNAL TABLE `interactions`(
  `_t` string,
  `contactid_binary` string,
  `contactid_type` string,
  `startdatetime_date` string,
  `enddatetime_date` string,
  `savedatetime_date` string,
  `channelid_binary` string,
  `channelid_type` string,
  `browser_browserversion` string,
  `browser_browsermajorname` string,
  `browser_browserminorname` string,
  `screen_screenheight` string,
  `screen_screenwidth` string,
  `contactvisitindex` string,
  `deviceid_binary` string,
  `deviceid_type` string,
  `geodata_areacode` string,
  `geodata_businessname` string,
  `geodata_city` string,
  `geodata_country` string,
  `geodata_dns` string,
  `geodata_isp` string,
  `geodata_metrocode` string,
  `geodata_postalcode` string,
  `geodata_region` string,
  `geodata_url` string,
  `ip_binary` string,
  `ip_type` string,
  `language` string,
  `locationid_binary` string,
  `locationid_type` string,
  `operatingsystem__id` string,
  `pages_datetime_date` string,
  `pages_duration` string,
  `pages_item__id_binary` string,
  `pages_item__id_type` string,
  `pages_item_language` string,
  `pages_item_version` string,
  `pages_sitecoredevice__id_binary` string,
  `pages_sitecoredevice__id_type` string,
  `pages_sitecoredevice_name` string,
  `pages_mvtest_valueatexposure` string,
  `pages_url_path` string,
  `pages_visitpageindex` string,
  `profiles_legal_persona_count` string,
  `profiles_legal_persona_total` string,
  `profiles_legal_persona_profilename` string,
  `profiles_legal_persona_values_b2b` string,
  `profiles_legal_persona_values_selfemployed` string,
  `profiles_legal_persona_values_legal_trade` string,
  `profiles_legal_persona_values_personalcivil` string,
  `profiles_legal_persona_patternid_binary` string,
  `profiles_legal_persona_patternid_type` string,
  `profiles_legal_persona_patternlabel` string,
  `referrer` string,
  `referringsite` string,
  `sitename` string,
  `traffictype` string,
  `useragent` string,
  `value` string,
  `visitpagecount` string,
  `id` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  's3://villani/data/interactions'

!echo ------------------------;
!echo ------------------------ devices;
!echo ------------------------;

CREATE TABLE `devices`(
  `_t` string,
  `lastknowncontactid_binary` string,
  `lastknowncontactid_type` string,
  `id` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
   's3://villani/data/devices'


