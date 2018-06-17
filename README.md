#Mongo start
./mongod  --dbpath ~/Servers/data/mongo/  --fork --logpath ~/var/log/mongo.log

#enable debug mode in js scripts
toolkit.js
var debug_on = true

#load schema to schema collection
mongoimport -d clive -c schema-84 ~/Documents/json-schema-with-pii/schema.json 

#Technical mapping generation
**Known issues**

- When both "oneOf" and "properties" have the same property it results in generating two or more target columns with the same name. After generating SQL we can see unnecessary GET_JSON_OBJECT with erronous $. json path

- TYPE - 
  All target types are string

- ISO timestamp strings to TIMESTAMP conversion - 
- Are not converted into timestamp. TIMESTAMP type Is not handled properly as during export from mongo there is an implicit conversion to struct, and another transformation is made to sanitise JSON so that HIVE can use ingest: $ -> d_data

- INT into DATE conversion - 
  Is not supported
  
- Mongo _id - 
  Schema doesn't provide mongo id. ids handled by Mongo
  
- COLUMN NAMES - 
  Column names are generated automatically and camel case, - they have to be changed manually if necessary
 
- implicit conversions
  {"d_date":"2017-07-10T10:52:12.192Z"}	- duting import to hdfs
  {"d_numberlong":"40846"} - during mongoexport

# Testing
N/A

# Many tables derived from single schema - Accepted-data database
Accepted-data collections use the same schema urn:jsonschema:uk:gov:dwp:universe:accepted:data:AcceptedData.
The only difference is that oneOf found in urn:jsonschema:uk:gov:dwp:universe:claim:ClaimElement.
The collection_to_schema_mapping configuration maps collections to schema for example:
collection 'address' to schema 'uk.gov.dwp.universe.claim.address.Address.json'.

Complex schema convert into many tables from different location 
# Databases that use the above steps to generate SQL anf CSV files
## accepted-data
## advances
## agent-core
## core
## matchingservice
## Penalties-and-deductions

I
jive accepts one location per mapping file hence we split mappings into individual file.
Generate splittable mapping
$ mongo -quiet DATABASE_NAME.js > DATABASE_NAME.csv

II
Split mapping in folder from DATABASE_NAME.csv
$ cd temp
$ ../toolkit.py -p ../DATABASE_NAME.csv --split-mappings


III
Generate sql from mappings from folder
$ cd ..
$ ./toolkit.py --generate-sql -o DATABASE_NAME.sql -p temp

IV
Combine one mapping from all mappings in a folder
$ ./toolkit.py --combine-mapping-files -p temp -o DATABASE_NAME.csv



$ jive -tm appointments.csv -l /etl/uc/mongo/${hiveconf:BATCH_DATE} -orc > appointments.sql

#Database that don't require any additional modeling
## appointments


# Complex schema convert into many tables from the same data location

##agenttodo
##agenttotoarchive
##core-todo
##core-journal

## steps
I
Generate schema
II
Generate where caluses
III
Generate SQL

#CLIVE list of commented tables
    journal_AnnualVerificationJournalEntry
    todo_REPORT_SELF_EMPLOYMENT_EARNINGS_PROPERTIES
    claimantCommitment
    agent_todo_archive_VERIFY_SOCIAL_HOUSING_PROPERTIES
# CLIVE list of co,emted views
    claimantCommitment_no_pii
