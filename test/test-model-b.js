// Author Chris Rozacki

load("config/global.js")
function get_oneOf(schema){
    return schema["properties"]["claimElement"]["oneOf"]
}

var collection_to_schema_mapping = JSON.parse(cat("config/model-b.json"))
var template = cat("model-b-mapping-test-template.csv")

cd("..")
load("toolkit.js")
load("generate_mapping.js")

var path        = "`claimElement`.`type`"
var id           =   "urn:jsonschema:uk:gov:dwp:universe:accepted:data:AcceptedData"
var mappings    = {}
var header      = true
var mapping_str = ""

for(var source_database_index in collection_to_schema_mapping.databases){
    var source_database        =    collection_to_schema_mapping.databases[source_database_index].database_name
    var source_collection_name =    collection_to_schema_mapping.databases[source_database_index].source_collection_name
    for(var collection in collection_to_schema_mapping.databases[source_database_index].target_tables){
        // get_mapping() will search for specific schema based on select_oneOf_ref
        var select_oneOf_ref        =   collection_to_schema_mapping.databases[source_database_index].target_tables[collection].schema
        var break_on_oneOf_id       =   "urn:jsonschema:uk:gov:dwp:universe:claim:ClaimElement"
        var stop_on_oneOf_id        =   ""
        var target_table            =   collection_to_schema_mapping.databases[source_database_index].target_tables[collection].table
        // get_mapping will generate mapping for single target table
        var mapping = get_mapping(
            id
            , break_on_oneOf_id
            , stop_on_oneOf_id
            , target_table                              // target table name
            , source_collection_name                    // source collection
            , select_oneOf_ref)

        mappings[target_table] = mapping[target_table]

    }
    mapping_str += get_mapping_as_string(
        mappings
        , source_database
        , source_collection_name
        ,header)
        mapping_str+="\n"
        header = false
}

print(mapping_str)
quit()

var mapping_lines = mapping_str.split("\n")
var template_lines = template.split("\n")
for(var i in template_lines){
    Assert(template_lines[i], mapping_lines[i], "compare mappings, line "+ i)
}

// otherwise ....
print("PASSED")
