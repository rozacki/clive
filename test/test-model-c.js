// Author Chris Rozacki

// Generates model where one mongo collection is converted into one table

load("config/global.js")
var collection_to_schema_mapping = JSON.parse(cat("config/model-c.json"))
// load the test template
var template = cat("model-c-mapping-test-template.csv")

cd("../")
load("toolkit.js")
load("generate_mapping.js")


var break_on_oneOf_id       =   ""
var select_oneOf_ref        =   ""
var stop_on_oneOf_id        =   ""
var header = true
var mapping_str = ""

for(var source_database_index in collection_to_schema_mapping.databases){
    var disabled = collection_to_schema_mapping.databases[source_database_index].hasOwnProperty('disabled')?true:false
    var source_database = collection_to_schema_mapping.databases[source_database_index].database_name

    if(disabled){
        continue
    }
    //print(collection_to_schema_mapping.databases[source_database_index].database_name)
    for(var collection in collection_to_schema_mapping.databases[source_database_index].collections){

        var mapping = get_mapping(
        collection_to_schema_mapping.databases[source_database_index].collections[collection].schema
        , break_on_oneOf_id
        , stop_on_oneOf_id
        , collection_to_schema_mapping.databases[source_database_index].collections[collection].collection
        , collection_to_schema_mapping.databases[source_database_index].collections[collection].collection)

        var mappings = {}
        mappings[collection_to_schema_mapping.databases[source_database_index].collections[collection].collection] =
        mapping[collection_to_schema_mapping.databases[source_database_index].collections[collection].collection]


        mapping_str += get_mapping_as_string(
        mappings
        , source_database
        , collection_to_schema_mapping.databases[source_database_index].collections[collection].collection
        ,header)
        mapping_str+="\n"
        header = false
    }
}

//print(mapping_str)

var mapping_lines = mapping_str.split("\n")
var template_lines = template.split("\n")
for(var i in template_lines){
    Assert(template_lines[i], mapping_lines[i], "compare mappings, line "+ i)
}

// otherwise ....
print("PASSED")