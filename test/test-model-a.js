// author: Chris Rozacki
// load csv splitable mappings template into memory
// dynamically generate splitable csv mapping from the current schema
// compare it line by line

load("config/global.js")
// load the test template
var template = cat("model-a-mapping-test-template.csv")
// load configuration for model a
var collection_to_schema_mapping = JSON.parse(cat("config/model-a.json"))

cd("../")
load("toolkit.js")
load("generate_mapping.js")

var mapping_str = ""
var header = true
for(var source_database_index in collection_to_schema_mapping.databases){
    source_database = collection_to_schema_mapping.databases[source_database_index].database_name
    for(var collection in collection_to_schema_mapping.databases[source_database_index].collections){

        //todo: global variable
        var break_on_oneOf_id = collection_to_schema_mapping.databases[source_database_index].break_on_one_of

        // get_mapping() will search for specific schema based on select_oneOf_ref
        // TODO: this is global variable - fix it!
        var select_oneOf_ref = collection_to_schema_mapping.databases[source_database_index].collections[collection].select_schema_one_of

        var mapping = get_mapping(
        collection_to_schema_mapping.databases[source_database_index].schema
        , break_on_oneOf_id
        , ""
        , collection_to_schema_mapping.databases[source_database_index].collections[collection].collection
        , collection_to_schema_mapping.databases[source_database_index].collections[collection].collection
        , select_oneOf_ref)

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
//quit()

var mapping_lines = mapping_str.split("\n")
var template_lines = template.split("\n")
for(var i in template_lines){
    Assert(template_lines[i], mapping_lines[i], "compare mappings, line "+ i)
}

// otherwise ....
print("PASSED")
