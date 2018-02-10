load("toolkit.js")
load("generate_mapping.js")

// Author Chris Rozacki

// Generates model where one mongo collection is converted into one table

var collection_to_schema_mapping = JSON.parse(cat("config/model-c.json"))

function generate_model_c(){

    // all collections schema is broken down in the same place
    var break_on_oneOf_id       =   ""
    // global variable that is set when drilling into selected one schema from many one Of
    // it's hydrated from collection_to_schema_mapping.$.schema
    var select_oneOf_ref        =   ""
    var stop_on_oneOf_id        =   ""

    var header = true
    for(var source_database_index in collection_to_schema_mapping.databases){
        var disabled = collection_to_schema_mapping.databases[source_database_index].hasOwnProperty('disabled')?true:false
        source_database = collection_to_schema_mapping.databases[source_database_index].database_name

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

            print_mapping(mappings
            , source_database
            , collection_to_schema_mapping.databases[source_database_index].collections[collection].collection, header)
            header = false
        }
    }
}
generate_model_c()