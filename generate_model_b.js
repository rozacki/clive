// Author Chris Rozacki

// One collection is converted into specific tables based on manually provided type. The same schema AcceptedData is reused.
// The collection is broken down  in ClaimElement element.
// This model is similar to model d but the list of 'types' which are used to provide tables is fixed.

load("toolkit.js")
load("generate_mapping.js")

//
var path = "`claimElement`.`type`"

// all collections point to one main schema, then it's broken down
var id                      =   "urn:jsonschema:uk:gov:dwp:universe:accepted:data:AcceptedData"
// will be used to postfix and generate target table name for example accepted_data_xyz
var target_table_name       =   "earningsData"

function get_oneOf(schema){
    return schema["properties"]["claimElement"]["oneOf"]
}

function get_accepted_data_mapping(){
    var mappings = {}
    var collection_to_schema_mapping = JSON.parse(cat("config/model-b.json"))
    var header = true

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
        print_mapping( mappings, source_database, source_collection_name, header)
        header = false
    }
}
if(method == "where"){
    print_dictionary_comma_separated(generate_sql_where_generic(map_oneOf_to_type(id, target_table_name, get_oneOf)), path )
}else
    get_accepted_data_mapping()