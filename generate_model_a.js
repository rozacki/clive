load("toolkit.js")
load("generate_mapping.js")

// Author Chris Rozacki

// Many collections reuse the same schema. They re-use the same JAVA interface urn:jsonschema:uk:gov:dwp:universe:claim:ClaimElement.
// All mongo collections document are instances of JAVA class urn:jsonschema:uk:gov:dwp:universe:accepted:data:AcceptedData
// that hold reference to the ClaimElement interface.

// For AcceptedData:
// there is one schema AcceptedData
// that has ~33 oneOf
// those 33 oneOf are distributed onto 33 collections
// All collections from accepted-data use "id" : "urn:jsonschema:uk:gov:dwp:universe:accepted:data:AcceptedData"
// schema as a holder to one of specialized 33 schemas. Each of 33 specialized schema is stored in separate collection

function generate_model_a(print_out){
    var mappings = []
    var header = true
    var collection_to_schema_mapping = JSON.parse(cat("config/model-a.json"))

    for(var source_database_index in collection_to_schema_mapping.databases){
        source_database = collection_to_schema_mapping.databases[source_database_index].database_name
        //print(collection_to_schema_mapping.databases[source_database_index].database_name)
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

            if(print_out){
                print_mapping(mapping
                , source_database
                , collection_to_schema_mapping.databases[source_database_index].collections[collection].collection, header)
                header = false
            }
            mappings.push(mapping)
        }
    }
    return mappings
}
generate_model_a(true)