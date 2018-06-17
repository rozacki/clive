// Author Chris Rozacki
// Date 2017-04-20

//Generate technical mapping from json schema

// to start mongo
// mongod --fork --logfile ~/var/log/mongod.log

// to transform into SQL
//

load("toolkit.js")
load("collection_ids.js")

//
var schema_database_name = "clive"
var conn                = new Mongo();
var db                  = conn.getDB(schema_database_name);

var json_path_separator = "."
// used to output mapping
var source_database = "sourcedb"
var source_collection = "sourcecoll"
var source_type = "string"
var target_database = "targetdatabase"
var target_column = "targetcolumn"
var mapping_separator = ","

// schema_name - name of class to generate mapping from
// path - generated jsonpath
function generate(schema_name, mappings, target_table_name, path, stop_on_oneOf_id, break_on_oneOf_id, select_oneOf_ref){
    debug("generate(stop_on_oneOf_id = " + stop_on_oneOf_id + ", break_on_oneOf_id =" + break_on_oneOf_id + ", select_oneOf_ref =" + select_oneOf_ref + ")")
    var schema = find_schema_by_id(schema_name)

    if(schema === null){
        debug("could not find schema " + schema_name)
        return
    }
    debug("schema found " + schema_name)
    var internal_schemas = find_internal_schemas_by_title(schema_name)

    if(path == null)
        path = ""
    return generate_mapping(schema, path, mappings, internal_schemas, target_table_name, stop_on_oneOf_id, break_on_oneOf_id, select_oneOf_ref )
}

// traverse and generates technical mapping from schema class
// todo: list of missing internal and external schemas to the list and return instead of printing them out
// todo: add parameter to search missing references only to establish how many is missing in other words how complete the schema is
function generate_mapping(schema_definition, current_path, mappings, internal_schemas, target_table_name, stop_on_oneOf_id, break_on_oneOf_id, select_oneOf_ref){
    debug("generate_mapping(stop_on_oneOf_id = " + ", break_on_oneOf_id =" + break_on_oneOf_id + ",select_oneOf_ref =" +  select_oneOf_ref + ")")
    var mapping = mappings[target_table_name]

    if ("additionalProperties" in schema_definition){
            debug("additionalProperties found but not processed " + current_path)
            return
    }

    if("properties" in schema_definition){
        for(var property in schema_definition.properties){
            var path =  property
            if(current_path.length != 0)
                path = current_path + json_path_separator + property

            switch(schema_definition.properties[property].type){
                case "array":{
                    path += "[*]"
                    // are we dealing with simple type or object
                    // not supported: array of arrays [][]
                    // "items"

                    // if schema is derived from empty json array e.g. a:[] then items element does not exist
                    // TODO: is it valid JSONSchema? Regardless, catch it and swallow

                    if (!schema_definition.properties[property].hasOwnProperty("items")){
                        debug("array is missing items element, carrying on...")
                        break;
                    }
                    // complex type in array
                    if(schema_definition.properties[property].items.type == "object"){
                        debug("found schema " + schema_definition.properties[property].items.id)
                        generate_mapping(schema_definition.properties[property].items, path, mappings, internal_schemas, target_table_name, stop_on_oneOf_id, break_on_oneOf_id, select_oneOf_ref)
                    }
                    // reference to type in array
                    else if(schema_definition.properties[property].items.type == null && schema_definition.properties[property].items.$ref != null){
                      /*EXAMPLE:
                        "attachmentSummaries" : {
                                 "type" : "array",
                                 "items" : {
                                   "$ref" : "urn:jsonschema:uk:gov:dwp:universe:attachment:AttachmentSummary"
                                 }
                               },
                        */

                         var schema = internal_schemas[schema_definition.properties[property].$ref]
                         if(schema == null){
                             debug("schema not found " + schema_definition.properties[property].$ref)
                             break;
                         }
                         debug("found schema " + schema_definition.properties[property].$ref)
                         generate_mapping(schema, path, mappings, internal_schemas, target_table_name, stop_on_oneOf_id, break_on_oneOf_id, select_oneOf_ref)
                    }
                    // simple type in array
                    else if(schema_definition.properties[property].items.type != null){
                    /* EXAMPLE:
                        "deliveryUnits" : {
                              "type" : "array",
                              "items" : {
                                "type" : "string",
                                "description" : "UUID (uk.gov.dwp.universe.organisation.DeliveryUnitId)"
                              }
                            },
                        */
                        // here we add to mapping
                        debug("adding to schema " + JSON.stringify(schema_definition.properties[property]))
                                            debug("adding to schema path " + path)
                        mapping.push(helper_create(schema_definition.properties[property].items, path))
                    }
                    break;
                }
                case "object":{
                    //todo: gather stats on how many $ref we skip I shall output them and carry on
                    if("$ref" in schema_definition.properties[property]){

                        var schema = internal_schemas[schema_definition.properties[property].$ref]
                        if(schema == null){
                            debug("schema not found " + schema_definition.properties[property].$ref)
                            break;
                        }
                        debug("found schema " + schema_definition.properties[property].$ref)
                        generate_mapping(schema, path, mappings, internal_schemas, target_table_name, stop_on_oneOf_id, break_on_oneOf_id, select_oneOf_ref)
                        break
                    }

                    generate_mapping(schema_definition.properties[property], path, mappings, internal_schemas, target_table_name, stop_on_oneOf_id, break_on_oneOf_id, select_oneOf_ref)
                    break;
                }
                // any simple type
                //also "any" type
                default:
                    // here create new mapping row
                    debug("adding to schema " + JSON.stringify(schema_definition.properties[property]))
                    debug("adding to schema path " + path)
                    mapping.push(helper_create(schema_definition.properties[property], path))
             }
        }
    }
    // after we handled all properties we start looking at "oneOf"
    if("oneOf" in schema_definition){
        debug("references found in oneOf "+ schema_definition.id)

        //
        if( stop_on_oneOf_id.indexOf(schema_definition.id) != -1){
            // we don't carry on
            debug("stopped on " + schema_definition.id)
        }else{
            //break_on_oneOf_id is string that if equals to current oneOf.id then we create separate mappings
            if(schema_definition.id == break_on_oneOf_id){

                // select oneof by ref
                if(select_oneOf_ref != ""){
                    // iterate and load all classes
                    for(ref in schema_definition.oneOf){
                        if(schema_definition.oneOf[ref].$ref == select_oneOf_ref){
                            debug("breaking on " + select_oneOf_ref)
                            // loads dependency
                            generate(schema_definition.oneOf[ref].$ref, mappings, target_table_name, current_path, stop_on_oneOf_id, break_on_oneOf_id, select_oneOf_ref)
                            break;
                        }
                    }
                    return mapping
                }

                // iterate and load all classes
                for(type in schema_definition.oneOf){
                    debug("breaking on " +  schema_definition.oneOf[type].$ref + " in " + break_on_oneOf_id)
                    // todo: find missing mongo id
                    //      convert int and string with date to date type
                    //      convert ISO timestamp string to timestamp
                    //      filter out - and @ character and generate global warnings table
                    //
                    var oneOf_schema = find_schema_by_title(schema_definition.oneOf[type].$ref)

                    // reach in and get literal from description to create table name
                    var target_table_name_suffix = get_tags(oneOf_schema["properties"]["type"]["description"])["jsontypename"]
                    var oneOf_target_table_name = target_table_name + "_" + target_table_name_suffix
                    mappings[oneOf_target_table_name] = []
                    generate(schema_definition.oneOf[type].$ref, mappings, oneOf_target_table_name ,current_path, stop_on_oneOf_id, break_on_oneOf_id, select_oneOf_ref)
                }
            }else{
                // iterate and load all classes
                for(type in schema_definition.oneOf){
                    // loads dependency
                    generate(schema_definition.oneOf[type].$ref, mappings, target_table_name, current_path, stop_on_oneOf_id, break_on_oneOf_id, select_oneOf_ref)
                }
            }
        }
    }
    return mappings
}

// test
//print(create_target_table_name("uk.gov.dwp.universe.agent.core.todo.properties.APACheckLandlordToDoProperties.json"))

// returns the last segement of asxchema name urn
function create_target_table_name(schema_name){
    var segments = schema_name.split('.')
    // skip ".json" file extension
    return '_' + segments[segments.length-2]
}

function print_mapping_row(row, source_database, source_collection, target_table){
    print(get_mapping_row(row, source_database, source_collection, target_table))
}

// returns row as string
// row usually contains: path, type, meta (as description)
// tries to work out:
// - column name from path
// - target type as string
function get_mapping_row(row, source_database, source_collection, target_table){

    var line = source_database + mapping_separator
                          + source_collection + mapping_separator
                          + row.path + mapping_separator
                          + row.type + mapping_separator
                          + target_table + mapping_separator

        if(row.column_name == null)
            line+=get_column_name(row.path)
        else
            line+=row.column_name

        var last_columns = mapping_separator

        if("target_type" in row){
            last_columns = last_columns + row.target_type + mapping_separator + mapping_separator
        }else{
            last_columns = last_columns + "string" + mapping_separator + mapping_separator
        }
        line += last_columns

        // use description as meta column
        if("description" in row){
                line += row["description"]
            }

     return line
}

function helper_create(property, path){
    // here we add to mapping and return
    target = {}
    target.path =  path
    for(var p in property){
        target[p]=property[p]
    }
    convert_to_tm_type(target)
    return target
}