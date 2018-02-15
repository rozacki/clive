// Author Chris Rozacki
var debug_on = false

// generates where clauses for each item in the map
// example: where `_removed`.`toDoProperties`.`type`="EXTRA_BEDROOM_DUE_TO_DISABILITY" or `toDoProperties`.`type`="EXTRA_BEDROOM_DUE_TO_DISABILITY";
function generate_sql_where_generic(map, path){
    var wheres =    {}
    for(var k in map){
        var where_clause = ""
        var first = true
            where_clause = "where `_removed`." + path + "='" + map[k] + "' or " +  path + "='" + map[k] + "'"
        wheres[k] = where_clause
    }

    return wheres
}

// Prints out target tables and where statement coma separated
function print_dictionary_comma_separated(dict){
    for( m in dict){
        print(m + "," + dict[m])
    }
}

// We use 'description' to pass meta-data like PII
// The function returns dictionary of tags
function get_tags(tags_line){
    var tags = {}

    //JSONTYPENAME
    if(tags_line.indexOf("JsonTypeName")!=-1){
        // reach in and get literal from description to create table name
        var tag = tags_line.replace("JsonTypeName : ", "")
        // cleansing the name from anything that is behind JsonNameType : XXXYYYZZ for example PII:....
        var cutOf = tag.indexOf(" ")
        if (cutOf!=-1)
            tag = tag.substr(0,tag.indexOf(" "))
        if(tag!="")
            tags["jsontypename"] = tag
    }
    //PII

    return tags
}

// used in tests to assert
function Assert(expected, current, context){
    var errors = 0
    if (expected!=current){
        print( "### Test FAILED " + context )
        print( "EXPECTED: " + expected )
        print( " CURRENT: "  + current )
        errors ++
    }
    if (errors==10)
        quit()
    print("### Test PASSED " + context )
}

// loads mapping from json file into
function load_mapping_from_json(file_path){
    var s = cat(file_path)
    var m = s.split("\n")
    var mapping = []
    for(var o in m){
        mapping.push(JSON.parse(m[o]))
    }
    return mapping
}

// prints serializes and prints out any object
function print_mapping_as_json(mapping){
    for(var row in mapping){
        print(JSON.stringify(mapping[row]))
    }
}

function print_mapping_splittable(mappings, source_database, source_collection_name){
    var mappings = get_mappings_splittable(mappings, source_database, source_collection_name)
    print(mappings)
}

// gets mapping array and returns mapping as string
function get_mappings_splittable_as_string(mappings, source_database, source_collection_name){
    var mappings_str = ""
    for(var target_table_name in mappings){
        mappings_str = mappings_str + "target_table=" + target_table_name + "\n"
        mappings_str = mappings_str + get_mapping_headers() +"\n"
        var mapping = mappings[target_table_name]
        for(var row in mapping){
            // row, source_database, source_collection, target_table
            mappings_str += get_mapping_row(mapping[row], source_database, source_collection_name , target_table_name) + "\n"
        }
    }
    // remove trailing \n
    return mappings_str.substr(0, mappings_str.length - 1)
}

function get_mapping_as_string(mappings, source_database, source_collection_name, header){
    var mappings_str    =   ""
    if (header){
        mappings_str =  get_mapping_headers() +"\n"
    }
    for(var target_table_name in mappings){
        var mapping = mappings[target_table_name]
        for(var row in mapping){
            // row, source_database, source_collection, target_table
            mappings_str += get_mapping_row(mapping[row], source_database, source_collection_name , target_table_name) + "\n"
        }
    }
    // remove trailing \n
    return mappings_str.substr(0, mappings_str.length - 1)
}

function print_mapping(mappings, source_database, source_collection_name, print_header=true){
    if(print_header)
        print_mapping_headers()
    for(var target_table_name in mappings){
        var mapping = mappings[target_table_name]
        for(var row in mapping){
            print_mapping_row(mapping[row], source_database, source_collection_name, target_table_name)
        }
    }
}

// generic function that generate mapping by breaking schema
// it returns object{  "target_table_name_a" : array, "target_table_name_b" : array .... }
function get_mapping(schema_name, break_on_oneOf_id, stop_on_oneOf_id, target_table_name, source_collection_name, select_oneOf_ref){
    debug("schema database name is " + schema_database_name)
    debug("schema collection name is " + schema_collection_name)

    debug("get_mapping(schema_name, break_on_oneOf_id, stop_on_oneOf_id, target_table_name, source_collection_name, select_oneOf_ref) ")
    debug("get_mapping("  + schema_name + "," +  break_on_oneOf_id + "," + stop_on_oneOf_id + "," + target_table_name+ "," +  source_collection_name + "," + select_oneOf_ref +")")

    var mappings = {}
    mappings[target_table_name] = []

    // mappings is a dictionary of all target tables
    var mappings = generate(schema_name, mappings, target_table_name, null, stop_on_oneOf_id, break_on_oneOf_id, select_oneOf_ref)
    for(var target_table_name in mappings){
        var mapping = mappings[target_table_name]

        // add collection id to the mapping
        // to join main table with child tables
        add_id_to_mapping(mapping, source_collection_name)

        mapping = filter_out_system_fields(mapping)
        mapping = deduplicate_rows(mapping)

        mappings[target_table_name] = mapping
    }
    return mappings
}

// collection id is usually included into schema but mapped onto Mongo _id
function add_id_to_mapping(mapping, source_collection_name){

    // collection can have compound key which is defined as array
    if(Array.isArray(collection_ids[source_collection_name])){

        for(var i in collection_ids[source_collection_name]){
            // add collection id to join main table with child tables
            var id          = {}
            //if collection has simple key thn its definition is as below
            id.path         = collection_ids[source_collection_name][i].path
            id.type         = "string"
            id.column_name  = collection_ids[source_collection_name][i].name
            mapping.push(id)
        }

        return
    }
    // add collection id to join main table with child tables
    var id          = {}
    //if collection has simple key thn its definition is as below
    //print(source_collection_name)
    id.path         = collection_ids[source_collection_name].path
    id.type         = "string"
    id.column_name  = "id"
    mapping.push(id)
}

// generate mapping between property and it's literal type, literal type is usually embedded as description in type property of schema
// uses get_oneOf() function defined outside of this scope
function map_oneOf_to_type(id, target_table_name, get_oneOf_function){
    var collection = db.getCollection(schema_collection_name)
    var schema = collection.findOne({'id':id})

    var oneOf = get_oneOf_function(schema)
    var refs = []
    for(var entry in  oneOf){
        refs.push(oneOf[entry]["$ref"])
    }
    var map = {}
    for(ref in refs){
        var t = find_schema_by_title(refs[ref])
        var name = get_tags(t["properties"]["type"]["description"])["jsontypename"]
        map[target_table_name + "_" + name] = name
    }

    return map
}

// find and loads schema
// title must end with ".json"
function find_schema_by_title(title){
    return find_schema_by_id(title)
    var collection_classes  = db.getCollection(schema_collection_name)
    return  collection_classes.findOne({"title":title.substring(0,title.length-5)});
}

function find_schema_by_id(id){
    var collection_classes  = db.getCollection(schema_collection_name)
    return  collection_classes.findOne({"id":id});
}

function debug(message){
    if(debug_on){
        print(message)
    }
}

// Converts technical mappgin types into java types
// Some mapping types are different than javascript
function convert_types(type){
    switch(type){
        case "int": return "integer";
        default: return type;
    }
}

function convert_to_tm_type(property){
    switch(property.type.toLowerCase()){
            case "string":{
                // check if we actually dealing with datetime
                if( "format" in property ){
                    if( property.format == "DATE_TIME")
                           property.path = property.path + ".d_date"
                           property.type = "string"
                           property.target_type = "timestamp"
                           break;
                    }
                break;
            }
            case "integer":  property.type = "int"
            case "any": property.type = "string"
        }
}

// used to show ignored_refs stats
var show_refs_flag=false
function show_refs(message){
    if(!show_refs_flag)
        return
    print(message)
}

//  convert object into array, then sort items
function sortProperties(obj)
{
  // convert object into arraymon
	var sortable=[];
	for(var key in obj)
		if(obj.hasOwnProperty(key))
			sortable.push([key, obj[key]]); // each item is an array in format [key, value]

	// sort items by value
	sortable.sort(function(a, b)
	{
	  return b[1]-a[1]; // compare numbers
	});
	return sortable; // array in format [ [ key1, val1 ], [ key2, val2 ], ... ]
}

// convert title or $ref to id
// example:
// "id" : "urn:jsonschema:uk:gov:dwp:universe:agent:core:todo:properties:InterventionFeedbackProperties"
// "title" : "uk.gov.dwp.universe.agent.core.todo.properties.InterventionFeedbackProperties"
// example:
// $ref: uk.gov.dwp.universe.claim.effective.EffectiveFromStartOfClaim.json
function convert_title_to_id(title){
    title = title.replace(".json","").replace(/\./g,":").replace("urn:jsonschema:","")
    return   "urn:jsonschema:" + title
}

//finds recursively  all internal ref in provided schema document. Internal refs are not loaded from another json file.
function find_internal_schemas_by_title(title){
    var schema = find_schema_by_title(title)
    var found_schemas = {}

    if(schema == null){
        debug("could not find schema by title " + title)
        return found_schemas
    }
    return find_internal_schema(schema,found_schemas)
}

// looks for all schemas id that exist inside provided schema only
// internal schemas are based on id
function find_internal_schema(schema, found_schemas){
    for(property in schema.properties){
        switch(schema.properties[property].type){
            case "object":{
                if("id" in schema.properties[property]){
                    found_schemas[schema.properties[property].id] = schema.properties[property]
                }
                break
            }
            case "array":{
                break
            }
        }
        //drill further down
        find_internal_schema(schema.properties[property], found_schemas)
    }
    return found_schemas
}

// Converts jsonath to legal sql (HIVE) column name
// For example:
// JSONPATH sanctionProgressEvents[*].trace.elements[*].description
// will be converted into sanctionProgressEvents_trace_elements_description
function get_column_name(jsonpath){
    return jsonpath.replace(/\./g,"_").replace(/\[/g,"").replace(/\]/g,"").replace(/\*/g,"")
}

function test_find_internal_schemas(){
    var found_schemas = find_internal_schemas("schema","uk.gov.dwp.universe.declaration.childcare.ChildcareDeclaration.json")
    for(schema in found_schemas){
        print(schema)
    }
}

function filter_out_system_fields(mapping){
    return mapping.filter(function(element){return element.path !== "_version" && element.path !== "createdDateTime"})
}

// deduplicate equal rows (used to  avoid get_json_object...)
// equals rows have the same source path and type
function deduplicate_rows(mapping){
    var dict={}
    var arr=[]
    for(var i in mapping){
        //todo - hash?
        dict[mapping[i].path + "_" + mapping[i].type] = mapping[i]
    }

    for(i in dict) {
        arr.push(dict[i]);
    }
    return arr;
}

function get_mapping_headers(){
    return "sourcesourceDB,sourceCollection,sourceFieldLocation,sourceDataType,destinationTable,destinationField,destinationDataType,function,meta"
}

function print_mapping_headers(){
    print(get_mapping_headers())
}
