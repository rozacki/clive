// Author Chris Rozacki
// Date 2017-04-08
// iterates mapping collection imported from csv file and find class for each entry
// found classes are added to the mapping collection and can be exported back to csv

load("toolkit.js")

//
var conn                = new Mongo();
var db                  = conn.getDB("uc");
// to gather stats on how many $refs are not handled
var found_refs = 0

// used collection names
var schema_collection_name = "schema"
var mapping_collection_name = "mapping"

// test collection names - swap to
var test_schema_collection_name = "schema_test"
var test_mapping_collection_name = "mapping_test"

// Groups and iterates source collections
function match_all(){
    // groups per source collection
    var collection_mapping      = db.getCollection(mapping_collection_name)
    var cursor_mapping          = collection_mapping.distinct("sourceCollection")


    for(var i in cursor_mapping){
        // before matching reset match counters
        match_counters = {}

        // set limit to some high size to match whole mapping
        match(cursor_mapping[i],1000, 0, 3)
    }

    show_refs(" $refs not handled " + found_refs)
}

function match_one(source_collection){
    // before matching reset match counters
    match_counters = {}
    // set limit to some high size = 100, offest=0, show all matches
    match(source_collection, 1000, 0, 3)
}
//
function match(source_collection, limit, offset, show_results){
    var collection_mapping      = db.getCollection(mapping_collection_name)
    var cursor_mapping          = collection_mapping.find({"sourceCollection":source_collection}).skip(offset).limit(limit);
    // found any match between mapping row and schema
    var matched_at_leats_one_schema = false
    // if defined globally (outside) then we can examine match_counters from mongo shell
    //var match_counters = {}

    print("====== source collection " + source_collection +" ========")
    while(cursor_mapping.hasNext()){
        var collection_classes  = db.getCollection(schema_collection_name)
        var cursor_classes      = collection_classes.find();

        mapping_row             = cursor_mapping.next();
        debug("sought jsonpath " + mapping_row.sourceFieldLocation)
        while(cursor_classes.hasNext()){
            type_definition = cursor_classes.next()
            debug("schema title "+ type_definition.title)
            // found match between row and current json schema
            var schema_matched = match_schema(mapping_row.sourceFieldLocation, type_definition, convert_types(mapping_row.sourceDataType))
            matched_at_leats_one_schema = schema_matched || matched_at_leats_one_schema
            if(schema_matched){
                debug ("MATCHED: " + mapping_row.sourceFieldLocation + " --- " + mapping_row.sourceDataType + " --- " + type_definition.title);
                if(match_counters[type_definition.title] == null){
                    match_counters[type_definition.title]=1
                }else{
                    match_counters[type_definition.title]++
                }
            }
        }
        if(!matched_at_leats_one_schema){
            print ("NOT MATCHED: " + mapping_row.sourceFieldLocation + " --- " + mapping_row.sourceDataType + " --- " + type_definition.title);
        }else{
            debug(JSON.stringify(match_counters))
            debug("MAPPING for " + mapping_row.sourceFieldLocation + " found " + Object.keys(match_counters).length + " times")
        }
    }


    var max = 0;
    var counter_name_max = ""
    var i = 0;

    sorted = sortProperties(match_counters)
    print("showing "+ show_results +" results")
    // loop ans show best x matches
    if(sorted.length>0){
        for(var i=0;i<sorted.length && i<show_results;++i){
            print("class best match " + sorted[i][0]+", matched mappings "+ sorted[i][1] + " out of " + cursor_mapping.size())
        }
    }else{
        print("class best match no schema found, matched mappings 0 out of " + cursor_mapping.size())
    }
}

// recursively finds property with provided name (from json_path)
// todo: load class based on ref
function match_schema(json_path, type_definition, source_type){
    if(json_path.length == 0){
        // we dealing with array item
        if(type_definition.type == source_type){
            return true;
        }
        return false;
    }

    // remember to reuse when we load class file

    // take first segment
    // create new jsonpath
    var json_path_original = json_path
    var segments = json_path.split('.')
    var json_segment = json_path
    json_path = ""
    if(segments.length>1){
        json_segment = segments.shift()
        json_path = segments.join('.')
    }
    if(json_segment.indexOf('[') != -1){
        json_segment = json_segment.substring(0,json_segment.indexOf('['))
    }

    debug("sought segment " + json_segment)

    if("properties" in type_definition){
        debug("found properties");
        if(json_segment in type_definition.properties){
            debug("found property")
            if(type_definition.properties[json_segment].type == source_type){
                return true
            }else{
                switch(type_definition.properties[json_segment].type){
                    case "array":{
                        // if we have array
                        return match_schema(json_path, type_definition.properties[json_segment].items, source_type);
                    }
                    case "object":{
                        //to gather stats on how many $ref we skip I shall output them and carry on
                        if("$ref" in type_definition.properties[json_segment]){
                            show_refs("ignored $ref " + JSON.stringify(type_definition.properties[json_segment]) + " for json segment " + json_segment)
                            found_refs++
                            return false
                        }
                        //if we have object and json_path is not empty then we carry on drilling
                        if(json_path.length > 0){
                            return match_schema(json_path, type_definition.properties[json_segment], source_type);
                        }
                        return false;
                    }
                    case "any":{
                        return true;
                    }

                    default:
                        if(source_type == "string"){
                            // if source type is string and schema type is a simple one (not object nor array)
                            return true;
                        }
                        print( " segment " + json_segment + ",expected source type " + source_type + " found type " + type_definition.properties[json_segment].type)
                        return false;
                }
            }
        }
    }
    if("oneOf" in type_definition){
        // iterate and load all classes
        for(type in type_definition.oneOf){
            debug("class name " + type_definition.oneOf[type].$ref.substring(0,type_definition.oneOf[type].$ref.length-5 ))
            var cursor_classes = find_schema_by_title(schema_collection_name, type_definition.oneOf[type].$ref)
            if(cursor_classes == null){
                print("class file definition could not be found " + type)
                return false;
            }

            return match_schema(json_path_original, cursor_classes, source_type)
        }
    }
    return false

}

// Unit tests
function test(){
    //
    conn                = new Mongo();
    db                  = conn.getDB("uc");
    match_counters      = {}
    match_one("childcareDeclaration")
    print(match_counters.length)
}