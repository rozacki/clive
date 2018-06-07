load("toolkit.js")
load("generate_mapping.js")


var mapping = get_mapping(
            "http://devices.json"   // schema_name
            , ""                    // break_on_oneOf_id
            , ""                    // stop_on_oneOf_id
            , "devices"             // target_table_name
            , "devices"             // source_collection_name
            , "")                   // select_oneOf_ref

print_mapping(mapping
                , "villani"
                , "devices", true)