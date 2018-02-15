load("toolkit.js")
load("generate_mapping.js")

/*
var mapping = get_mapping(
            "http://example.com/example.json"
            , ""
            , ""
            , "interactions"
            , "interactions"
            , "")

print_mapping(mapping
                , "analytics"
                , "interactions", true)

var mapping = get_mapping(
            "http://contacts.json"
            , ""
            , ""
            , "contacts"
            , "contacts"
            , "")

print_mapping(mapping
                , "villani"
                , "contacts", true)
*/

var mapping = get_mapping(
            "http://devices.json"
            , ""
            , ""
            , "devices"
            , "devices"
            , "")

print_mapping(mapping
                , "villani"
                , "devices", true)