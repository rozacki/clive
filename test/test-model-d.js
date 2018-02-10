// author: Chris Rozacki
load("config/global.js")

cd('../');
load("generate_model_d.js")

// generate mappings to test some properties
var mappings_temp = generate_model_d("", false)

// flatten out mappings: remove database index
var mappings = []
for(var db_index in mappings_temp){
    for(var target_table_name in mappings_temp[db_index])
        mappings[target_table_name] = mappings_temp[db_index][target_table_name]
}

/*
// to update mapping templates
for(var target_table in mappings){
    print_mapping_as_json(mappings[target_table])
}
quit()
*/

// load mapping template
var mapping_template = load_mapping_from_json("test/model-d-mapping-test-template.json")

// concat all  target tables and check number of rows
var no_rows = 0
for( var i in mappings){
    no_rows+=mappings[i].length
}
Assert(3330, no_rows, "total number of rows")

// there should be 87 target table tables
Assert( 270, Object.keys(mappings).length ,"number of target tables")

// there should be 45 for the main agent_todo
Assert( 44,Object.keys(mappings["agent_todo"]).length, "number of columns")

// iterate and compare template and mapping rows
// the mapping template is not assoc array but flat list
var j=0
for(var target_table in mappings){
    for(var i in mappings[target_table]){
        Assert(JSON.stringify(mapping_template[j]),JSON.stringify(mappings[target_table][i]), "compare rows")
        j++
    }
}
// otherwise ....
print("PASSED")