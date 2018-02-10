load("toolkit.js")
load("generate_mapping.js")

// Author Chris Rozacki

// Generates snowflake schema-like from single collection
// Individual 'types' are loaded in individual tables because:
// - single table would be too wide for HIVE to cope with
// - one column can have multiple types

var collection_to_schema_mapping= {'databases':[
                                    {'database_name': 'agent-core', 'collections': [
                                      { 'source_collection_name' : 'agentToDo'
                                      , 'break_on_one_of' : 'urn:jsonschema:uk:gov:dwp:universe:agent:todo:AgentToDoProperties'
                                      , 'stop_on_one_of' : ["urn:jsonschema:uk:gov:dwp:universe:declaration:BaseDeclaration"]
                                      , 'schema' : "uk.gov.dwp.universe.agent.todo.AgentToDo.json"
                                      , 'target_table_name' : 'agent_todo'
                                      , 'id' : 'urn:jsonschema:uk:gov:dwp:universe:agent:todo:AgentToDo'
                                      , 'get_one_of_function': get_oneOf_agenttodo
                                      , 'select_on_one_of': ''
                                      , 'where_predicate_path' : '`toDoProperties`.`type`'
                                      }
                                    ,
                                      { 'source_collection_name' : 'agentToDoArchive'
                                      , 'break_on_one_of' : 'urn:jsonschema:uk:gov:dwp:universe:agent:todo:AgentToDoProperties'
                                      , 'stop_on_one_of' : ["urn:jsonschema:uk:gov:dwp:universe:declaration:BaseDeclaration"]
                                      , 'schema' : "uk.gov.dwp.universe.agent.todo.AgentToDo.json"
                                      , 'target_table_name' : 'agent_todo_archive'
                                      , 'id' : 'urn:jsonschema:uk:gov:dwp:universe:agent:todo:AgentToDo'
                                      , 'get_one_of_function': get_oneOf_agenttodo
                                      , 'select_on_one_of': ''
                                      , 'where_predicate_path' : '`toDoProperties`.`type`'
                                      }
                                    ]},
                                    {'database_name': 'core'
                                    , 'collections': [
                                      { 'source_collection_name' : 'journal'
                                      , 'break_on_one_of' : 'urn:jsonschema:uk:gov:dwp:universe:journal:JournalEntryData'
                                      , 'stop_on_one_of' : ["urn:jsonschema:uk:gov:dwp:universe:declaration:Summary"]
                                      , 'schema' : "uk.gov.dwp.universe.journal.JournalEntry.json"
                                      , 'target_table_name' : 'journal'
                                      , 'id' : 'urn:jsonschema:uk:gov:dwp:universe:journal:JournalEntry'
                                      , 'get_one_of_function': get_oneOf_journal
                                      , 'select_on_one_of' : ''
                                      , 'where_predicate_path' : '`journalEntryProperties`.`type`'
                                      }
                                    ,
                                      { 'source_collection_name' : 'toDo'
                                      , 'break_on_one_of' : ["urn:jsonschema:uk:gov:dwp:universe:todo:ClaimantToDoProperties"]
                                      , 'stop_on_one_of' : ''
                                      , 'schema' : "uk.gov.dwp.universe.todo.ClaimantToDo.json"
                                      , 'target_table_name' : 'todo'
                                      , 'id' : 'urn:jsonschema:uk:gov:dwp:universe:todo:ClaimantToDo'
                                      , 'get_one_of_function': get_oneOf_core_todo
                                      , 'select_on_one_of': ''
                                      , 'where_predicate_path' : '`properties`.`type`'
                                      }
                                    ,
                                      { 'source_collection_name' : 'claimantCommitment'
                                      , 'break_on_one_of' : ["urn:jsonschema:uk:gov:dwp:universe:commitment:ClaimantCommitmentProperties"]
                                      , 'stop_on_one_of' : ''
                                      , 'schema' : "uk.gov.dwp.universe.commitment.ClaimantCommitment"
                                      , 'target_table_name' : 'claimant_commitment'
                                      , 'id' : 'urn:jsonschema:uk:gov:dwp:universe:commitment:ClaimantCommitment'
                                      , 'get_one_of_function': get_oneOf_core_claimant_commitment
                                      , 'select_on_one_of': ''
                                      , 'where_predicate_path' : '`properties`.`type`'
                                      }
                                    ]}
                                  ]}
// BEGIN: core-claimant commitment
// used by map_oneOf_to_type()
function get_oneOf_core_claimant_commitment(schema){
    return schema["properties"]["properties"]["oneOf"]
}
// END: core-claimant commitment

// BEGIN: core-todo
// used by map_oneOf_to_type()
function get_oneOf_core_todo(schema){
    return schema["properties"]["properties"]["oneOf"]
}

// END: core-todo

//BEGIN : core-journal
// used by map_oneOf_to_type()
function get_oneOf_journal(schema){
    return schema["properties"]["journalEntryProperties"]["oneOf"]
}
// END : core-journal

// BEGIN : acceptedata earningsdata
function get_oneOf_earningsdata(schema){
    return schema["properties"]["claimElement"]["oneOf"]
}

// END : acceptedata earningsdata

// BEGIN : agenttodo and agenttodoarchive
// returns map of
function get_oneOf_agenttodo(schema){
    return schema["properties"]["toDoProperties"]["oneOf"]
}
// END : agenttodo and agenttodoarchive

// returns  array  { index  = {"target table name" : array of technical mapping rows}}
function generate_model_d(method, print_out){
    var sql_where_list = []
    var mappings = []
    var header = true
    //var collection_to_schema_mapping = JSON.parse(cat("config/model-d.json")
    for(var database_index in collection_to_schema_mapping.databases){
        var source_database = collection_to_schema_mapping.databases[database_index].database_name
        debug("source database" + source_database)
        for(var collection_index in collection_to_schema_mapping.databases[database_index].collections){
            var collection = collection_to_schema_mapping.databases[database_index].collections[collection_index]
            var disabled = collection.hasOwnProperty('disabled')?true:false
            var source_collection_name = collection.source_collection_name
            var target_table_name = collection.target_table_name
            var break_on_oneOf_id = collection.break_on_one_of
            var stop_on_oneOf_id = collection.stop_on_one_of
            var schema = collection.schema
            var id = collection.id
            var get_oneOf_function = collection.get_one_of_function
            var select_oneOf_ref = collection.select_on_one_of
            var where_predicate_path = collection.where_predicate_path

            debug("source collection " + source_collection_name)

            if(disabled)
                continue
            // disable during test
            if(method == "where"){
                //var where_clause = generate_sql_where_function(map_oneOf_to_type(id, target_table_name, get_oneOf_function))
                var where_clause = generate_sql_where_generic(map_oneOf_to_type(id, target_table_name, get_oneOf_function), where_predicate_path)
                sql_where_list.push(where_clause)
                if(print_out)
                    print_dictionary_comma_separated(where_clause)
            }else{
                var mapping = get_mapping(schema, break_on_oneOf_id, stop_on_oneOf_id, target_table_name, source_collection_name, select_oneOf_ref)
                if(print_out)
                    print_mapping(mapping, source_database, source_collection_name, header)
                header = false
                mappings.push(mapping)
            }
        }
    }

    if(method == "where")
        return sql_where_list
    return mappings
}
generate_model_d(method, print_out)