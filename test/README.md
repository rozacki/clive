# tests run against specific schema that has to be loaded into mongo
# the schema is in test folder
# to load test schema into mongo:
$ mongoimport -d clive -c schema-test test/schema-test.json

# in toolkit.js change var schema_collection_name = "schema-test"
# in geberate_model_Z.ja comment "generate_model_X(method, print_out)"

# to run tests invoke
$ cd test
$ mongo test-model-d.js
$ mongo test-model-a.js

## troubleshooting
to enable debug mode that provides more debug info, in toolkit.js set var debug_on = true
