# JASON: Data Normalization Service
a service that normalizes json files

## Features

## Things to consider while building Jason

Normalizations
* field names. the key values may be different. ex) "name" or "fullName" we should handle these so that all the columns representing 'name' is the same.
* handle different types. some ID might have type string or some might have int.
* flattening nested structures. some fields might have structures that are nested unnecessarily. we should either flatten or collapse depending on the situation. this should be two separate functions 1) flatten_json, 2) unflatten_json


Performance
* make sure to take advantage of concurrency and parallelism when we can.
* indexing certain columns can help



