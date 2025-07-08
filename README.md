# JASON: Data Normalization Service
a service that normalizes json files

## Features

## Things to consider while building Jason

Normalizations
* field names. the key values may be different. ex) "name" or "fullName" we should handle these so that all the columns representing 'name' is the same.
* handle different types. some ID might have type string or some might have int.
* flattening nested structures. some fields might have structures that are nested unnecessarily. we should either flatten or collapse depending on the situation. this should be two separate functions 1) flatten_json, 2) unflatten_json

Data Reconciliation
* after normalizing our json data, we can also use panda's normalization functions
* 

Performance
* make sure to take advantage of concurrency and parallelism when we can.
* indexing certain columns can help


## The standard approach to Data Reconciliation 
1. format json: remove whitespaces, order by keys
2. validate against JSON schema
3. normalize using json_normalize() from pandas
4. check correct data type
5. identify correct keys (eg. name vs. fullName)
6. outer merge & flag differences
7. summarize those differences
8. automate & schedule

## Going beyond
1. create a UI application into interactive app
2. fuzzy matching , levenshtein matching for keys or names
3. use Ollama for data difference summary. since data recon services are always dependent upon the data, we can also make a less-accurate data recon service that can handle all types of data.

