import json
# exercise 1: flatten into a list of user-project pairs with total hours per user / project


# let's define a bunch of commonly used functions depending on the structure of the JSON file. 
# functions: 
# 1. parse json -> 
# input: file path 
# output: data structure with JSON content
# parses the json content into a python data structure either into a list or a object.

# 2. write_json
# input: python dictionary or list
# output: no returned value
# takes a python data structure and writes that content into a json file.

# 3. flatten json 
# input: python dictionary or list
# output: flattened data 
# 

def parse_json(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    if isinstance(data, dict):
        pass
    elif isinstance(data, list):
        pass
    return data

def write_json(data, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, index=4)


returned = parse_json('nested-user-data.json')
print("returned: ", returned)


        