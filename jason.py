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

def parse_json_from_string(json_str: str):
    return json.loads(json_str)

def write_json(data, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, index=4)

# def flatten_json(data, parent_key): # let us assume the data is a single object
    # steps
    # given a python data, we need to output a python data that is flattened
    # step 1: go through the dictionary (data) 
    # initialize a new key name like this: parent_key + '_' + key
    # step 2: for each "value" check if it is a map or a list
    # if the value is a list, we will go through the lsit
    # for each item in the list, we will recurse --> flatten_json(new_dict, key)

    # if the value is not a list or dictionary: 
    # replace  


# people are saying it is going to involve comparing two json files and seeing any mismatch stuff. let's prioritize that instead of flattening the json first. 




returned = parse_json('nested-user-data.json')
print("returned: ", returned)


# exercise 1. given a simple JSON file, load the JSON from a file and string
# access nested values (get all the emails)
# handle missing keys gracefully
# compare two json objects ignoring order. 


# for nested json: we will traverse the json until the val is no longer an instance of a dict or a list
# assume the master element is always a dict ? 
# if val is a dict, then 
# if val is a list, we traverse that array in a for loop and call the function traverse_nested_json again

def traverse_nested_json(mapping: dict, wanted_attributes, arr):
    for key, val in mapping.items():
        if isinstance(val, list):
            print('called isinstance nested 1')
            for item in val: 
                traverse_nested_json(item, wanted_attributes, arr)
        elif isinstance(val, dict):
            print('called isinstance nested 2')
            traverse_nested_json(val, wanted_attributes, arr)
        else: # the value is finally not nested
            if key.lower() in wanted_attributes:
                arr.append(val)


json_obj = parse_json('json-file-1.json')
print("json obj: ", json_obj)
emails = []
traverse_nested_json(json_obj, {'email', 'contact'}, emails)
print("emails: ", emails)


# exercise 2. data normalization -- different formatting in files (emails, contact, name, fullname)
def normalize_user(user):
    return {
        "name": user.get("name") or user.get("fullName"),
        "email": user.get("email") or user.get("contact")
    }

# maybe a general normalization funct where we define the "alternatives" names for the columns, and the actual columns
def normalize(actual_names: list, alternate_names: dict, obj):
    normalized = {}
    for name in actual_names:
        value = obj.get(name)
        if value is None and name in alternate_names:
            for alternate in alternate_names[name]:
                value = obj.get(alternate)
                if value is None:
                    break
        normalized[name] = value
    return normalized

# usage case for normalize
actual_names = ["name", "email", "id"]
alternate_names = {
    "name": ["fullName"],
    "email": ["contact"],
    "id": ["userId"]
}

for i in range(len(json_obj["users"])):
    json_obj["users"][i] = normalize(actual_names, alternate_names, json_obj["users"][i])

print("normalized json: ", json_obj)


# handle different ID types (string vs. integer)
# let's handle them as strings. 
# when handling different ID types, we normalize the key names first, and then we will convert the value of each key if they are a different type
correct_types = {
    "name": str,
    "email": str,
    "id": str
}

print(json_obj)


def handle_types(correct_types, obj):
    for key, val in obj.items():
        if type(val) != correct_types[key]:
            obj[key] = correct_types[key](val)
    return obj

    

for i in range(len(json_obj["users"])):
    obj = handle_types(correct_types, json_obj["users"][i])
    json_obj["users"][i] = obj
    
print("handle different ID types ",json_obj)

# clean email formats. (case normalization whitespace stripping)
# 
def clean_email(email):
    if email is None:
        return

    cleaned_email = email.strip().lower()
    