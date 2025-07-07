import json
import re
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






# exercise 1. given a simple JSON file, load the JSON from a file and string
# access nested values (get all the emails)
# handle missing keys gracefully
# compare two json objects ignoring order. 


# for nested json: we will traverse the json until the val is no longer an instance of a dict or a list
# assume the master element is always a dict ? 
# if val is a dict, then 
# if val is a list, we traverse that array in a for loop and call the function traverse_nested_json again




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



    

for i in range(len(json_obj["users"])):
    obj = handle_types(correct_types, json_obj["users"][i])
    json_obj["users"][i] = obj
    
print("handle different ID types ",json_obj)

# clean email formats. (case normalization whitespace stripping)
# 
def clean_email(email):
    if email is None:
        return None
    cleaned_email = email.strip().lower()  # strips whitespace and lowercases the email addresses first
    cleaned_email = re.sub(r'(\+[^@]*)@', '@', cleaned_email)  # remove everything between + and @ in local part
    if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', cleaned_email):
        # the regexp
        # ^ beginning
        # [] -> a-z lowercase and uppercase 0-9, ., -, % + - all allowed. 
        # @ matches the at symbol
        # [] -> a-z lowercase and uppercase 0-9, . - 
        # . 
        # com, org, io, etc
        return None  # Or raise an exception
    
    return cleaned_email


# Test emails - mix of valid and invalid formats
emails = [
    'john.doe@example.com',                # Valid
    'JOHN.DOE@EXAMPLE.COM',                # Valid but uppercase
    '  user@domain.com  ',                 # Valid with whitespace
    'user+filter@gmail.com',               # Valid with + filter (will be cleaned)
    'johndoeexample.com',                  # Invalid - missing @
    'john.doe@',                           # Invalid - missing domain
    '@example.com',                        # Invalid - missing username
    'john#doe@example.com',                # Invalid - invalid character #
    'john.doe@example.c',                  # Invalid - TLD too short
    'john@doe@example.com',                # Invalid - multiple @ symbols
    '',                                    # Invalid - empty string
    None                                   # Invalid - None value
]

# Test the clean_email function
print("Testing email cleaning function:")
for email in emails:
    cleaned = clean_email(email)
    status = "Valid" if cleaned else "Invalid"
    print(f"Original: {email} → Cleaned: {cleaned} → {status}")


# parsing different names 
# ex) "John Smith", vs "Smith, John"
# i think we should do this case by case. 
# so let's assume if it is in the format of Smith, John I will assume that this is in the format of lastName, firstName 
# maybe there arem multiple format of names 
# one rule that is always true: names will always have a first and a last name separated by a whitespace. 
# 1. split the name string into two variables.
# 2. if the first half has nothing in it, (no comma, no nothing)
# 3. if the first half has some things, 

def parse_name(name):
    '''
    parse different name formats and returns a standardized dictionary. string -> dict
    '''

    result = {
        'first_name': None,
        'last_name': None,
        'middle_name': None,
        'title': None,
        'suffix': None,
    }
    if not name or not isinstance(name, str):
        return result
    name = ' '.join(name.strip().split())
    titles = ['mr', 'mrs', 'miss', 'dr', 'prof', 'rev', 'hon']

    suffixes = ['jr', 'sr', 'i', 'ii','iii', 'iv', 'v', 'phd', 'md', 'esq']
    if ',' in name:
        parts = [p.strip() for p in name.split(',', 1)]
        result['last_name'] = parts[0]
        
        # Handle remaining parts (first name, middle names, suffixes)
        remaining = parts[1].strip()
        
        # Check for professional suffix in second part (e.g., "Smith, John, PhD")
        if ',' in remaining:
            name_part, suffix_part = [p.strip() for p in remaining.split(',', 1)]
            remaining = name_part
            if suffix_part.lower().replace('.', '') in suffixes:
                result['suffix'] = suffix_part
        
        # Process first/middle from remaining
        name_parts = remaining.split()
        if name_parts:
            result['first_name'] = name_parts[0]
            if len(name_parts) > 1:
                result['middle_name'] = ' '.join(name_parts[1:])
    else:
        # Standard format: "[Title] First [Middle] Last [Suffix]"
        parts = name.split()
        
        # Handle title
        if parts and parts[0].lower().replace('.', '') in titles:
            result['title'] = parts[0]
            parts = parts[1:]
        
        # Handle suffix (at the end)
        if parts and parts[-1].lower().replace('.', '') in suffixes:
            result['suffix'] = parts[-1]
            parts = parts[:-1]
        # Also check for comma suffix format "John Doe, PhD"
        elif len(parts) >= 2 and ',' in parts[-1]:
            last_part = parts[-1]
            if ',' in last_part:
                name_part, suffix = last_part.split(',', 1)
                if suffix.strip().lower().replace('.', '') in suffixes:
                    result['suffix'] = suffix.strip()
                    parts[-1] = name_part
        
        # Now handle first, middle, last
        if len(parts) == 1:
            # Single name
            result['first_name'] = parts[0]
        elif len(parts) == 2:
            # First and last only
            result['first_name'] = parts[0]
            result['last_name'] = parts[1]
        elif len(parts) >= 3:
            # First, middle, and last
            result['first_name'] = parts[0]
            result['last_name'] = parts[-1]
            result['middle_name'] = ' '.join(parts[1])
    return result



# i think a good way to approach this interview
# build a really good pre-processor / parser that will normalize all the data by parsing, cleaning, etc
# after the data has been normalized, we can compare. 
# when normalizing, we can implement matching functions, comparing names with levenshtein distance, handle partial matches with priority rules
# if any are still different, we can build a report of that

# when performing the matching algorithm to see if two json are correct, we will optimize matching for many data and 
# implement indexing strategies for fast lookups

# considering edges cases 
# missing records, detect duplicate entries, resolve conflicting information, process malformed json gracefully 
# design data reconciliation service
# create a diff engine architecture 
# implement streaming parser for huge files 


# let's first implement a json parsing class that will handle a lot of the parsing and normalization logic for us.
# this "Jason" class will be a data normalizer. Might as well be named JsonNormalizer
# includes a bunch of utility classes as well as normalization functions like "normalize" def normalize 
# we can either use concurrency or parallelism while parsing two different files (maybe it is a function that will call the different tool functions we have)
class Jason:
    def __init__(self, file_paths):
        self.file_paths = file_paths

    def parse_json_from_file(self, file_path):
        with open(file_path, 'r') as f:
            data = json.load(f)
        if isinstance(data, dict):
            pass
        elif isinstance(data, list):
            pass
        return data

    def parse_json_from_string(self, json_str: str):
        return json.loads(json_str)

    def write_json_to_file(self, data, filename):
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, index=4)

    def traverse_nested_json(self, mapping: dict, wanted_attributes, arr):
        for key, val in mapping.items():
            if isinstance(val, list):
                print('called isinstance nested 1')
                for item in val: 
                    self.traverse_nested_json(item, wanted_attributes, arr)
            elif isinstance(val, dict):
                print('called isinstance nested 2')
                self.traverse_nested_json(val, wanted_attributes, arr)
            else: # the value is finally not nested
                if key.lower() in wanted_attributes:
                    arr.append(val)

    def normalize_user(self, user):
        return {
            "name": user.get("name") or user.get("fullName"),
            "email": user.get("email") or user.get("contact")
        }

# maybe a general normalization funct where we define the "alternatives" names for the columns, and the actual columns
    def normalize(self, actual_names: list, alternate_names: dict, obj):
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

    def handle_types(correct_types, obj):
        for key, val in obj.items():
            if type(val) != correct_types[key]:
                obj[key] = correct_types[key](val)
        return obj
