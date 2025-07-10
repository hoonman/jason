import json
import re

# people are saying it is going to involve comparing two json files and seeing any mismatch stuff. let's prioritize that instead of flattening the json first. 



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

    def clean_email(self, email):
        if email is None:
            return None
        cleaned_email = email.strip().lower()
        cleaned_email = re.sub(r'(\+[^@]*)@', '@', cleaned_email)
        if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', cleaned_email):
            return None
        return cleaned_email

    def parse_name(self, name):
        '''Parse different name formats and returns a standardized dictionary.'''
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
            # Last name first format: "Smith, John"
            parts = [p.strip() for p in name.split(',', 1)]
            result['last_name'] = parts[0]
            
            # Handle remaining parts
            remaining = parts[1].strip()
            
            if ',' in remaining:
                name_part, suffix_part = [p.strip() for p in remaining.split(',', 1)]
                remaining = name_part
                if suffix_part.lower().replace('.', '') in suffixes:
                    result['suffix'] = suffix_part
            
            name_parts = remaining.split()
            if name_parts:
                result['first_name'] = name_parts[0]
                if len(name_parts) > 1:
                    result['middle_name'] = ' '.join(name_parts[1:])
        else:
            # Standard format: "John Smith"
            parts = name.split()
            
            if parts and parts[0].lower().replace('.', '') in titles:
                result['title'] = parts[0]
                parts = parts[1:]
            
            if parts and parts[-1].lower().replace('.', '') in suffixes:
                result['suffix'] = parts[-1]
                parts = parts[:-1]
            elif len(parts) >= 2 and ',' in parts[-1]:
                last_part = parts[-1]
                if ',' in last_part:
                    name_part, suffix = last_part.split(',', 1)
                    if suffix.strip().lower().replace('.', '') in suffixes:
                        result['suffix'] = suffix.strip()
                        parts[-1] = name_part
            
            if len(parts) == 1:
                result['first_name'] = parts[0]
            elif len(parts) == 2:
                result['first_name'] = parts[0]
                result['last_name'] = parts[1]
            elif len(parts) >= 3:
                result['first_name'] = parts[0]
                result['last_name'] = parts[-1]
                result['middle_name'] = ' '.join(parts[1:-1])
                
        return result

    def flatten_json(self, data, parent_key=''):
        """Flatten a nested JSON object into a single level dictionary."""
        flattened = {}
        
        if isinstance(data, dict):
            for key, value in data.items():
                new_key = f"{parent_key}_{key}" if parent_key else key
                if isinstance(value, (dict, list)):
                    flattened.update(self.flatten_json(value, new_key))
                else:
                    flattened[new_key] = value
        elif isinstance(data, list):
            for i, item in enumerate(data):
                new_key = f"{parent_key}_{i}" if parent_key else str(i)
                if isinstance(item, (dict, list)):
                    flattened.update(self.flatten_json(item, new_key))
                else:
                    flattened[new_key] = item
        else:
            flattened[parent_key] = data
            
        return flattened

    def compare_json(self, json1, json2, ignore_order=True):
        """Compare two JSON objects and return a report of differences."""
        if type(json1) != type(json2):
            return {'type_mismatch': {'json1_type': type(json1).__name__, 'json2_type': type(json2).__name__}}
        
        if isinstance(json1, dict):
            all_keys = set(json1.keys()) | set(json2.keys())
            differences = {}
            
            for key in all_keys:
                if key not in json1:
                    differences[key] = {'missing_in_json1': json2[key]}
                elif key not in json2:
                    differences[key] = {'missing_in_json2': json1[key]}
                else:
                    sub_diff = self.compare_json(json1[key], json2[key], ignore_order)
                    if sub_diff:
                        differences[key] = sub_diff
            
            return differences if differences else {}
        
        elif isinstance(json1, list):
            if ignore_order:
                try:
                    sorted1 = sorted(json1)
                    sorted2 = sorted(json2)
                    if sorted1 == sorted2:
                        return {}
                except:
                    pass
            
            if len(json1) != len(json2):
                return {'length_mismatch': {'json1_length': len(json1), 'json2_length': len(json2)}}
            
            differences = {}
            for i, (item1, item2) in enumerate(zip(json1, json2)):
                sub_diff = self.compare_json(item1, item2, ignore_order)
                if sub_diff:
                    differences[i] = sub_diff
            
            return differences if differences else {}
        
        else:
            if json1 != json2:
                return {'value_mismatch': {'json1_value': json1, 'json2_value': json2}}
            return {}