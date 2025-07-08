import unittest
from jason import Jason
import json

class TestJason(unittest.TestCase):
    def setUp(self):
        file_paths = ['/Users/jaehsong/Documents/mercury/jason/json_files/json-file-1.json', 
                      '/Users/jaehsong/Documents/mercury/jason/json_files/nested-user-data.json']
        self.jason = Jason(file_paths)
    
    def test_clean_email(self):
        """Test email cleaning functionality"""
        test_cases = [
            ('john.doe@example.com', 'john.doe@example.com'),         # Valid
            ('JOHN.DOE@EXAMPLE.COM', 'john.doe@example.com'),         # Uppercase
            ('  user@domain.com  ', 'user@domain.com'),               # Whitespace
            ('user+filter@gmail.com', 'user@gmail.com'),              # + filter
            ('johndoeexample.com', None),                             # Missing @
            ('john.doe@', None),                                      # Missing domain
            ('@example.com', None),                                   # Missing username
            ('john#doe@example.com', None),                           # Invalid character
            ('john.doe@example.c', None),                             # TLD too short
            ('john@doe@example.com', None),                           # Multiple @ symbols
            ('', None),                                               # Empty string
            (None, None)                                              # None value
        ]
        
        for input_email, expected in test_cases:
            with self.subTest(input_email=input_email):
                self.assertEqual(self.jason.clean_email(input_email), expected)
    
    def test_parse_name(self):
        """Test name parsing functionality"""
        test_cases = [
            # Standard format
            ('John Smith', {
                'first_name': 'John', 'last_name': 'Smith', 
                'middle_name': None, 'title': None, 'suffix': None
            }),
            # Last name first
            ('Smith, John', {
                'first_name': 'John', 'last_name': 'Smith', 
                'middle_name': None, 'title': None, 'suffix': None
            }),
            # With title
            ('Dr. John Smith', {
                'first_name': 'John', 'last_name': 'Smith', 
                'middle_name': None, 'title': 'Dr.', 'suffix': None
            }),
            # With suffix
            ('John Smith Jr.', {
                'first_name': 'John', 'last_name': 'Smith', 
                'middle_name': None, 'title': None, 'suffix': 'Jr.'
            }),
            # With middle name
            ('John Robert Smith', {
                'first_name': 'John', 'last_name': 'Smith', 
                'middle_name': 'Robert', 'title': None, 'suffix': None
            }),
            # Complex case
            # ('Smith, Dr. John Robert, PhD', {
            #     'first_name': 'John', 'last_name': 'Smith', 
            #     'middle_name': 'Robert', 'title': 'Dr.', 'suffix': 'PhD'
            # }),
            # Empty and None
            ('', {
                'first_name': None, 'last_name': None, 
                'middle_name': None, 'title': None, 'suffix': None
            }),
            (None, {
                'first_name': None, 'last_name': None, 
                'middle_name': None, 'title': None, 'suffix': None
            })
        ]
        
        for input_name, expected in test_cases:
            with self.subTest(input_name=input_name):
                result = self.jason.parse_name(input_name)
                self.assertEqual(result, expected)
    
    def test_normalize(self):
        """Test normalization function"""
        # Test data
        obj = {"name": "John", "full_name": "John Smith", "email_address": "john@example.com"}
        actual_names = ["name", "email"]
        alternate_names = {"name": ["full_name"], "email": ["email_address"]}
        
        expected = {"name": "John", "email": "john@example.com"}
        result = self.jason.normalize(actual_names, alternate_names, obj)
        
        self.assertEqual(result, expected)
    
    def test_flatten_json(self):
        """Test JSON flattening functionality"""
        nested_json = {
            "user": {
                "name": "John",
                "contact": {
                    "email": "john@example.com",
                    "phone": "123-456-7890"
                }
            },
            "projects": [
                {"title": "Project 1", "hours": 10},
                {"title": "Project 2", "hours": 20}
            ]
        }
        
        expected = {
            "user_name": "John",
            "user_contact_email": "john@example.com",
            "user_contact_phone": "123-456-7890",
            "projects_0_title": "Project 1",
            "projects_0_hours": 10,
            "projects_1_title": "Project 2",
            "projects_1_hours": 20
        }
        
        flattened = self.jason.flatten_json(nested_json)
        self.assertEqual(flattened, expected)
    
    def test_compare_json(self):
        """Test JSON comparison functionality"""
        # Identical JSONs
        json1 = {"name": "John", "age": 30}
        json2 = {"name": "John", "age": 30}
        self.assertEqual(self.jason.compare_json(json1, json2), {})
        
        # Different values
        json1 = {"name": "John", "age": 30}
        json2 = {"name": "John", "age": 31}
        expected = {"age": {"value_mismatch": {"json1_value": 30, "json2_value": 31}}}
        self.assertEqual(self.jason.compare_json(json1, json2), expected)
        
        # Missing keys
        json1 = {"name": "John", "age": 30}
        json2 = {"name": "John"}
        expected = {"age": {"missing_in_json2": 30}}
        self.assertEqual(self.jason.compare_json(json1, json2), expected)
        
        # Nested differences
        json1 = {"user": {"name": "John", "age": 30}}
        json2 = {"user": {"name": "John", "age": 31}}
        expected = {"user": {"age": {"value_mismatch": {"json1_value": 30, "json2_value": 31}}}}
        self.assertEqual(self.jason.compare_json(json1, json2), expected)

if __name__ == '__main__':
    unittest.main()