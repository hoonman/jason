predefined_config = {
    'key_cols': ["id", "first_name", "last_name", "email", "routing_number", "created_on"],
    'numeric_tolerance': 0.01,
    'time_tolerance_seconds': 60,
    'log_file': 'reconciliation.log',
    'notification_threshold': 10,  
    'notification_email': None,
    'incremental': False,
    'last_run_file': '.last_run.json',
    'chunk_size': 10000  
}


predefined_metrics = {
    'total_records': 0,
    'matching_records': 0,
    'mismatches': {},
    'run_time': 0
}

predefined_schema = {
    "type": "object",
    "properties": {
        "customers": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "first_name": {"type": "string"},
                    "last_name": {"type": "string"},
                    "email": {"type": "string"},
                    "account_number" : {"type": "string"},
                    "account_number_masked" : {"type": "string"},
                    "routing_number" : {"type": "string"},
                    "created_on": {"type": "string", "format": "date-time"}
                }
            }
        }
    }
}