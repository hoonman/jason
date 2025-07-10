predefined_config = {
    'key_cols': ["cust_customer.id", "order_order_id"],
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