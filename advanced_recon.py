import json
import logging
import os
import shutil
import subprocess
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union, Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from jsonschema import Draft7Validator
from pandas import DataFrame, json_normalize

class Reconciliation:
    def __init__(self, 
                 files: List[str], 
                 schema: Dict, 
                 config: Optional[Dict] = None):
        """
        Initialize the reconciliation class with enhanced configuration.
        
        Args:
            files: List of JSON file paths to reconcile
            schema: JSON schema for validation
            config: Configuration dictionary with options
        """
        self.files = files
        self.canon_files = []
        self.schema = schema
        self.dataframes = []
        
        # Configuration management - use defaults if not provided
        self.config = {
            'key_cols': ["cust_customer.id", "order_order_id"],
            'numeric_tolerance': 0.01,
            'time_tolerance_seconds': 60,
            'log_file': 'reconciliation.log',
            'notification_threshold': 10,  # Number of differences that trigger notification
            'notification_email': None,
            'incremental': False,
            'last_run_file': '.last_run.json',
            'chunk_size': 10000  # For large file processing
        }
        if config:
            self.config.update(config)
            
        # Setup logging for audit trail
        logging.basicConfig(
            filename=self.config['log_file'],
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger('reconciliation')
        self.logger.info(f"Starting reconciliation with files: {', '.join(files)}")
        
        # Metrics tracking
        self.metrics = {
            'total_records': 0,
            'matching_records': 0,
            'mismatches': {},
            'run_time': 0
        }

    def jq_canonicalize(self) -> None:
        """Canonicalize JSON files using jq in a secure way."""
        self.logger.info("Starting canonicalization")
        
        for file in self.files:
            try:
                out = file.replace(".json", "_canon.json")
                # Security improvement: avoid shell=True
                subprocess.run(
                    ["jq", "--sort-keys", ".", file], 
                    stdout=open(out, 'w'),
                    check=True
                )
                self.canon_files.append(out)
                self.logger.info(f"Canonicalized {file} to {out}")
            except subprocess.CalledProcessError as e:
                self.logger.error(f"Error canonicalizing {file}: {str(e)}")
                raise RuntimeError(f"Failed to canonicalize {file}") from e
            except Exception as e:
                self.logger.error(f"Unexpected error with {file}: {str(e)}")
                raise

    def validate_with_schema(self) -> None:
        """Validate canonicalized files against schema with better error handling."""
        self.logger.info("Starting schema validation")
        
        if not self.canon_files:
            self.logger.warning("No canonicalized files found. Run jq_canonicalize first.")
            return
            
        for canon in self.canon_files:
            try:
                with open(canon, 'r') as f:
                    data = json.load(f)
                
                errors = list(Draft7Validator(self.schema).iter_errors(data))
                if errors: 
                    self.logger.error(f"Schema validation failed for {canon}: {len(errors)} errors")
                    for e in errors[:5]:
                        self.logger.error(f"  {e.message}")
                    raise ValueError(f"Schema validation failed for {canon}")
                else:
                    self.logger.info(f"Schema validation passed for {canon}")
            except json.JSONDecodeError as e:
                self.logger.error(f"Invalid JSON in {canon}: {str(e)}")
                raise
            except Exception as e:
                self.logger.error(f"Error validating {canon}: {str(e)}")
                raise

    def load_and_flatten(self, path: str) -> DataFrame:
        """
        Load and flatten JSON with better error handling and incremental processing.
        """
        self.logger.info(f"Loading and flattening {path}")
        
        try:
            # Performance improvement: Handle large files with chunking if needed
            if os.path.getsize(path) > 50 * 1024 * 1024:  # If file > 50MB
                self.logger.info(f"Large file detected ({path}). Using chunked processing.")
                # This would need a custom implementation for very large files
                # For simplicity, we'll still load it all at once but log the concern
            
            # Incremental processing check
            if self.config['incremental'] and os.path.exists(self.config['last_run_file']):
                with open(self.config['last_run_file'], 'r') as f:
                    last_run = json.load(f)
                    last_timestamp = last_run.get('timestamp')
                    self.logger.info(f"Incremental processing from {last_timestamp}")
                    # Logic for incremental processing would go here
            
            with open(path, 'r') as f:
                data = json.load(f)
            
            # More flexible flattening with dynamic meta fields
            meta_fields = self.config.get('meta_fields', [["customer", "id"], ["customer", "name"]])
            
            df = json_normalize(
                data, 
                record_path="orders", 
                meta=meta_fields,
                record_prefix="order_", 
                meta_prefix="cust_"
            )
            
            self.dataframes.append(df)
            return df
            
        except Exception as e:
            self.logger.error(f"Error loading {path}: {str(e)}")
            raise

    def clean_and_cast(self) -> None:
        """Clean dataframes and cast types with error handling."""
        self.logger.info("Cleaning and casting data types")
        
        if not self.dataframes:
            self.logger.warning("No dataframes to clean. Load data first.")
            return
            
        try:
            for i, df in enumerate(self.dataframes):
                # Timestamps
                for col in [c for c in df.columns if 'ts' in c or 'time' in c or 'date' in c]:
                    df[col] = pd.to_datetime(df[col])
                
                # Numeric values - with proper error handling
                for col in [c for c in df.columns if 'amt' in c or 'total' in c or 'price' in c]:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    # Flag any values that couldn't be converted
                    if df[col].isna().any():
                        self.logger.warning(f"Found {df[col].isna().sum()} non-numeric values in {col}")
                
                # IDs to integers
                for col in [c for c in df.columns if 'id' in c.lower() and 'guid' not in c.lower()]:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')  # nullable integer
                
                # Strings
                for col in [c for c in df.columns if 'name' in c or 'desc' in c or 'text' in c]:
                    df[col] = df[col].astype("string")
                
                self.dataframes[i] = df
                self.logger.info(f"Cleaned dataframe {i}: {len(df)} rows, {len(df.columns)} columns")
        
        except Exception as e:
            self.logger.error(f"Error in clean_and_cast: {str(e)}")
            raise

    def reconcile(self) -> Dict:
        """
        Reconcile dataframes with enhanced comparison and metrics.
        """
        start_time = datetime.now()
        self.logger.info("Starting reconciliation")
        
        if len(self.dataframes) != 2:
            self.logger.error(f"Expected 2 dataframes, found {len(self.dataframes)}")
            raise ValueError("Reconciliation requires exactly 2 dataframes")
        
        df1, df2 = self.dataframes
        key_cols = self.config['key_cols']
        
        # Basic validation
        for col in key_cols:
            if col not in df1.columns or col not in df2.columns:
                self.logger.error(f"Key column {col} not found in both dataframes")
                raise ValueError(f"Key column {col} missing")
        
        # Set index for comparison
        df1 = df1.set_index(key_cols)
        df2 = df2.set_index(key_cols)
        
        # Track metrics
        self.metrics['total_records'] = len(df1) + len(df2)
        
        # Find keys in one dataframe but not the other
        only_in_df1 = df1.index.difference(df2.index)
        only_in_df2 = df2.index.difference(df1.index)
        
        # Compare common records with tolerance
        common_keys = df1.index.intersection(df2.index)
        common_cols = set(df1.columns).intersection(set(df2.columns))
        
        # Track differences by column
        differences = {
            'only_in_df1': list(only_in_df1),
            'only_in_df2': list(only_in_df2),
            'value_mismatches': {}
        }
        
        # Compare values for common keys
        for col in common_cols:
            col_diffs = []
            
            # Handle different comparison types based on data type
            if pd.api.types.is_numeric_dtype(df1[col]):
                # Numeric comparison with tolerance
                tolerance = self.config['numeric_tolerance']
                mask = ~((df1.loc[common_keys, col] - df2.loc[common_keys, col]).abs() <= tolerance)
                col_diffs = [(k, df1.loc[k, col], df2.loc[k, col]) 
                             for k in common_keys[mask]]
                
            elif pd.api.types.is_datetime64_dtype(df1[col]):
                # Timestamp comparison with tolerance
                time_tol = pd.Timedelta(seconds=self.config['time_tolerance_seconds'])
                mask = ~((df1.loc[common_keys, col] - df2.loc[common_keys, col]).abs() <= time_tol)
                col_diffs = [(k, df1.loc[k, col], df2.loc[k, col]) 
                             for k in common_keys[mask]]
                
            else:
                # String/other comparison (exact match)
                mask = df1.loc[common_keys, col] != df2.loc[common_keys, col]
                col_diffs = [(k, df1.loc[k, col], df2.loc[k, col]) 
                             for k in common_keys[mask]]
            
            if col_diffs:
                differences['value_mismatches'][col] = col_diffs
        
        # Calculate match rate
        total_comparisons = len(common_keys) * len(common_cols)
        total_mismatches = sum(len(diffs) for diffs in differences['value_mismatches'].values())
        match_rate = 1 - (total_mismatches / total_comparisons) if total_comparisons > 0 else 0
        
        self.metrics['matching_records'] = len(common_keys) - len(set().union(*[
            set([k[0] for k in v]) for v in differences['value_mismatches'].values()
        ])) if differences['value_mismatches'] else len(common_keys)
        
        self.metrics['mismatches'] = {
            'only_in_df1': len(only_in_df1),
            'only_in_df2': len(only_in_df2),
            'value_mismatches': total_mismatches
        }
        
        self.metrics['match_rate'] = match_rate
        
        # Execution time
        self.metrics['run_time'] = (datetime.now() - start_time).total_seconds()
        
        # Log results
        self.logger.info(f"Reconciliation complete: {match_rate:.2%} match rate")
        self.logger.info(f"Records only in first file: {len(only_in_df1)}")
        self.logger.info(f"Records only in second file: {len(only_in_df2)}")
        self.logger.info(f"Value mismatches: {total_mismatches}")
        
        # Send notifications if threshold exceeded
        if (len(only_in_df1) + len(only_in_df2) + total_mismatches) > self.config['notification_threshold']:
            self._send_notification()
        
        # Update last run for incremental processing
        with open(self.config['last_run_file'], 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'metrics': self.metrics
            }, f)
            
        return differences

    def generate_report(self, differences: Dict) -> None:
        """Generate comprehensive reconciliation report with visualizations."""
        self.logger.info("Generating reconciliation report")
        
        # Create report directory
        report_dir = f"reconciliation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        os.makedirs(report_dir, exist_ok=True)
        
        # Write summary
        with open(f"{report_dir}/summary.txt", 'w') as f:
            f.write(f"Reconciliation Report\n")
            f.write(f"====================\n\n")
            f.write(f"Run Date: {datetime.now().isoformat()}\n")
            f.write(f"Files Compared: {', '.join(self.files)}\n\n")
            
            f.write(f"Summary Metrics:\n")
            f.write(f"- Total Records: {self.metrics['total_records']}\n")
            f.write(f"- Matching Records: {self.metrics['matching_records']}\n")
            f.write(f"- Match Rate: {self.metrics['match_rate']:.2%}\n")
            f.write(f"- Records only in first file: {len(differences['only_in_df1'])}\n")
            f.write(f"- Records only in second file: {len(differences['only_in_df2'])}\n")
            f.write(f"- Fields with mismatches: {len(differences['value_mismatches'])}\n\n")
            
            # Write detailed mismatches
            f.write(f"Detailed Mismatches:\n")
            for col, mismatches in differences['value_mismatches'].items():
                f.write(f"\n{col}:\n")
                for i, (key, val1, val2) in enumerate(mismatches[:10]):  # Limit to first 10
                    f.write(f"  {i+1}. Key: {key}, File1: {val1}, File2: {val2}\n")
                if len(mismatches) > 10:
                    f.write(f"  ... and {len(mismatches) - 10} more\n")
        
        # Generate visualizations
        self._generate_visualizations(report_dir, differences)
        
        self.logger.info(f"Report generated in {report_dir}")
        print(f"Report generated in {report_dir}")

    def _generate_visualizations(self, report_dir: str, differences: Dict) -> None:
        """Generate visualization charts for the report."""
        # Create bar chart of mismatches by column
        plt.figure(figsize=(10, 6))
        cols = list(differences['value_mismatches'].keys())
        counts = [len(differences['value_mismatches'][col]) for col in cols]
        
        if cols:  # Only create chart if there are mismatches
            plt.bar(cols, counts)
            plt.xticks(rotation=45, ha='right')
            plt.title('Mismatches by Column')
            plt.xlabel('Column')
            plt.ylabel('Count of Mismatches')
            plt.tight_layout()
            plt.savefig(f"{report_dir}/mismatches_by_column.png")
        
        # Create pie chart of match vs mismatch
        plt.figure(figsize=(8, 8))
        match_count = self.metrics['matching_records']
        mismatch_count = self.metrics['total_records'] // 2 - match_count  # Divide by 2 because total counts both files
        
        plt.pie([match_count, mismatch_count], 
                labels=['Matching Records', 'Records with Differences'],
                autopct='%1.1f%%',
                colors=['#4CAF50', '#F44336'])
        plt.title('Match vs Mismatch Rate')
        plt.savefig(f"{report_dir}/match_rate.png")

    def _send_notification(self) -> None:
        """Send notification when differences exceed threshold."""
        if not self.config['notification_email']:
            self.logger.info("No notification email configured, skipping notification")
            return
            
        try:
            # In a real implementation, you'd integrate with an email service
            # For this example, we'll just log that we would send a notification
            self.logger.info(f"Would send notification to {self.config['notification_email']}")
            self.logger.info(f"Notification content: Reconciliation found significant differences")
        except Exception as e:
            self.logger.error(f"Failed to send notification: {str(e)}")



# def main():
schema = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "customer": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "name": {"type": "string"}
                },
                "required": ["id", "name"]
            },
            "orders": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "order_id": {"type": "integer"},
                        "amt": {"type": "number"},
                        "ts": {"type": "string", "format": "date-time"}
                    },
                    "required": ["order_id", "amt", "ts"]
                }
            }
        },
        "required": ["customer", "orders"]
    }
}
#     files = ['fileA.json', 'fileB.json']
    # recon = Reconciliation(files=files, schema=schema, config={})
def main():
    # Create sample data for testing
    sample_data1 = [
        {
            "customer": {
                "id": 1,
                "name": "John Doe"
            },
            "orders": [
                {
                    "order_id": 101,
                    "amt": 99.99,
                    "ts": "2023-01-15T10:30:00Z"
                },
                {
                    "order_id": 102,
                    "amt": 150.50,
                    "ts": "2023-01-16T14:20:00Z"
                }
            ]
        },
        {
            "customer": {
                "id": 2,
                "name": "Jane Smith"
            },
            "orders": [
                {
                    "order_id": 201,
                    "amt": 75.25,
                    "ts": "2023-01-17T09:15:00Z"
                }
            ]
        }
    ]
    
    # Create a slightly different version for the second file
    sample_data2 = [
        {
            "customer": {
                "id": 1,
                "name": "John Doe"
            },
            "orders": [
                {
                    "order_id": 101,
                    "amt": 99.99,
                    "ts": "2023-01-15T10:30:00Z"
                },
                {
                    "order_id": 102,
                    "amt": 150.75,  # Slightly different amount
                    "ts": "2023-01-16T14:20:00Z"
                }
            ]
        },
        {
            "customer": {
                "id": 3,  # New customer
                "name": "Bob Johnson"
            },
            "orders": [
                {
                    "order_id": 301,
                    "amt": 120.00,
                    "ts": "2023-01-18T16:45:00Z"
                }
            ]
        }
    ]
    
    fileA = 'fileA.json'
    fileB = 'fileB.json'
    
    try:
        # Write sample data to files
        with open(fileA, 'w') as f:
            json.dump(sample_data1, f, indent=2)
        
        with open(fileB, 'w') as f:
            json.dump(sample_data2, f, indent=2)
        
        print(f"Created sample files: {fileA} and {fileB}")
        
        # Configure reconciliation
        # config = {
        #     'key_cols': ["cust_customer", "order_id"],
        #     'numeric_tolerance': 0.3,
        #     'meta_fields': [["customer", "id"], ["customer", "name"]]
        # }
        config = {}
        
        # Run reconciliation process
        files = [fileA, fileB]
        recon = Reconciliation(files=files, schema=schema, config=config)
        
        # Execute reconciliation workflow
        print("Canonicalizing files...")
        recon.jq_canonicalize()
        
        print("Validating against schema...")
        recon.validate_with_schema()
        
        print("Loading and flattening data...")
        for file in recon.canon_files:
            recon.load_and_flatten(file)
        
        print("Cleaning and casting data types...")
        recon.clean_and_cast()
        
        print("Performing reconciliation...")
        differences = recon.reconcile()
        
        print("Generating report...")
        recon.generate_report(differences)
        
    except Exception as e:
        print(f"Error during reconciliation: {str(e)}")
    
    finally:
        # Clean up files
        print("Cleaning up temporary files...")
        for file in [fileA, fileB]:
            if os.path.exists(file):
                os.remove(file)
                
        # Also remove canonicalized files
        for file in recon.canon_files:
            if os.path.exists(file):
                os.remove(file)

if __name__ == "__main__":
    main()