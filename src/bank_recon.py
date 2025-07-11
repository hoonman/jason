import json
import os
import sys
import logging
import subprocess
from datetime import datetime
import pandas as pd
from typing import Dict, List, Optional, Tuple, Union, Any
from config import predefined_config, predefined_metrics
from pandas import DataFrame, json_normalize

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# Append the `pyscripts` directory to sys.path
pyscripts_dir = os.path.join(parent_dir, 'pyscripts')
sys.path.append(pyscripts_dir)

from jason import parse_name


# for reconciliation for this specific file structure, we must first load the file into dataframes. flatten the structure, normalize fields, canonicalize, validate with schema.

# step 1: load the json into dataframes and print them out and verify that they were loaded
# step 2: flatten the structure by performing json_normalize
# step 3: schema alignment --> rename the columns, data types 
# step 4: schema validation with our predefined schema (optional but since we already have dataframes, we will skip)
# step 5: reconcile (compare)
# step 6: create a report


class BankRecon:
    def __init__(self, files: List[str], schema: Dict, custom_config: Optional[Dict] = None):
        self.files = files
        self.canon_files = []
        self.schema = schema
        self.dataframes = []

        if not custom_config:
            self.config = predefined_config
        else:
            self.config.update(custom_config)

        logging.basicConfig(filename=self.config['log_file'], level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger('reconciliation')
        self.logger.info(f"Starting reconciliation with files: {', '.join(files)}")

        self.metrics = predefined_metrics

    def jq_canonicalize(self) -> None:
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
                raise RuntimeError(f"Failed to canononicalize {file}") from e
            except Exception as e:
                self.logger.error(f"Unexpected error with {file}: {str(e)}")
                raise

    def validate_with_schema(self) -> None:
        return

    def load_and_flatten(self, path: str) -> DataFrame:
        print("lf called")
        try:
            with open(path, 'r') as f:
                data = json.load(f)
            
            # turn into dataframe
            meta_fields = self.config.get('meta_fields', [["name", "given"], ["contact", "email"], ["bankDetails", "accNumMasked"], ["bankDetails", "rtgNum"]])

            df = json_normalize(
                data,
                record_path=["customers"], # which LIST becomes a column? 
                record_prefix=""
            )
            self.dataframes.append(df)
            return df
        except Exception as e:
            self.logger.info(f"error in load_and_flatten: {e}")
        return 

    def normalize_legacy(self, df):
        out = pd.DataFrame({
            "id": df["customerId"].astype(str),
            "first_name": df["firstName"].str.strip().str.lower(),
            "last_name": df["lastName"].str.strip().str.lower(),
            "email": df["email"].str.strip().str.lower(),
            # "account_number": df["accountNumber"].astype(str),
            # "account_number_masked": [None for _ in df["customerId"]],
            "routing_number": df["routingNumber"].astype(str),
            "created_on": pd.to_datetime(df["signupDate"]).dt.date,
        })
        self.dataframes[1] = out
        return out
    
    def normalize_core(self, df):
        # normalize the names first
        names = df["name.given"]
        first_names = []
        last_names = []
        for i in range(len(names)):
            result = parse_name(names[i])
            first_names.append(result['first_name'].strip().lower())
            last_names.append(result['last_name'].strip().lower())

        out = pd.DataFrame({
            "id": df["id"].astype(str),
            "first_name": first_names,
            "last_name": last_names,
            "email": df["contact.email"].str.strip().str.lower(),
            # "account_number": [None for _ in df["id"]],
            # "account_number_masked": df["bankDetails.acctNumMasked"].str.strip().str.lower(),
            "routing_number": df["bankDetails.rtgNum"].str.strip().str.lower(),
            "created_on": pd.to_datetime(df["createdAt"]).dt.date
        })
        self.dataframes[0] = out
        return out

    def get_dataframes(self):
        return self.dataframes


    def clean_and_cast(self) -> None:
        return

    def reconcile(self) -> Dict:
        start_time = datetime.now()
        print(f"Starting reconciliation")

        if len(self.dataframes) != 2:
            print(f"Expected 2 dataframes. found {len(self.dataframes)}")
            raise ValueError("Reconciliation requires exactly 2 dataframes")

        df1, df2 = self.dataframes
        key_cols = self.config['key_cols']

        # key column validation make sure all key columns are in the dataframes
        for col in key_cols:
            if col not in df1.columns or col not in df2.columns:
                print(f"Key column {col} not found in both dataframes")
                raise ValueError(f"Key column {col} missing")

        df1 = df1.set_index(key_cols)
        df2 = df2.set_index(key_cols)

        self.metrics['total_records'] = len(df1) + len(df2)
        only_in_df1 = df1.index.difference(df2.index)
        only_in_df2 = df2.index.difference(df1.index)

        common_keys = df1.index.intersection(df2.index)
        common_cols = set(df1.columns).intersection(set(df2.columns))

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
        # if (len(only_in_df1) + len(only_in_df2) + total_mismatches) > self.config['notification_threshold']:
        #     self._send_notification()
        
        # Update last run for incremental processing
        with open(self.config['last_run_file'], 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'metrics': self.metrics
            }, f)
            
        print("differences: ", differences)
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
            f.write(f"- Actual fields with mismatches: {differences['value_mismatches']}\n\n")

            # Write detailed mismatches
            f.write(f"Detailed Mismatches:\n")
            for col, mismatches in differences['value_mismatches'].items():
                f.write(f"\n{col}:\n")
                for i, (key, val1, val2) in enumerate(mismatches[:10]):  # Limit to first 10
                    f.write(f"  {i+1}. Key: {key}, File1: {val1}, File2: {val2}\n")
                if len(mismatches) > 10:
                    f.write(f"  ... and {len(mismatches) - 10} more\n")
        
        # Generate visualizations
        # self._generate_visualizations(report_dir, differences)
        
        self.logger.info(f"Report generated in {report_dir}")
        print(f"Report generated in {report_dir}")


def main():
    files = ['./json_files/customers_core.json', './json_files/customers_legacy.json']
    schema = {}
    bankRecon = BankRecon(files, schema)
    bankRecon.load_and_flatten(files[0])
    bankRecon.load_and_flatten(files[1])


    dataframes = bankRecon.get_dataframes()
    core = bankRecon.normalize_core(dataframes[0])
    legacy = bankRecon.normalize_legacy(dataframes[1])
    print("core: \n", core)
    print("legacy: \n", legacy)

    diffs = bankRecon.reconcile()
    bankRecon.generate_report(diffs)


if __name__ == "__main__":
    main()