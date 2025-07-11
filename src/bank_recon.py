import json
import logging
import subprocess
import pandas as pd
from typing import Dict, List, Optional, Tuple, Union, Any
from config import predefined_config, predefined_metrics
from pandas import DataFrame, json_normalize


# for reconciliation for this specific file structure, we must first load the file into dataframes. flatten the structure, normalize fields, canonicalize, validate with schema.

# step 1: load the json into dataframes and print them out and verify that they were loaded
# step 2: flatten the structure by performing json_normalize
# step 3: rename the columns


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
            print("df: \n", df)
            return df
        except Exception as e:
            self.logger.info(f"error in load_and_flatten: {e}")
        return 

    def normalize_legacy(self, df):
        out = pd.DataFrame({
            "id": df["customerId"].astype(str),
            "firstName": df["firstName"].astype(str),
            "lastName": df["lastName"].astype(str),
            "email": df["email"].astype(str),
            "accountNumber": df["accountNumber"].astype(str),
            "routingNumber": df["routingNumber"].astype(str),
            "createdOn": df["signupDate"].astype(str),
        })
        return out
    
    def normalize_core(self, df):




def main():
    files = ['./json_files/customers_core.json', './json_files/customers_legacy.json']
    schema = {}
    bankRecon = BankRecon(files, schema)
    bankRecon.load_and_flatten(files[0])

if __name__ == "__main__":
    main()