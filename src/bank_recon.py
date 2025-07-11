import json
import os
import sys
import logging
import subprocess
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
            print("df: \n", df)
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
            "account_number": df["accountNumber"].astype(str),
            "routing_number": df["routingNumber"].astype(str),
            "created_on": pd.to_datetime(df["signupDate"]).dt.date,
        })
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
        print("first names: ", first_names, "\nlast names: ", last_names)

        print("info of the core df: \n",df.info())
        out = pd.DataFrame({
            "id": df["id"].astype(str),
            "first_name": first_names,
            "last_names": last_names,
            "email": df["contact.email"].str.strip().str.lower(),
            "account_number_masked": df["bankDetails.acctNumMasked"].str.strip().str.lower(),
            "routing_number": df["bankDetails.rtgNum"].str.strip().str.lower(),
            "created_on": pd.to_datetime(df["createdAt"]).dt.date
        })
        return out

    def get_dataframes(self):
        return self.dataframes

    def clean_and_cast(self) -> None:
        if not self.dataframes:
            self.logger.warning("no dataframes to clean. load data first")

        # try:
        #     for i, df in enumerate(self.dataframes):
        #         df[""]
        return

    



def main():
    files = ['./json_files/customers_core.json', './json_files/customers_legacy.json']
    schema = {}
    bankRecon = BankRecon(files, schema)
    bankRecon.load_and_flatten(files[0])
    bankRecon.load_and_flatten(files[1])


    dataframes = bankRecon.get_dataframes()
    core = bankRecon.normalize_core(dataframes[0])
    legacy= bankRecon.normalize_legacy(dataframes[1])
    print("core: \n", core.info())
    print("legacy: \n", legacy.info())

if __name__ == "__main__":
    main()