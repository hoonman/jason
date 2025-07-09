
import json
import subprocess         
import pandas as pd
from pandas import json_normalize
from jsonschema import Draft7Validator
import datetime

# step 1: canonicalize json on disk (structural normalization).  jq sorts keys and strips extraneous whitespace → repeatable, git‑friendly files.

# step 2: schema validation with minimal schema to enforce presence/type of crucial fields. 

# validate with the defined schema 
    
# step 3: load and flatten (json_normalize from pandas)

# step 4: clean and cast types to enforce datetime as dtype, float for amt, int for IDs, string for name

# define recon keys

# step 5: merge & flag differences 
# compute per-field diffs 

# step 6: report
# dump to csv
# only_A.to_csv("report_only_A.csv", index=False)
# only_B.to_csv("report_only_B.csv", index=False)

# both[both["amt_diff"] != 0].to_csv("report_amt_mismatch.csv", index=False)
# both[both["ts_diff"] != datetime.timedelta(0)].to_csv("report_ts_mismatch.csv", index=False)

# TODO: step 8 automate & schedule

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

# let us define a class Reconciliation in order to make this modular
class Reconciliation:
    def __init__(self, files, schema, config):
        self.files = files
        self.canon_files = []
        self.schema = schema
        self.dataframes = []
        # self.key_cols = ["cust_customer.id", "order_order_id"]
        self.config = {
            'key_cols': ["cust_customer.id", "order_order_id"],
            'numeric_tolerance': 0.01,
            'time_tolerance_seconds': 60,
            'log_file': 'reconciliation.log',
            'notification_threshold': 10, # number of diffs that trigger notifs
            'notification_email': None,
            'incremental': False,
            'last_run_file': '.last_run.json',
            'chunk_size': 10000 
        }
        if config:
            self.config.update(config)

        self.metrics = {
            'total_records': 0,
            'matching_records': 0,
            'mismatches': {},
            'run_time': 0
        }

    def jq_canonicalize(self):
        for file in self.files:
            out = file.replace(".json", "_canon.json")
            subprocess.run(
                f"jq --sort-keys . {file} > {out}",
                shell=True, check=True
            )
            self.canon_files.append(out)

    def validate_with_schema(self):
        if self.canon_files:
            for canon in self.canon_files:
                data = json.load(open(canon))
                errors = list(Draft7Validator(self.schema).iter_errors(data))
                if errors: 
                    print(f"Error while validating with schema: {canon}")
                    for e in errors[:5]:
                        print(" ", e.message)
                    raise SystemExit(1)
                else:
                    print("no errors found! we are good to move on")

    def load_and_flatten(self, path):
        data = json.load(open(path))
        df = json_normalize(data, record_path="orders", meta=[["customer", "id"], ["customer", "name"]], record_prefix="order_", meta_prefix="cust_")
        # we are extracting "orders" and normalizing it.
        # meta= additional fields to normalize (extracts customer's id and name) 
        # string to prepend to column names from the normalized records 
        # what is the difference between normal vs. meta? (is meta just additional columns? )
        self.dataframes.append(df)
        return df

    def clean_and_cast(self):
        if self.dataframes:
            for df in self.dataframes:
                df["order_ts"] = pd.to_datetime(df["order_ts"], utc=True)
                df["order_amt"] = df["order_amt"].astype(float)
                df["cust_customer.id"] = df["cust_customer.id"].astype(int)
                df["cust_customer.name"] = df["cust_customer.name"].astype("string")

    def merge_flag_diffs(self, dfA, dfB):
        merged = dfA.merge(dfB, on=self.config['key_cols'], how="outer", suffixes=("_A", "_B"), indicator=True)
        only_A = merged[merged["_merge"] == "left_only"]
        only_B = merged[merged["_merge"] == "right_only"]
        both = merged[merged["_merge"] == "both"].copy()

        both["amt_diff"] = both["order_amt_A"] - both["order_amt_B"]
        both["ts_diff"] = both["order_ts_A"] - both["order_ts_B"]
        return {"only_A": only_A, "only_B": only_B, "both": both}

    def report(self):
        # this function will call the rest of the functions and create a comprehensive report 
        self.jq_canonicalize()
        self.validate_with_schema()
        # self.load_and_flatten()
        for file in self.canon_files:
            print("file name in canon files: ", file)
            self.load_and_flatten(file)
        self.clean_and_cast()
        merged_objects = self.merge_flag_diffs(self.dataframes[0], self.dataframes[1])

        print("Records only in A: ", len(merged_objects["only_A"]))
        print("Records only in B: ", len(merged_objects["only_B"]))
        print("Shared records with amount mismatch: ", (merged_objects["both"]["amt_diff"] != 0).sum())
        print("shared records with timestamp mismatch: ", (merged_objects["both"]["ts_diff"] != datetime.timedelta(0)).sum())

def main():
    recon = Reconciliation(["fileA.json", "fileB.json"], schema, config={})
    print("recon calls report: ")
    recon.report()
    
if __name__ == "__main__":
    main()