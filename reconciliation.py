
import json
import subprocess         
import pandas as pd
from pandas import json_normalize
from jsonschema import Draft7Validator
import datetime

# step 1: canonicalize json on disk (structural normalization).  jq sorts keys and strips extraneous whitespace → repeatable, git‑friendly files.
for fname in ("fileA.json", "fileB.json"):
    out = fname.replace(".json", "_canon.json")
    # subprocess.run(
    #     ["jq", "--sort-keys", ".", fname, ">", out],
    #     shell=True, check=True
    # )
    subprocess.run(
        f"jq --sort-keys . {fname} > {out}",
        shell=True, check=True
    )

# step 2: schema validation with minimal schema to enforce presence/type of crucial fields. 
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

# validate with the defined schema 
for canon in ("fileA_canon.json", "fileB_canon.json"):
    data = json.load(open(canon))
    errors = list(Draft7Validator(schema).iter_errors(data))
    if errors:
        print(f"Schema errors in {canon}:")
        for e in errors[:5]:
            print(" ", e.message)
        raise SystemExit(1)
    else:
        print("no errors!")


    
# step 3: load and flatten (json_normalize from pandas)
def load_and_flatten(path):
    data = json.load(open(path))
    df = json_normalize(data, record_path="orders", meta=[["customer", "id"], ["customer", "name"]], record_prefix="order_", meta_prefix="cust_")
    # we are extracting "orders" and normalizing it.
    # meta= additional fields to normalize (extracts customer's id and name) 
    # string to prepend to column names from the normalized records 
    # what is the difference between normal vs. meta? (is meta just additional columns? )
    return df

dfA = load_and_flatten('fileA_canon.json')
dfB = load_and_flatten('fileB_canon.json')

# step 4: clean and cast types to enforce datetime as dtype, float for amt, int for IDs, string for name
for df in (dfA, dfB):
    df["order_ts"] = pd.to_datetime(df["order_ts"], utc=True)
    df["order_amt"] = df["order_amt"].astype(float)
    df["cust_customer.id"] = df["cust_customer.id"].astype(int)
    df["cust_customer.name"] = df["cust_customer.name"].astype("string")

# define recon keys
key_cols = ["cust_customer.id", "order_order_id"]

# step 5: merge & flag differences 
merged = dfA.merge(dfB, on=key_cols, how="outer", suffixes=("_A", "_B"), indicator=True)

only_A = merged[merged["_merge"] == "left_only"]
only_B = merged[merged["_merge"] == "right_only"]
both = merged[merged["_merge"] == "both"].copy()

print("only a: \n", only_A)
print("only B: \n", only_B)
print("both: \n", both)

print("only a: \n", only_A.info())
print("only B: \n", only_B.info())
print("both: \n", both.info())


# compute per-field diffs 
both["amt_diff"] = both["order_amt_A"] - both["order_amt_B"]
both["ts_diff"] = both["order_ts_A"] - both["order_ts_B"]

# step 6: report
print("Records only in A: ", len(only_A))
print("Records only in B: ", len(only_B))
print("Shared records with amount mismatch: ", (both["amt_diff"] != 0).sum())
print("shared records with timestamp mismatch: ", (both["ts_diff"] != datetime.timedelta(0)).sum())

# dump to csv
only_A.to_csv("report_only_A.csv", index=False)
only_B.to_csv("report_only_B.csv", index=False)

both[both["amt_diff"] != 0].to_csv("report_amt_mismatch.csv", index=False)
both[both["ts_diff"] != datetime.timedelta(0)].to_csv("report_ts_mismatch.csv", index=False)

# TODO: step 8 automate & schedule