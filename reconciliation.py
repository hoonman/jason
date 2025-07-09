
import json
import subprocess         
import pandas as pd
from pandas import json_normalize
from jsonschema import Draft7Validator
import datetime

# --- STEP 1: Canonicalize JSON on disk (structural normalization) ---
# Choice: jq sorts keys and strips extraneous whitespace → repeatable, git‑friendly files.
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

# --- STEP 2: Optional Schema Validation ---
# Choice: Define a minimal schema to enforce presence/type of crucial fields.
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

# validate with the schema 
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


    
# load and flatten (json_normalize from pandas)
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


print(dfA.info())


# clean and cast types
# enforce datetime dtype, float for amt, int for IDs
# string for name
for df in (dfA, dfB):
    df["order_ts"] = pd.to_datetime(df["order_ts"], utc=True)
    df["order_amt"] = df["order_amt"].astype(float)
    df["cust_customer.id"] = df["cust_customer.id"].astype(int)
    df["cust_customer.name"] = df["cust_customer.name"].astype("string")

print(dfA.info())
print(dfB.info())


