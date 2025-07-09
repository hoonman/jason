
import json
import subprocess          # for jq canonicalization
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


    