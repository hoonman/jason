import json, random, datetime

def generate_file(filename: str, n_records: int):
    data = []
    for i in range(n_records):
        # Build a customer record
        cust_id = i % 1000
        record = {
            "customer": {
                "id": cust_id,
                "name": f"Customer {cust_id}"
            },
            # Each customer has 1–5 orders
            "orders": [
                {
                    "order_id": i * 10 + j,
                    "amt": round(random.uniform(10.0, 1000.0), 2),
                    "ts": (
                        datetime.datetime.now(datetime.timezone.utc)
                        - datetime.timedelta(days=random.randint(0, 365))
                    ).isoformat()
                }
                for j in range(random.randint(1, 5))
            ]
        }
        data.append(record)

    with open(filename, "w") as f:
        json.dump(data, f, indent=2)

if __name__ == "__main__":
    generate_file("fileA.json", 5000)
    generate_file("fileB.json", 5000)
    print("Generated fileA.json and fileB.json (5000 records each)")
