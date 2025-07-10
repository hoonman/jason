import json, random, datetime

def generate_file(filename: str, n_records: int, customer_pool=None, seed=None):
    """Generate a JSON file with customer and order data.
    
    Args:
        filename: Output JSON filename
        n_records: Number of records to generate
        customer_pool: Optional pre-generated customer data to ensure commonality
        seed: Random seed for reproducibility
    """
    if seed is not None:
        random.seed(seed)
    
    data = []
    for i in range(n_records):
        # Build a customer record
        cust_id = i % 1000
        
        # Use customer from pool if available and applicable
        if customer_pool and cust_id < len(customer_pool) and random.random() < 0.7:
            # 70% chance to use a customer from the pool
            record = customer_pool[cust_id].copy()
            # Generate new orders for existing customers
            record["orders"] = [
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
        else:
            record = {
                "customer": {
                    "id": cust_id,
                    "name": f"Customer {cust_id}"
                },
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
    
    return data

if __name__ == "__main__":
    # Generate common customer pool first
    common_customers = []
    for i in range(500):  # Create pool of 500 common customers
        common_customers.append({
            "customer": {
                "id": i,
                "name": f"Customer {i}"
            },
            "orders": []  # Empty orders initially
        })
    
    # Use same customer pool but different random seeds
    generate_file("fileA.json", 5000, customer_pool=common_customers, seed=42)
    generate_file("fileB.json", 5000, customer_pool=common_customers, seed=43)
    print("Generated fileA.json and fileB.json (5000 records each, with shared customer data)")