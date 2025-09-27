from prefect import flow, task

@task(name="Process Data Task 1")
def extract():
    print("Extracting data...")
    return {"data": [1, 2, 3]}

@task(name="Process Data Task 2")
def process_data(data):
    print("Processing data 2 ...")
    # Simulate data processing
    return [d * 2 for d in data]

@task(name="Process Data Task 3")
def finalize_data(data):
    print("Finalizing data 3...")
    # Simulate data processing
    return [d / 2 for d in data]

@flow(name="Data Processing Flow")
def main_flow():
    print("Starting data processing flow...")
    raw_data = extract()
    processed_data = process_data(raw_data["data"])
    finalized_data = finalize_data(processed_data)
    print("Data processing complete.")
    return finalized_data

if __name__ == "__main__":
    result = main_flow()
    print(f"Processed Data: {result}")
    
    

