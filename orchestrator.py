from prefect import flow, task

@task(name="Process Data Task 1")
def extract():
    return {"data": [1, 2, 3]}

@task(name="Process Data Task 2")
def process_data(data):
    # Simulate data processing
    return [d * 2 for d in data]

@flow(name="Data Processing Flow")
def main_flow():
    raw_data = extract()
    processed_data = process_data(raw_data["data"])
    return processed_data

if __name__ == "__main__":
    result = main_flow()
    print(f"Processed Data: {result}")
    
    

