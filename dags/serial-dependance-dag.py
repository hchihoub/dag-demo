import json
import pendulum
from datetime import timedelta
from airflow.decorators import dag, task

@dag(
    schedule=None,
    start_date=pendulum.datetime(2025, 9, 1, tz="UTC"),
    catchup=False,
    tags=["example", "serial"],
    description="DAG with 4 serial dependent tasks using TaskFlow API",
)
def serial_dependencies_dag():
    """
    ### Serial Dependencies DAG
    
    This DAG demonstrates 4 tasks that run sequentially,
    where each task depends on the completion of the previous one.
    """
    
    @task()
    def extract_data():
        """
        Task 1: Extract data
        Simulates extracting data from a source
        """
        print("Extracting data from source...")
        data = {"records": [
            {"id": 1, "value": 100},
            {"id": 2, "value": 200},
            {"id": 3, "value": 300}
        ]}
        print(f"Extracted {len(data['records'])} records")
        return data
    
    @task()
    def clean_data(raw_data: dict):
        """
        Task 2: Clean data
        Processes and cleans the extracted data
        """
        print("Cleaning extracted data...")
        cleaned_data = []
        for record in raw_data["records"]:
            if record["value"] > 0:
                cleaned_data.append({
                    "id": record["id"],
                    "value": record["value"],
                    "status": "clean"
                })
        print(f"Cleaned {len(cleaned_data)} records")
        return {"cleaned_records": cleaned_data}
    
    @task()
    def transform_data(cleaned_data: dict):
        """
        Task 3: Transform data
        Applies business logic transformations
        """
        print("Transforming cleaned data...")
        transformed_data = []
        for record in cleaned_data["cleaned_records"]:
            transformed_record = {
                "id": record["id"],
                "value": record["value"] * 1.1,  # Apply 10% increase
                "transformed": True
            }
            transformed_data.append(transformed_record)
        print(f"Transformed {len(transformed_data)} records")
        return {"transformed_records": transformed_data}
    
    @task()
    def load_data(transformed_data: dict):
        """
        Task 4: Load data
        Simulates loading data to a destination
        """
        print("Loading transformed data...")
        for record in transformed_data["transformed_records"]:
            print(f"Loading record ID {record['id']} with value {record['value']}")
        print(f"Successfully loaded {len(transformed_data['transformed_records'])} records")
        return {"status": "success", "records_loaded": len(transformed_data["transformed_records"])}
    
    # Define task dependencies
    raw_data = extract_data()
    cleaned_data = clean_data(raw_data)
    transformed_data = transform_data(cleaned_data)
    final_result = load_data(transformed_data)

# Instantiate the DAG
dag_instance = serial_dependencies_dag()
