import json
import pendulum
from datetime import timedelta
from airflow.decorators import dag, task

@dag(
    schedule="0 6 * * *",  # Run daily at 6:00 AM UTC
    start_date=pendulum.datetime(2025, 9, 1, tz="UTC"),
    catchup=False,
    tags=["example", "scheduled"],
    description="Scheduled DAG that runs daily at 6 AM UTC with mixed dependencies",
    max_active_runs=1,
)
def scheduled_daily_dag():
    """
    ### Scheduled Daily DAG
    
    This DAG runs daily at 6:00 AM UTC and demonstrates a mix of
    serial and parallel task dependencies for a typical ETL workflow.
    """
    
    @task()
    def check_data_availability():
        """
        Initial task: Check if source data is available
        """
        print("Checking data availability...")
        # Simulate checking data sources
        sources = {
            "customer_db": True,
            "transaction_logs": True,
            "external_api": True,
            "file_uploads": True
        }
        
        available_sources = [source for source, available in sources.items() if available]
        print(f"Available data sources: {available_sources}")
        
        return {"available_sources": available_sources, "total_sources": len(sources)}
    
    @task()
    def extract_customer_data():
        """
        Parallel extraction task 1: Extract customer data
        """
        print("Extracting customer data...")
        customer_data = {
            "total_customers": 5000,
            "new_customers_today": 25,
            "active_customers": 3500
        }
        print(f"Extracted data for {customer_data['total_customers']} customers")
        return customer_data
    
    @task()
    def extract_transaction_data():
        """
        Parallel extraction task 2: Extract transaction data
        """
        print("Extracting transaction data...")
        transaction_data = {
            "total_transactions": 15000,
            "daily_revenue": 75000.5,
            "avg_transaction_value": 5.0,
            "payment_methods": ["credit_card", "debit_card", "paypal", "bank_transfer"]
        }
        print(f"Extracted {transaction_data['total_transactions']} transactions")
        print(f"Daily revenue: ${transaction_data['daily_revenue']}")
        return transaction_data
    
    @task()
    def extract_external_data():
        """
        Parallel extraction task 3: Extract external API data
        """
        print("Extracting external API data...")
        external_data = {
            "market_rates": {
                "USD": 1.0,
                "EUR": 0.85,
                "GBP": 0.75
            },
            "weather_impact": 0.95,
            "competitor_analysis": "stable"
        }
        print(f"Extracted external data: {list(external_data.keys())}")
        return external_data
    
    @task()
    def transform_and_combine_data(availability_check, customer_data, transaction_data, external_data):
        """
        Sequential task: Transform and combine all extracted data
        """
        print("Transforming and combining all data sources...")
        
        # Combine all data sources
        combined_data = {
            "data_sources": availability_check,
            "customers": customer_data,
            "transactions": transaction_data,
            "external": external_data,
            "processing_timestamp": pendulum.now().isoformat()
        }
        
        # Calculate some derived metrics
        if customer_data["total_customers"] > 0:
            avg_revenue_per_customer = transaction_data["daily_revenue"] / customer_data["total_customers"]
            combined_data["avg_revenue_per_customer"] = round(avg_revenue_per_customer, 2)
        
        print(f"Combined data processing completed at {combined_data['processing_timestamp']}")
        return combined_data
    
    @task()
    def generate_daily_summary(combined_data):
        """
        Final task: Generate daily summary report
        """
        print("Generating daily summary report...")
        
        summary = {
            "report_date": pendulum.now().format("YYYY-MM-DD"),
            "total_customers": combined_data["customers"]["total_customers"],
            "new_customers": combined_data["customers"]["new_customers_today"],
            "total_transactions": combined_data["transactions"]["total_transactions"],
            "daily_revenue": combined_data["transactions"]["daily_revenue"],
            "avg_revenue_per_customer": combined_data.get("avg_revenue_per_customer", 0),
            "data_sources_available": len(combined_data["data_sources"]["available_sources"]),
            "processing_completed": True
        }
        
        print("=== DAILY SUMMARY REPORT ===")
        for key, value in summary.items():
            print(f"{key}: {value}")
        print("=== END REPORT ===")
        
        return summary
    
    # Define task dependencies
    # First, check data availability
    availability_check = check_data_availability()
    
    # Then run parallel extraction tasks (all depend on availability check)
    customer_extraction = extract_customer_data()
    transaction_extraction = extract_transaction_data()
    external_extraction = extract_external_data()
    
    # Make sure extractions only run after availability check
    customer_extraction.set_upstream(availability_check)
    transaction_extraction.set_upstream(availability_check)
    external_extraction.set_upstream(availability_check)
    
    # Transform and combine data (depends on all extractions)
    combined_data = transform_and_combine_data(
        availability_check,
        customer_extraction,
        transaction_extraction,
        external_extraction
    )
    
    # Generate final summary report
    final_summary = generate_daily_summary(combined_data)

# Instantiate the DAG
dag_instance = scheduled_daily_dag()
