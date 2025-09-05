import json
import pendulum
from datetime import timedelta
from airflow.decorators import dag, task

@dag(
    schedule=None,
    start_date=pendulum.datetime(2025, 9, 1, tz="UTC"),
    catchup=False,
    tags=["example", "parallel"],
    description="DAG with 4 parallel independent tasks",
)
def parallel_tasks_dag():
    """
    ### Parallel Tasks DAG
    
    This DAG demonstrates 4 tasks that run in parallel,
    where each task is independent and can run simultaneously.
    """
    
    @task()
    def process_system_logs():
        """
        Task 1: Process system logs independently
        """
        print("Processing system logs...")
        log_count = 1500
        error_count = 23
        warning_count = 45
        print(f"Processed {log_count} log entries")
        print(f"Found {error_count} errors and {warning_count} warnings")
        return {"logs_processed": log_count, "errors": error_count, "warnings": warning_count}
    
    @task()
    def generate_daily_report():
        """
        Task 2: Generate daily report independently
        """
        print("Generating daily business report...")
        report_data = {
            "date": "2025-09-01",
            "total_sales": 50000,
            "new_customers": 125,
            "active_users": 2340
        }
        print(f"Report generated for {report_data['date']}")
        print(f"Sales: ${report_data['total_sales']}, New customers: {report_data['new_customers']}")
        return report_data
    
    @task()
    def backup_database():
        """
        Task 3: Backup database independently
        """
        print("Starting database backup...")
        # Simulate backup process
        tables_backed_up = ["users", "orders", "products", "payments", "logs"]
        backup_size_gb = 2.3
        print(f"Backed up {len(tables_backed_up)} tables")
        print(f"Backup size: {backup_size_gb} GB")
        
        backup_result = {
            "status": "success",
            "tables_backed_up": tables_backed_up,
            "backup_size_gb": backup_size_gb,
            "backup_timestamp": pendulum.now().isoformat()
        }
        print("Database backup completed successfully!")
        return backup_result
    
    @task()
    def send_notifications():
        """
        Task 4: Send notifications independently
        """
        print("Sending notifications...")
        # Simulate sending various notifications
        notifications = [
            {"type": "email", "recipient": "admin@company.com", "subject": "Daily System Status"},
            {"type": "slack", "channel": "#operations", "message": "Systems running normally"},
            {"type": "sms", "recipient": "+1234567890", "message": "Backup completed"},
            {"type": "webhook", "url": "https://api.monitoring.com/status", "payload": {"status": "ok"}}
        ]
        
        sent_notifications = []
        for notification in notifications:
            print(f"Sending {notification['type']} notification...")
            sent_notifications.append({
                "type": notification["type"],
                "status": "sent",
                "timestamp": pendulum.now().isoformat()
            })
        
        print(f"Successfully sent {len(sent_notifications)} notifications")
        return {"notifications_sent": sent_notifications, "total_count": len(sent_notifications)}
    
    # Define parallel tasks - all tasks run independently
    logs_task = process_system_logs()
    report_task = generate_daily_report()
    backup_task = backup_database()
    notifications_task = send_notifications()
    
    # In a parallel DAG, tasks don't have dependencies between them
    # They all run at the same time when the DAG is triggered

# Instantiate the DAG
dag_instance = parallel_tasks_dag()

