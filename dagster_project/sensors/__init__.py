"""
Sensors for event-driven pipeline triggers.
Example: Trigger pipeline when new CSV files are detected.
"""
from dagster import sensor, RunRequest, SensorEvaluationContext, DefaultSensorStatus
import os
from pathlib import Path

@sensor(
    name="new_data_sensor",
    description="Triggers pipeline when new data files are detected",
    default_status=DefaultSensorStatus.STOPPED,  # Start disabled
)
def new_data_file_sensor(context: SensorEvaluationContext):
    """
    Monitors data/bronze directory for new CSV files.
    Triggers bronze_ingestion_job when files are modified.
    """
    bronze_dir = Path("data/bronze")
    
    if not bronze_dir.exists():
        return
    
    # Check for CSV files
    csv_files = list(bronze_dir.glob("*.csv"))
    
    if not csv_files:
        context.log.info("No CSV files found in bronze directory")
        return
    
    # Get the most recent file modification time
    latest_mtime = max(f.stat().st_mtime for f in csv_files)
    
    # Get last run time from cursor (if exists)
    last_mtime = float(context.cursor) if context.cursor else 0
    
    # If files have been modified since last check
    if latest_mtime > last_mtime:
        context.log.info(f"New data detected! Triggering pipeline...")
        
        # Update cursor with new timestamp
        context.update_cursor(str(latest_mtime))
        
        # Trigger the bronze ingestion job
        return RunRequest(
            run_key=f"data_update_{latest_mtime}",
            run_config={},
        )
    
    context.log.info("No new data detected")