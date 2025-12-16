#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Schedules the weather data fetcher to run at regular intervals.
"""

import schedule
import time
import sys
from fetch_weather_from_openmeteo import main as fetch_weather_main

def job():
    """Defines the job to be scheduled."""
    print("Running scheduled weather data fetch...")
    try:
        fetch_weather_main()
        print("Scheduled weather data fetch finished successfully.")
    except Exception as e:
        print(f"An error occurred during the scheduled job: {e}")

if __name__ == "__main__":
    # Schedule the job every 60 minutes
    schedule.every(60).minutes.do(job)

    print("Scheduler started. The weather data will be updated every 60 minutes.")
    print("Press Ctrl+C to exit.")

    # Run the job immediately for the first time
    job()

    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            print("\nScheduler stopped by user.")
            sys.exit(0)
