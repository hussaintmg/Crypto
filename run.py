#!/usr/bin/env python3
"""
Real-Time Crypto Analytics Platform
Main entry point for running different components
"""

import argparse
import subprocess
import sys
import os
from src.etl_pipeline import ETLPipeline
from src.dashboard import main as run_dashboard

def run_etl():
    """Run ETL pipeline"""
    pipeline = ETLPipeline()
    pipeline.run_scheduled()

def run_dashboard_only():
    """Run dashboard only"""
    run_dashboard()

def run_all():
    """Run both ETL and dashboard"""
    from multiprocessing import Process
    
    etl_process = Process(target=run_etl)
    dashboard_process = Process(target=run_dashboard)
    
    etl_process.start()
    dashboard_process.start()
    
    try:
        etl_process.join()
        dashboard_process.join()
    except KeyboardInterrupt:
        print("\nShutting down...")
        etl_process.terminate()
        dashboard_process.terminate()
        etl_process.join()
        dashboard_process.join()

def init_database():
    """Initialize database schema"""
    from src.database import DatabaseManager
    db = DatabaseManager()
    print("Database initialized successfully")

def main():
    parser = argparse.ArgumentParser(description='Real-Time Crypto Analytics Platform')
    parser.add_argument('--component', choices=['etl', 'dashboard', 'all', 'init-db'],
                       default='all', help='Component to run')
    
    args = parser.parse_args()
    
    if args.component == 'etl':
        run_etl()
    elif args.component == 'dashboard':
        run_dashboard_only()
    elif args.component == 'init-db':
        init_database()
    else:
        run_all()

if __name__ == "__main__":
    main()