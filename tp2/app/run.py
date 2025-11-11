#!/usr/bin/env python3
"""
Main execution script for the e-commerce graph database project.
Can run tests or execute the ETL process.
"""

import sys
import argparse
from pathlib import Path

# Add current directory to path to import test module
sys.path.insert(0, str(Path(__file__).parent))

from test import run_all_tests
from etl import etl


def main():
    """Main entry point for the application."""
    parser = argparse.ArgumentParser(
        description="E-Commerce Graph Database - Main Execution Script"
    )
    parser.add_argument(
        "command",
        choices=["test", "etl", "all"],
        help="Command to execute: 'test' runs tests, 'etl' runs ETL, 'all' runs both",
    )
    
    args = parser.parse_args()
    
    if args.command == "test":
        print("Running test suite...")
        success = run_all_tests()
        sys.exit(0 if success else 1)
        
    elif args.command == "etl":
        print("Running ETL process...")
        try:
            etl()
            print("\n✓ ETL process completed successfully")
            sys.exit(0)
        except Exception as e:
            print(f"\n✗ ETL process failed: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
            
    elif args.command == "all":
        print("Running ETL, then tests...")
        # First run ETL
        try:
            etl()
            print("\n✓ ETL process completed successfully")
        except Exception as e:
            print(f"\n✗ ETL process failed: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
        
        # Then run tests
        print("\n" + "=" * 60)
        print("Now running test suite...")
        print("=" * 60)
        success = run_all_tests()
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

