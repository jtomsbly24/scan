#!/usr/bin/env python3

import traceback
from indicators import sync_master_to_working, ensure_computed_table

def main():
    print("🔧 Starting computed table update...")

    try:
        sync_master_to_working()
        ensure_computed_table()
        print("✅ Computation completed successfully.")
    except Exception as e:
        print("❌ Error updating computation:")
        print(traceback.format_exc())
        exit(1)

if __name__ == "__main__":
    main()
