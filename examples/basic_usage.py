#!/usr/bin/env python3
"""
Basic usage example for RoboLake CLI

This example demonstrates how to use the RoboLake libraries programmatically.
"""

import sys
from pathlib import Path

# Add the package to the path for development
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from robolake_cli import ROSbagProcessor, DataCatalog
from rich.console import Console

console = Console()

def main():
    """Demonstrate basic RoboLake functionality"""
    
    console.print("🤖 RoboLake Basic Usage Example", style="bold blue")
    console.print("=" * 50)
    
    # 1. Initialize a catalog
    catalog_path = Path("example_catalog")
    console.print(f"\n1. Initializing catalog at {catalog_path}")
    
    try:
        catalog = DataCatalog(str(catalog_path))
        console.print("✅ Catalog initialized successfully!")
    except Exception as e:
        console.print(f"❌ Error initializing catalog: {e}", style="red")
        return
    
    # 2. Create a sample table
    console.print("\n2. Creating sample table")
    
    sample_data = {
        "timestamp": [1.0, 2.0, 3.0, 4.0, 5.0],
        "topic": ["/imu", "/imu", "/gps", "/gps", "/imu"],
        "value": [0.1, 0.2, 42.0, 43.0, 0.3]
    }
    
    import pandas as pd
    df = pd.DataFrame(sample_data)
    
    # Store in catalog
    table_path = catalog_path / "tables" / "sample_data.parquet"
    table_path.parent.mkdir(exist_ok=True)
    df.to_parquet(table_path)
    
    console.print("✅ Sample table created!")
    
    # 3. Query the data
    console.print("\n3. Querying sample data")
    
    try:
        # Load tables into DuckDB
        catalog._load_tables_to_duckdb()
        
        # Execute a query
        result = catalog.query("SELECT * FROM sample_data WHERE topic = '/imu'")
        
        console.print("📊 Query results:")
        console.print(result.to_string())
        
    except Exception as e:
        console.print(f"❌ Error querying data: {e}", style="red")
    
    # 4. Show catalog info
    console.print("\n4. Catalog information")
    
    tables = catalog.list_tables()
    console.print(f"📋 Tables in catalog: {tables}")
    
    if tables:
        table_info = catalog.get_table_info(tables[0])
        console.print(f"📊 Table info for '{tables[0]}':")
        for key, value in table_info.items():
            console.print(f"  {key}: {value}")
    
    console.print("\n✅ Example completed successfully!")
    console.print("\n💡 Next steps:")
    console.print("  - Install rosbags: pip install 'robolake-cli[rosbags]'")
    console.print("  - Try with real ROSbag files")
    console.print("  - Explore the CLI commands")

if __name__ == "__main__":
    main() 