"""
Data catalog for RoboLake CLI

Manages local data storage and SQL querying capabilities.
"""

from pathlib import Path
from typing import Dict, Any, List, Optional
import pandas as pd
import duckdb
import logging
import json
from datetime import datetime

class DataCatalog:
    """Manages local data catalog for robotics data"""
    
    def __init__(self, catalog_path: str):
        self.catalog_path = Path(catalog_path)
        self.catalog_path.mkdir(parents=True, exist_ok=True)
        
        # Create tables directory
        tables_dir = self.catalog_path / "tables"
        tables_dir.mkdir(exist_ok=True)
        
        # Initialize DuckDB connection for local queries
        self.conn = duckdb.connect(":memory:")
    
    def append_rosbag(self, table_name: str, rosbag_path: str, topics: List[str] = None) -> None:
        """Append ROSbag data to catalog table"""
        try:
            from .processor import ROSbagProcessor
            
            # Process ROSbag
            processor = ROSbagProcessor(rosbag_path)
            df = processor.convert_to_dataframe(topics)
            
            if df.empty:
                logging.warning("No data extracted from ROSbag")
                return
            
            # Append to table
            self._append_dataframe(table_name, df)
            
            logging.info(f"Successfully appended ROSbag data to table: {table_name}")
            
        except Exception as e:
            logging.error(f"Error appending ROSbag data: {e}")
            raise
    
    def _append_dataframe(self, table_name: str, df: pd.DataFrame) -> None:
        """Append DataFrame to table"""
        table_path = self.catalog_path / "tables" / f"{table_name}.parquet"
        
        if table_path.exists():
            # Read existing data and append
            existing_df = pd.read_parquet(table_path)
            combined_df = pd.concat([existing_df, df], ignore_index=True)
        else:
            combined_df = df
        
        # Write back to parquet
        combined_df.to_parquet(table_path, index=False)
        
        # Update DuckDB view
        self.conn.execute(f"""
            CREATE OR REPLACE VIEW {table_name} AS 
            SELECT * FROM read_parquet('{table_path}')
        """)
    
    def query(self, sql: str) -> pd.DataFrame:
        """Execute SQL query against catalog"""
        try:
            # First, try to load all available tables into DuckDB
            self._load_tables_to_duckdb()
            
            # Execute query
            result = self.conn.execute(sql)
            df = result.df()
            
            return df
            
        except Exception as e:
            logging.error(f"Error executing query: {e}")
            raise
    
    def _load_tables_to_duckdb(self) -> None:
        """Load all tables from catalog into DuckDB for querying"""
        tables_dir = self.catalog_path / "tables"
        
        if not tables_dir.exists():
            return
        
        # Find all parquet files
        for parquet_file in tables_dir.glob("*.parquet"):
            table_name = parquet_file.stem
            
            # Create view if it doesn't exist
            self.conn.execute(f"""
                CREATE OR REPLACE VIEW {table_name} AS 
                SELECT * FROM read_parquet('{parquet_file}')
            """)
    
    def list_tables(self) -> List[str]:
        """List all tables in the catalog"""
        tables = []
        tables_dir = self.catalog_path / "tables"
        
        if tables_dir.exists():
            for parquet_file in tables_dir.glob("*.parquet"):
                tables.append(parquet_file.stem)
        
        return tables
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get information about a specific table"""
        info = {
            "name": table_name,
            "exists": False,
            "row_count": 0,
            "columns": [],
            "size_bytes": 0
        }
        
        # Check if table exists
        table_path = self.catalog_path / "tables" / f"{table_name}.parquet"
        if table_path.exists():
            info["exists"] = True
            info["size_bytes"] = table_path.stat().st_size
            
            # Get row count
            try:
                df = pd.read_parquet(table_path)
                info["row_count"] = len(df)
                info["columns"] = list(df.columns)
            except Exception as e:
                logging.warning(f"Error reading table for info: {e}")
        
        return info
    
    def delete_table(self, table_name: str) -> bool:
        """Delete a table from the catalog"""
        try:
            # Delete parquet file
            table_path = self.catalog_path / "tables" / f"{table_name}.parquet"
            if table_path.exists():
                table_path.unlink()
            
            # Remove from DuckDB
            self.conn.execute(f"DROP VIEW IF EXISTS {table_name}")
            
            return True
            
        except Exception as e:
            logging.error(f"Error deleting table: {e}")
            return False 