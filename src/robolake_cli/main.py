#!/usr/bin/env python3
"""
RoboLake CLI - Robotics data processing platform

Convert ROSbag files to analytics-ready formats like Parquet, CSV, and JSON.
"""

import click
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
from pathlib import Path
import sys
import logging

from .processor import ROSbagProcessor
from .catalog import DataCatalog

console = Console()

@click.group()
@click.version_option()
def main():
    """RoboLake - Robotics Data Processing Platform
    
    Convert ROSbag files to analytics-ready formats for data science and analysis.
    """
    pass

@main.command()
@click.argument('input_file', type=click.Path(exists=True))
@click.option('--format', type=click.Choice(['parquet', 'csv', 'json']), default='parquet', help='Output format')
@click.option('--topics', help='Comma-separated list of topics to extract')
@click.option('--output', '-o', help='Output file path')
@click.option('--catalog', help='Data catalog path for storage')
def convert(input_file, format, topics, output, catalog):
    """Convert ROSbag file to specified format"""
    console.print(f"🤖 Converting {input_file} to {format}")
    
    # Parse topics if provided
    topic_list = None
    if topics:
        topic_list = [t.strip() for t in topics.split(',')]
    
    # Determine output path
    if not output:
        input_path = Path(input_file)
        output = input_path.with_suffix(f'.{format}')
    
    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Processing ROSbag...", total=None)
            
            # Initialize processor
            processor = ROSbagProcessor(input_file)
            
            # Convert based on format
            if format == 'parquet':
                result_path = processor.convert_to_parquet(output, topic_list)
            elif format == 'csv':
                df = processor.convert_to_dataframe(topic_list)
                df.to_csv(output, index=False)
                result_path = output
            elif format == 'json':
                df = processor.convert_to_dataframe(topic_list)
                df.to_json(output, orient='records', indent=2)
                result_path = output
            
            progress.update(task, description="✅ Conversion complete!")
        
        console.print(f"📁 Output saved to: {result_path}")
        
        # If catalog specified, also store in catalog
        if catalog:
            console.print(f"🗄️  Storing in data catalog: {catalog}")
            data_catalog = DataCatalog(catalog)
            table_name = Path(input_file).stem
            data_catalog.append_rosbag(table_name, input_file, topic_list)
            console.print(f"✅ Data stored in table: {table_name}")
            
    except Exception as e:
        console.print(f"❌ Error converting file: {e}", style="red")
        sys.exit(1)

@main.command()
@click.argument('input_file', type=click.Path(exists=True))
def info(input_file):
    """Show information about ROSbag file"""
    console.print(f"📊 Analyzing {input_file}")
    
    try:
        processor = ROSbagProcessor(input_file)
        metadata = processor.metadata
        
        # Create info table
        table = Table(title=f"ROSbag Information: {Path(input_file).name}")
        table.add_column("Property", style="cyan")
        table.add_column("Value", style="green")
        
        table.add_row("Topics", str(len(metadata.get('topics', []))))
        table.add_row("Message Count", str(metadata.get('message_count', 0)))
        table.add_row("Duration", f"{metadata.get('duration', 0):.2f} seconds")
        table.add_row("Start Time", str(metadata.get('start_time', 'Unknown')))
        table.add_row("End Time", str(metadata.get('end_time', 'Unknown')))
        
        console.print(table)
        
        # Show topics if available
        if metadata.get('topics'):
            console.print("\n📋 Available Topics:")
            for topic in metadata['topics']:
                console.print(f"  • {topic}")
                
    except Exception as e:
        console.print(f"❌ Error analyzing file: {e}", style="red")
        sys.exit(1)

@main.command()
@click.argument('catalog_path', type=click.Path())
@click.option('--force', is_flag=True, help='Overwrite existing catalog')
def init(catalog_path, force):
    """Initialize new data catalog"""
    console.print(f"🗄️  Initializing catalog at {catalog_path}")
    
    try:
        catalog_path = Path(catalog_path)
        
        if catalog_path.exists() and not force:
            console.print("⚠️  Catalog already exists. Use --force to overwrite.", style="yellow")
            return
        
        # Initialize catalog
        data_catalog = DataCatalog(str(catalog_path))
        
        console.print("✅ Data catalog initialized successfully!")
        console.print(f"📁 Catalog location: {catalog_path}")
        
    except Exception as e:
        console.print(f"❌ Error initializing catalog: {e}", style="red")
        sys.exit(1)

@main.command()
@click.argument('catalog_path', type=click.Path(exists=True))
@click.argument('sql_query')
def query(catalog_path, sql_query):
    """Execute SQL query against data catalog"""
    console.print(f"🔍 Executing query on catalog: {catalog_path}")
    
    try:
        data_catalog = DataCatalog(catalog_path)
        result = data_catalog.query(sql_query)
        
        if not result.empty:
            console.print(f"📊 Query returned {len(result)} rows:")
            console.print(result.to_string())
        else:
            console.print("📭 Query returned no results")
            
    except Exception as e:
        console.print(f"❌ Error executing query: {e}", style="red")
        sys.exit(1)

if __name__ == '__main__':
    main() 