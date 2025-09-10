#!/usr/bin/env python3
"""
Script to download all 99 TPC-DS queries from DuckDB repository
and generate Python code for run_queries.py
"""

import requests
import sys
from typing import Dict

def download_query(query_num: int) -> str:
    """Download a single TPC-DS query"""
    url = f"https://raw.githubusercontent.com/duckdb/duckdb/main/extension/tpcds/dsdgen/queries/{query_num:02d}.sql"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text.strip()
    except Exception as e:
        print(f"Error downloading query {query_num}: {e}")
        return None

def clean_query(sql: str) -> str:
    """Clean and format SQL query"""
    # Remove extra whitespace and normalize
    lines = [line.rstrip() for line in sql.split('\n')]
    # Remove empty lines at start and end
    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop()
    
    return '\n'.join(lines)

def generate_query_dict() -> str:
    """Generate the SAMPLE_QUERIES dictionary with all 99 TPC-DS queries"""
    
    print("Downloading all 99 TPC-DS queries...")
    
    queries = {}
    failed_queries = []
    
    for i in range(1, 100):
        print(f"Downloading query {i:02d}...", end="")
        sql = download_query(i)
        
        if sql:
            cleaned_sql = clean_query(sql)
            query_key = f"q{i:02d}"
            query_name = f"TPC-DS Query {i:02d}"
            
            queries[query_key] = {
                'name': query_name,
                'sql': cleaned_sql
            }
            print(" ✓")
        else:
            failed_queries.append(i)
            print(" ✗")
    
    if failed_queries:
        print(f"\nFailed to download queries: {failed_queries}")
    
    print(f"\nSuccessfully downloaded {len(queries)} queries")
    
    # Generate Python code
    python_code = "# TPC-DS Official Queries (99 queries)\n"
    python_code += "SAMPLE_QUERIES = {\n"
    
    for query_key, query_info in queries.items():
        python_code += f"    '{query_key}': {{\n"
        python_code += f"        'name': '{query_info['name']}',\n"
        python_code += f"        'sql': '''\n{query_info['sql']}\n        '''\n"
        python_code += f"    }},\n"
    
    python_code += "}\n"
    
    return python_code

if __name__ == "__main__":
    try:
        code = generate_query_dict()
        
        # Save to file
        with open("tpcds_queries.py", "w") as f:
            f.write(code)
        
        print("\n✓ Generated tpcds_queries.py with all 99 TPC-DS queries")
        print("You can now copy the SAMPLE_QUERIES dictionary to run_queries.py")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
