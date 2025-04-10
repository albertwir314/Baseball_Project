import os
import sqlite3
import pandas as pd
from glob import glob

def csv_to_sqlite(csv_directory, db_path, if_exists="replace"):
    """
    Reads all CSV files in a directory and uploads each as a separate table to SQLite database.
    
    Args:
        csv_directory (str): Path to directory containing CSV files
        db_path (str): Path to SQLite database file (will be created if doesn't exist)
        if_exists (str): What to do if table exists ('fail', 'replace', 'append')
    
    Returns:
        dict: Summary of processed files with table names and row counts
    """
    result = {
        'success': [],
        'skipped': [],
        'failed': []
    }
    
    try:
        # Connect to SQLite database
        conn = sqlite3.connect(db_path)
        
        # Find all CSV files in the directory
        csv_files = glob(os.path.join(csv_directory, "*.csv"))
        print(csv_files)
        
        if not csv_files:
            print(f"No CSV files found in: {csv_directory}")
            return result
        
        for csv_file in csv_files:
            base_name = os.path.basename(csv_file)
            table_name = os.path.splitext(base_name)[0]
            
            # Clean table name (SQLite compatible)
            table_name = ''.join(c if c.isalnum() else '_' for c in table_name)
            
            try:
                # Read CSV (with low_memory=False to avoid mixed type warnings)
                df = pd.read_csv(csv_file, low_memory=False)
                
                if df.empty:
                    result['skipped'].append({
                        'file': base_name,
                        'reason': 'Empty CSV'
                    })
                    continue
                
                # Upload to SQLite
                df.to_sql(
                    name=table_name,
                    con=conn,
                    if_exists=if_exists,
                    index=False
                )
                
                row_count = len(df)
                result['success'].append({
                    'file': base_name,
                    'table': table_name,
                    'rows': row_count
                })
                
                print(f"Success: {base_name} → {table_name} ({row_count} rows)")
                
            except pd.errors.EmptyDataError:
                result['skipped'].append({
                    'file': base_name,
                    'reason': 'Empty file'
                })
            except Exception as e:
                result['failed'].append({
                    'file': base_name,
                    'error': str(e)
                })
                print(f"Error processing {base_name}: {e}")
        
        # Commit changes and close connection
        conn.commit()
        
    except Exception as e:
        print(f"Database connection error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
    
    # Print summary
    print("\nProcessing summary:")
    print(f"Successfully loaded: {len(result['success'])} files")
    print(f"Skipped: {len(result['skipped'])} files")
    print(f"Failed: {len(result['failed'])} files")
    
    return result


list_years = [2024, 2023, 2022, 2021, 2020]
for i in list_years:
    csv_to_sqlite(
        csv_directory=f"./output_data/{i}", 
        db_path="./baseball-database.db",
        if_exists="replace"
    )