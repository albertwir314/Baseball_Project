import sqlite3
import re

def combine_tables_with_union(db_path):
    """
    Combine tables with pattern YYYYsuffix using CREATE TABLE AS SELECT with UNION ALL.
    Properly handles table names that start with numbers.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get all table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]
    
    # Group tables by suffix
    suffix_groups = {}
    for table in tables:
        if re.match(r'^\d{4}.+$', table):  # Tables starting with 4 digits
            suffix = table[4:]
            if suffix not in suffix_groups:
                suffix_groups[suffix] = []
            suffix_groups[suffix].append(table)
    
    # Process each suffix group
    for suffix, year_tables in suffix_groups.items():
        if len(year_tables) < 2:
            print(f"Skipping '{suffix}' - only 1 table found")
            continue
        
        combined_table = suffix
        print(f"\nCombining {len(year_tables)} tables into '{combined_table}': {year_tables}")
        
        # 1. Drop the combined table if it exists
        cursor.execute(f"DROP TABLE IF EXISTS \"{combined_table}\"")
        
        # 2. Build the UNION ALL query for all tables
        union_parts = []
        for table in year_tables:
            year = table[:4]
            # Use quoted table name in PRAGMA statement
            cursor.execute(f'PRAGMA table_info("{table}")')
            columns = [col[1] for col in cursor.fetchall()]
            columns_str = ', '.join(f'"{col}"' for col in columns)  # Quote column names too
            
            # Create SELECT statement for this table with year literal
            union_part = f'SELECT {columns_str}, {year} AS source_year FROM "{table}"'
            union_parts.append(union_part)
        
        full_union = " UNION ALL ".join(union_parts)
        
        # 3. Create the new table with all data in one operation
        create_sql = f"""
        CREATE TABLE "{combined_table}" AS
        {full_union}
        """
        cursor.execute(create_sql)
        print(f"Created {combined_table} with all combined data")
        
        # 4. Verify counts
        cursor.execute(f'SELECT COUNT(*) FROM "{combined_table}"')
        total_rows = cursor.fetchone()[0]
        print(f"Total rows in {combined_table}: {total_rows}")
    
    conn.commit()
    conn.close()
    print("\nAll tables processed successfully")

if __name__ == "__main__":
    database_path = "baseball-database.db"  # Change to your DB path
    combine_tables_with_union(database_path)