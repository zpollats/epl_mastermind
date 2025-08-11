"""
FPL Historical Data Exploration
Explores the vaastav/Fantasy-Premier-League dataset structure
"""

import pandas as pd
import duckdb
from pathlib import Path

# Base URL for the historical data
BASE_URL = "https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data"

# Seasons to load (last 5 seasons)
SEASONS = ["2019-20", "2020-21", "2021-22", "2022-23", "2023-24"]

def explore_season_structure(season: str):
    """Explore the data structure for a given season"""
    print(f"\n=== Exploring {season} ===")
    
    # Check merged_gws.csv - this will be our primary data source
    url = f"{BASE_URL}/{season}/gws/merged_gw.csv"
    
    try:
        df = pd.read_csv(url)
        print(f"Merged GW data shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        print(f"Date range: GW {df['GW'].min()} to GW {df['GW'].max()}")
        print(f"Unique players: {df['name'].nunique()}")
        
        # Show sample data
        print("\nSample data:")
        print(df.head(3)[['name', 'position', 'team', 'GW', 'total_points', 'minutes', 'goals_scored', 'assists']].to_string())
        
        return df
        
    except Exception as e:
        print(f"Error loading {season}: {e}")
        return None

def check_data_consistency():
    """Check data consistency across seasons"""
    print("\n=== Data Consistency Check ===")
    
    all_columns = set()
    season_info = {}
    
    for season in SEASONS:
        url = f"{BASE_URL}/{season}/gws/merged_gw.csv"
        try:
            df = pd.read_csv(url)
            all_columns.update(df.columns)
            season_info[season] = {
                'columns': set(df.columns),
                'shape': df.shape,
                'players': df['name'].nunique(),
                'gameweeks': df['GW'].nunique()
            }
        except Exception as e:
            print(f"Failed to load {season}: {e}")
    
    print(f"Total unique columns across all seasons: {len(all_columns)}")
    
    # Check for column consistency
    base_columns = season_info[SEASONS[0]]['columns'] if SEASONS[0] in season_info else set()
    for season, info in season_info.items():
        missing = base_columns - info['columns']
        extra = info['columns'] - base_columns
        if missing or extra:
            print(f"{season}: Missing {missing}, Extra {extra}")
        else:
            print(f"{season}: Columns consistent ✓")

def setup_duckdb_database():
    """Create initial DuckDB database and load sample data"""
    print("\n=== Setting up DuckDB ===")
    
    # Create data directory
    Path("data").mkdir(exist_ok=True)
    
    # Connect to DuckDB
    conn = duckdb.connect("data/fpl.db")
    
    # Load one season as a test
    test_season = "2023-24"
    url = f"{BASE_URL}/{test_season}/gws/merged_gw.csv"
    
    try:
        # Load directly from URL into DuckDB
        conn.execute(f"""
        CREATE OR REPLACE TABLE player_gameweeks AS 
        SELECT * FROM read_csv_auto('{url}')
        """)
        
        # Basic queries to verify data
        result = conn.execute("SELECT COUNT(*) as total_rows FROM player_gameweeks").fetchone()
        print(f"Loaded {result[0]} rows into DuckDB")
        
        result = conn.execute("SELECT COUNT(DISTINCT name) as unique_players FROM player_gameweeks").fetchone()
        print(f"Unique players: {result[0]}")
        
        result = conn.execute("SELECT MIN(GW), MAX(GW) FROM player_gameweeks").fetchone()
        print(f"Gameweek range: {result[0]} to {result[1]}")
        
        # Show sample of high-scoring gameweeks
        print("\nTop 5 individual gameweek performances:")
        result = conn.execute("""
        SELECT name, team, position, GW, total_points, minutes, goals_scored, assists
        FROM player_gameweeks 
        WHERE total_points > 0
        ORDER BY total_points DESC 
        LIMIT 5
        """).fetchall()
        
        for row in result:
            print(f"{row[0]} ({row[1]}, {row[2]}) - GW{row[3]}: {row[4]} pts")
    
    except Exception as e:
        print(f"Error setting up DuckDB: {e}")
    
    finally:
        conn.close()

def main():
    """Main exploration function"""
    print("FPL Historical Data Exploration")
    print("=" * 40)
    
    # Explore structure for each season
    for season in SEASONS[-2:]:  # Just check last 2 seasons initially
        explore_season_structure(season)
    
    # Check consistency
    check_data_consistency()
    
    # Setup DuckDB
    setup_duckdb_database()
    
    print("\n" + "=" * 40)
    print("Exploration complete!")
    print("Next steps:")
    print("1. Review data structure and quality")
    print("2. Design dbt staging models")
    print("3. Create historical data loader")

if __name__ == "__main__":
    main()
