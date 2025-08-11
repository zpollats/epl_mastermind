"""
Complete Historical FPL Data Loader
Handles both merged files and individual gameweek files to create complete dataset
"""

import pandas as pd
import duckdb
from pathlib import Path
import requests
from typing import List, Optional, Tuple
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
BASE_URL = "https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data"
COMPLETE_SEASONS = ["2020-21", "2021-22", "2022-23", "2023-24"]  # These have complete merged files
PARTIAL_SEASONS = ["2024-25"]  # These need reconstruction
DATABASE_PATH = "data/fpl_complete.db"

class FPLDataLoader:
    """Handles loading and combining FPL historical data"""
    
    def __init__(self, database_path: str = DATABASE_PATH):
        self.database_path = database_path
        self.ensure_data_directory()
        
    def ensure_data_directory(self):
        """Create data directory if it doesn't exist"""
        Path("data").mkdir(exist_ok=True)
        
    def load_merged_season(self, season: str) -> Optional[pd.DataFrame]:
        """Load a complete season from merged_gw.csv"""
        url = f"{BASE_URL}/{season}/gws/merged_gw.csv"
        logger.info(f"Loading merged data for {season}")
        
        try:
            df = pd.read_csv(url, on_bad_lines='skip')
            df['season'] = season
            logger.info(f"✅ Loaded {len(df)} rows for {season}")
            return df
        except Exception as e:
            logger.error(f"❌ Failed to load {season}: {e}")
            return None
    
    def check_available_gameweeks(self, season: str) -> List[int]:
        """Check which individual gameweek files are available for a season"""
        available_gws = []
        
        # Check GW 1-38 (full season)
        for gw in range(1, 39):
            url = f"{BASE_URL}/{season}/gws/gw{gw}.csv"
            try:
                response = requests.head(url, timeout=10)
                if response.status_code == 200:
                    available_gws.append(gw)
            except:
                continue
                
        logger.info(f"Available individual GW files for {season}: {len(available_gws)} gameweeks")
        return available_gws
    
    def load_individual_gameweek(self, season: str, gameweek: int) -> Optional[pd.DataFrame]:
        """Load an individual gameweek file"""
        url = f"{BASE_URL}/{season}/gws/gw{gameweek}.csv"
        
        try:
            df = pd.read_csv(url, on_bad_lines='skip')
            df['season'] = season
            df['GW'] = gameweek  # Ensure GW column exists
            return df
        except Exception as e:
            logger.warning(f"Failed to load {season} GW{gameweek}: {e}")
            return None
    
    def reconstruct_complete_season(self, season: str) -> Optional[pd.DataFrame]:
        """Reconstruct a complete season from individual gameweek files"""
        logger.info(f"Reconstructing complete season for {season}")
        
        # First, try to get the merged file for early gameweeks
        merged_df = self.load_merged_season(season)
        
        # Check what gameweeks are available individually
        available_gws = self.check_available_gameweeks(season)
        
        if not available_gws:
            logger.warning(f"No individual gameweek files found for {season}")
            return merged_df
        
        # Determine strategy based on what we have
        if merged_df is not None:
            merged_gws = sorted(merged_df['GW'].unique())
            missing_gws = [gw for gw in available_gws if gw not in merged_gws]
            logger.info(f"Merged file has GWs {min(merged_gws)}-{max(merged_gws)}, loading {len(missing_gws)} additional GWs")
        else:
            missing_gws = available_gws
            logger.info(f"No merged file, loading all {len(missing_gws)} individual GWs")
        
        # Load missing gameweeks
        individual_dfs = []
        for gw in missing_gws:
            gw_df = self.load_individual_gameweek(season, gw)
            if gw_df is not None:
                individual_dfs.append(gw_df)
        
        # Combine everything
        all_dfs = []
        if merged_df is not None:
            all_dfs.append(merged_df)
        all_dfs.extend(individual_dfs)
        
        if all_dfs:
            # Find common columns across all dataframes
            common_columns = set.intersection(*[set(df.columns) for df in all_dfs])
            logger.info(f"Common columns across all data: {len(common_columns)}")
            
            # Combine using only common columns
            combined_df = pd.concat([df[list(common_columns)] for df in all_dfs], ignore_index=True)
            
            # Remove duplicates (in case of overlap between merged and individual files)
            combined_df = combined_df.drop_duplicates(subset=['name', 'GW', 'season'], keep='first')
            
            logger.info(f"✅ Complete {season}: {len(combined_df)} rows, GWs {combined_df['GW'].min()}-{combined_df['GW'].max()}")
            return combined_df
        
        return None
    
    def load_all_historical_data(self) -> pd.DataFrame:
        """Load all historical data using the appropriate method for each season"""
        logger.info("🚀 Starting complete historical data load")
        
        all_season_data = []
        
        # Load complete seasons from merged files
        for season in COMPLETE_SEASONS:
            df = self.load_merged_season(season)
            if df is not None:
                all_season_data.append(df)
        
        # Reconstruct partial seasons
        for season in PARTIAL_SEASONS:
            df = self.reconstruct_complete_season(season)
            if df is not None:
                all_season_data.append(df)
        
        if not all_season_data:
            raise ValueError("No data loaded successfully")
        
        # Find common columns across all seasons
        common_columns = set.intersection(*[set(df.columns) for df in all_season_data])
        logger.info(f"Final common columns: {len(common_columns)}")
        
        # Combine all seasons
        final_df = pd.concat([df[list(common_columns)] for df in all_season_data], ignore_index=True)
        
        # Data quality summary
        logger.info(f"📊 Complete Dataset Summary:")
        logger.info(f"   Total rows: {len(final_df):,}")
        logger.info(f"   Seasons: {sorted(final_df['season'].unique())}")
        logger.info(f"   Unique players: {final_df['name'].nunique():,}")
        logger.info(f"   Gameweek range: {final_df['GW'].min()}-{final_df['GW'].max()}")
        
        # Check season completeness
        for season in final_df['season'].unique():
            season_data = final_df[final_df['season'] == season]
            gw_range = f"{season_data['GW'].min()}-{season_data['GW'].max()}"
            gw_count = season_data['GW'].nunique()
            logger.info(f"   {season}: {len(season_data):,} rows, GWs {gw_range} ({gw_count} unique)")
        
        return final_df
    
    def save_to_database(self, df: pd.DataFrame):
        """Save the complete dataset to DuckDB"""
        logger.info(f"💾 Saving {len(df):,} rows to database: {self.database_path}")
        
        conn = duckdb.connect(self.database_path)
        
        try:
            # Register the dataframe and create table
            conn.register('complete_data', df)
            
            conn.execute("""
            CREATE OR REPLACE TABLE player_gameweeks AS 
            SELECT * FROM complete_data
            """)
            
            # Create indexes for better query performance
            conn.execute("CREATE INDEX IF NOT EXISTS idx_player_season_gw ON player_gameweeks(name, season, GW)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_season_gw ON player_gameweeks(season, GW)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_team_season ON player_gameweeks(team, season)")
            
            # Verify the data
            stats = conn.execute("""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(DISTINCT name) as unique_players,
                COUNT(DISTINCT season) as seasons,
                COUNT(DISTINCT CONCAT(season, '-', GW)) as unique_gameweeks
            FROM player_gameweeks
            """).fetchone()
            
            logger.info(f"✅ Database created successfully:")
            logger.info(f"   Total rows: {stats[0]:,}")
            logger.info(f"   Unique players: {stats[1]:,}")
            logger.info(f"   Seasons: {stats[2]}")
            logger.info(f"   Unique gameweeks: {stats[3]:,}")
            
        except Exception as e:
            logger.error(f"❌ Database error: {e}")
            raise
        finally:
            conn.close()
    
    def run_complete_load(self):
        """Execute the complete historical data loading process"""
        try:
            # Load all data
            complete_df = self.load_all_historical_data()
            
            # Save to database
            self.save_to_database(complete_df)
            
            logger.info("🎉 Complete historical data load finished successfully!")
            logger.info(f"📁 Database saved to: {self.database_path}")
            logger.info("🎯 Ready for dbt transformations!")
            
        except Exception as e:
            logger.error(f"💥 Load failed: {e}")
            raise

def main():
    """Main execution function"""
    print("🚀 FPL Complete Historical Data Loader")
    print("=" * 50)
    
    loader = FPLDataLoader()
    loader.run_complete_load()
    
    print("\n" + "=" * 50)
    print("✅ Ready for next steps:")
    print("1. Set up dbt and run staging models")
    print("2. Design feature engineering models")
    print("3. Prepare for live API integration")

if __name__ == "__main__":
    main()