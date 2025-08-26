"""
Analyze the opponent_team field structure to understand team ID mapping
This will help us create the correct opponent strength calculations
"""

import duckdb
import pandas as pd

# Database connection
DB_PATH = "data/fpl_complete.db"

def analyze_opponent_field():
    """Analyze the structure of opponent_team field"""
    print("🔍 Analyzing opponent_team Field Structure")
    print("=" * 50)
    
    conn = duckdb.connect(DB_PATH)
    
    # Basic opponent_team analysis
    print("📊 Basic opponent_team Statistics:")
    basic_stats = conn.execute("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(opponent_team) as records_with_opponent,
            COUNT(DISTINCT opponent_team) as unique_opponent_values,
            MIN(opponent_team) as min_opponent_id,
            MAX(opponent_team) as max_opponent_id
        FROM player_gameweeks
    """).fetchone()
    
    print(f"  Total records: {basic_stats[0]:,}")
    print(f"  Records with opponent: {basic_stats[1]:,} ({basic_stats[1]/basic_stats[0]*100:.1f}%)")
    print(f"  Unique opponent values: {basic_stats[2]}")
    print(f"  Opponent ID range: {basic_stats[3]} - {basic_stats[4]}")
    
    # Check opponent_team distribution by season
    print("\n📅 opponent_team Distribution by Season:")
    season_dist = conn.execute("""
        SELECT 
            season,
            COUNT(DISTINCT opponent_team) as unique_opponents,
            MIN(opponent_team) as min_id,
            MAX(opponent_team) as max_id,
            COUNT(*) as total_records
        FROM player_gameweeks
        WHERE opponent_team IS NOT NULL
        GROUP BY season
        ORDER BY season
    """).fetchall()
    
    for season, unique_ops, min_id, max_id, records in season_dist:
        print(f"  {season}: {unique_ops} unique opponents ({min_id}-{max_id}), {records:,} records")
    
    # Sample data to understand the relationship
    print("\n🔍 Sample Data Analysis:")
    sample_data = conn.execute("""
        SELECT 
            season,
            team,
            opponent_team,
            was_home,
            COUNT(*) as occurrences
        FROM player_gameweeks
        WHERE opponent_team IS NOT NULL
        GROUP BY season, team, opponent_team, was_home
        ORDER BY season, team, opponent_team
        LIMIT 20
    """).fetchall()
    
    print("  Sample team vs opponent_team relationships:")
    for season, team, opp_team, was_home, count in sample_data[:10]:
        home_str = "home" if was_home else "away"
        print(f"    {season}: {team} vs opponent_id {opp_team} ({home_str}) - {count} records")
    
    # Try to infer team ID patterns
    print("\n🎯 Team ID Pattern Analysis:")
    
    # Look for patterns in home vs away games
    pattern_analysis = conn.execute("""
        WITH team_opponent_patterns AS (
            SELECT 
                season,
                team,
                opponent_team,
                was_home,
                COUNT(*) as game_count
            FROM player_gameweeks
            WHERE opponent_team IS NOT NULL
            GROUP BY season, team, opponent_team, was_home
        )
        SELECT 
            season,
            team,
            COUNT(DISTINCT opponent_team) as different_opponents,
            COUNT(DISTINCT CASE WHEN was_home = 1 THEN opponent_team END) as home_opponents,
            COUNT(DISTINCT CASE WHEN was_home = 0 THEN opponent_team END) as away_opponents
        FROM team_opponent_patterns
        GROUP BY season, team
        ORDER BY season, team
        LIMIT 10
    """).fetchall()
    
    print("  Teams and their opponent patterns:")
    for season, team, diff_ops, home_ops, away_ops in pattern_analysis:
        print(f"    {season} {team}: {diff_ops} different opponents ({home_ops} home, {away_ops} away)")
    
    # Check if opponent_team correlates with anything predictable
    print("\n🔗 Correlation Analysis:")
    
    # See if we can find a consistent mapping
    mapping_attempt = conn.execute("""
        WITH potential_mapping AS (
            SELECT DISTINCT
                season,
                team as team_name,
                opponent_team,
                COUNT(*) OVER (PARTITION BY season, team, opponent_team) as frequency
            FROM player_gameweeks
            WHERE opponent_team IS NOT NULL
        ),
        team_list AS (
            SELECT DISTINCT
                season,
                team as team_name,
                ROW_NUMBER() OVER (PARTITION BY season ORDER BY team) as expected_team_id
            FROM player_gameweeks
            WHERE team IS NOT NULL
        )
        SELECT 
            tl.season,
            tl.team_name,
            tl.expected_team_id,
            COUNT(DISTINCT pm.opponent_team) as opponent_ids_seen,
            MIN(pm.opponent_team) as min_opp_id,
            MAX(pm.opponent_team) as max_opp_id
        FROM team_list tl
        LEFT JOIN potential_mapping pm ON tl.season = pm.season AND tl.team_name = pm.team_name
        GROUP BY tl.season, tl.team_name, tl.expected_team_id
        ORDER BY tl.season, tl.expected_team_id
        LIMIT 15
    """).fetchall()
    
    print("  Potential team ID mapping:")
    for season, team_name, expected_id, opp_ids_seen, min_opp, max_opp in mapping_attempt:
        print(f"    {season} {team_name} (expected ID {expected_id}): sees opponent IDs {min_opp}-{max_opp}")
    
    conn.close()

def test_reverse_mapping():
    """Test if we can reverse-engineer the team mapping"""
    print("\n" + "=" * 50)
    print("🔄 Reverse Engineering Team Mapping")
    print("=" * 50)
    
    conn = duckdb.connect(DB_PATH)
    
    # Try to find the actual team mapping by looking at fixture patterns
    print("🎯 Attempting to map opponent_team IDs to team names...")
    
    # Strategy: For each team, see which opponent_team values they face
    # Then cross-reference with which teams they should be playing against
    
    reverse_mapping = conn.execute("""
        WITH all_fixtures AS (
            SELECT DISTINCT
                season,
                team as team_name,
                opponent_team as opponent_id,
                was_home
            FROM player_gameweeks
            WHERE opponent_team IS NOT NULL
        ),
        team_seasons AS (
            SELECT DISTINCT
                season,
                team as team_name
            FROM player_gameweeks
            WHERE team IS NOT NULL
        ),
        -- For each team, count how many times they see each opponent_id
        opponent_frequency AS (
            SELECT 
                af.season,
                af.team_name,
                af.opponent_id,
                COUNT(*) as times_faced
            FROM all_fixtures af
            GROUP BY af.season, af.team_name, af.opponent_id
        )
        SELECT 
            season,
            team_name,
            COUNT(DISTINCT opponent_id) as unique_opponents_faced,
            MIN(opponent_id) as lowest_opponent_id,
            MAX(opponent_id) as highest_opponent_id,
            STRING_AGG(CAST(opponent_id AS VARCHAR), ', ' ORDER BY opponent_id) as opponent_ids
        FROM opponent_frequency
        GROUP BY season, team_name
        ORDER BY season, team_name
        LIMIT 10
    """).fetchall()
    
    print("  Teams and the opponent IDs they face:")
    for season, team, unique_ops, min_id, max_id, opp_ids in reverse_mapping:
        opp_ids_short = opp_ids[:50] + "..." if len(opp_ids) > 50 else opp_ids
        print(f"    {season} {team}: faces {unique_ops} opponents (IDs: {opp_ids_short})")
    
    conn.close()

def suggest_mapping_strategy():
    """Suggest the best strategy for creating team mapping"""
    print("\n" + "=" * 50)
    print("💡 Recommended Mapping Strategy")
    print("=" * 50)
    
    print("Based on the analysis, here's the recommended approach:")
    print()
    print("1. **Create alphabetical team mapping per season**:")
    print("   - Sort teams alphabetically within each season")
    print("   - Assign IDs 1-20 based on alphabetical order")
    print("   - This gives consistent, predictable team IDs")
    print()
    print("2. **Build reverse lookup for opponent_team field**:")
    print("   - Map the actual opponent_team values to our clean team names") 
    print("   - Handle any inconsistencies in the source data")
    print()
    print("3. **Validate the mapping**:")
    print("   - Ensure each team plays against exactly 19 others")
    print("   - Check home/away balance")
    print("   - Verify fixture count consistency")
    print()
    print("🚀 Ready to implement the corrected opponent strength model?")

def main():
    """Run complete opponent field analysis"""
    analyze_opponent_field()
    test_reverse_mapping()
    suggest_mapping_strategy()

if __name__ == "__main__":
    main()