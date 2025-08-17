-- Player summary statistics for analysis and reporting
-- This model is designed to be used in the dbt marts layer

{{ config(
    materialized='table',
    docs={'description': 'Player summary statistics aggregated by season'}
) }}

SELECT 
    player_name,
    position,
    season,
    team_name,

    -- Performance summary
    COUNT(*) AS games_played,
    SUM(total_points) AS total_points,
    AVG(total_points) AS avg_points_per_game,
    SUM(minutes) AS total_minutes,
    AVG(minutes) AS avg_minutes_per_game,

    -- Goal Contributions
    SUM(goals_scored) AS total_goals,
    SUM(assists) AS total_assists,
    SUM(goals_scored + assists) AS total_goal_contributions,

    -- Consistency metrics
    STDDEV(total_points) AS points_consistency,
    MIN(total_points) AS worst_performance,
    MAX(total_points) AS best_performance,

    -- Clean sheets (Mostly relevent for GK and DEF)
    SUM(clean_sheets) AS total_clean_sheets,

    -- Financial summary
    AVG(player_value) AS avg_value,
    AVG(ownership_percent) AS avg_ownership,

    -- Performance categories
    SUM(CASE WHEN total_points <= 2 THEN 1 ELSE 0 END) AS blanks,
    SUM(CASE WHEN total_points >= 10 THEN 1 ELSE 0 END) AS big_hauls,
    SUM(CASE WHEN total_points >= 15 THEN 1 ELSE 0 END) AS huge_hauls,

    -- Rates
    AVG(CASE WHEN total_points <= 2 THEN 1 ELSE 0 END) AS blank_rate,
    AVG(CASE WHEN total_points >= 10 THEN 1 ELSE 0 END) AS big_haul_rate
FROM {{ ref('stg_player_gameweeks') }}
WHERE appearance_type != 'no_appearance'
GROUP BY player_name, position, season, team_name