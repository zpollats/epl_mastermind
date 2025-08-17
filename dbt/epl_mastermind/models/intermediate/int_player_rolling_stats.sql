-- Calculate rolling averages and form metrics for player performance
-- This model is designed to be used in the dbt intermediate layer

{{ config(
    materialized='view',
    docs={'description': 'Rolling statistics for players over specifid windows'}
) }}

WITH player_gameweeks AS (
    SELECT * FROM {{ ref('stg_player_gameweeks') }}
    WHERE appearance_type != 'no_appearance'
),

rolling_stats AS (
    SELECT 
        *,
        AVG(total_points) OVER (
            PARTITION BY player_name, season
            ORDER BY gameweek 
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS avg_points_5gw,
        
        AVG(minutes) OVER (
            PARTITION BY player_name, season
            ORDER BY gameweek 
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS avg_minutes_5gw,
        
        AVG(goals_scored) OVER (
            PARTITION BY player_name, season
            ORDER BY gameweek 
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS avg_goals_5gw,

        AVG(assists) OVER (
            PARTITION BY player_name, season
            ORDER BY gameweek 
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS avg_assists_5gw,

        AVG(total_points) OVER (
            PARTITION BY player_name, season
            ORDER BY gameweek 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS avg_points_3gw,

        SUM(total_points) OVER (
            PARTITION BY player_name, season
            ORDER BY gameweek
            ROWS UNBOUNDED PRECEDING
        ) AS season_points_to_date,

        SUM(minutes) OVER (
            PARTITION BY player_name, season
            ORDER BY gameweek
            ROWS UNBOUNDED PRECEDING
        ) AS season_minutes_to_date,

        COUNT(*) OVER (
            PARTITION BY player_name, season
            ORDER BY gameweek
            ROWS UNBOUNDED PRECEDING
        ) AS games_played_to_date,
        
        CASE 
            WHEN minutes > 0 THEN total_points::FLOAT / minutes * 90
            ELSE 0
        END AS points_per_90min

    FROM player_gameweeks
),

consistency_metrics AS (
    SELECT 
        *,
        STDDEV(total_points) OVER (
            PARTITION BY player_name, season
            ORDER BY gameweek
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS points_stddev_5gw,

        AVG(CASE WHEN total_points <= 2 THEN 1 ELSE 0 END) OVER (
            PARTITION BY player_name, season
            ORDER BY gameweek 
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS blank_rate_5gw,

        AVG(CASE when total_points >= 10 THEN 1 ELSE 0 END) OVER (
            PARTITION BY player_name, season
            ORDER BY gameweek 
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS big_haul_rate_5gw

    FROM rolling_stats
)

SELECT * FROM consistency_metrics