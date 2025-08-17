-- Position-specific benchmarks and percentile rankings
-- This model is designed to be used in the dbt intermediate layer

{{ config(
    materialized='view',
    docs={'description': 'Position-specific performance benchmarks and player rankings'}
) }}

WITH position_stats AS (
    SELECT
        season,
        gameweek,
        position,
        AVG(total_points) AS avg_position_points,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_points) AS median_position_points,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_points) AS p75_position_points,
        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY total_points) AS p90_position_points,
        MAX(total_points) AS max_position_points,
        AVG(minutes) AS avg_position_minutes,
        COUNT(*) AS players_in_position
    
    FROM {{ ref('stg_player_gameweeks') }}
    WHERE appearance_type != 'no_appearance'
    GROUP BY season, gameweek, position
),

player_rankings AS (
    SELECT 
        p.*,
        ps.*,

        PERCENT_RANK() OVER (
            PARTITION BY p.season, p.gameweek, p.position
            ORDER BY p.total_points DESC
        ) AS points_percentile_in_position,

        PERCENT_RANK() OVER (
            PARTITION BY p.season, p.gameweek, p.position
            ORDER BY p.minutes DESC
        ) AS minutes_percentile_in_position
    
    FROM {{ ref('stg_player_gameweeks') }} p
    JOIN position_stats ps 
        ON p.season = ps.season
        AND p.gameweek = ps.gameweek
        AND p.position = ps.position
    WHERE p.appearance_type != 'no_appearance'
)

SELECT * FROM player_rankings