-- Team information and mappings
-- This model is designed to be used in the dbt staging layer

{{ config(
    materialized='view',
    docs={'description': 'Distinct team information across all seasons'}
) }}

WITH team_data AS (
    SELECT DISTINCT 
        team AS team_name,
        season
    FROM {{ source('fpl_raw', 'player_gameweeks') }}
    WHERE team IS NOT NULL
),

team_stats AS (
    SELECT 
        team_name,
        season,
        COUNT(DISTINCT name) AS squad_size,
        COUNT(DISTINCT "GW") AS gameweeks_played,
        AVG(total_points) AS avg_team_points
    FROM {{ source('fpl_raw', 'player_gameweeks') }}
    WHERE team IS NOT NULL
    GROUP BY team_name, season
)

SELECT
    t.team_name,
    t.season,
    ts.squad_size,
    ts.gameweeks_played,
    ts.avg_team_points
FROM team_data t
LEFT JOIN team_stats ts 
    ON t.team_name = ts.team_name
    AND t.season = ts.season