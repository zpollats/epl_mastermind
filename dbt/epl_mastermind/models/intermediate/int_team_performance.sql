-- Team-level performance metrics and strength indicators
-- This model is designed to be used in the dbt intermediate layer

{{ config(
    materialized='view',
    docs={'description': 'Team performance metrics and relative strength indicators'}
) }}

WITH team_gameweeks AS (
    SELECT
        team_name, 
        season,
        gameweek,
        home_away,
        match_result,
        COUNT(*) AS players_used,
        SUM(total_points) AS team_total_points,
        SUM(goals_scored) AS team_goals,
        SUM(goals_conceded) AS team_goals_conceded,
        SUM(clean_sheets) AS team_clean_sheets,
        AVG(total_points) AS avg_player_points
    FROM {{ ref('stg_player_gameweeks') }}
    WHERE appearance_type != 'no_appearance'
    GROUP BY team_name, season, gameweek, home_away, match_result
),

team_strength AS (
    SELECT
        team_name,
        season,
        gameweek,
        *,
        -- Rolling team performances
        AVG(team_total_points) OVER (
            PARTITION BY team_name, season
            ORDER BY gameweek
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS team_form_5gw,

        AVG(team_goals) OVER (
            PARTITION BY team_name, season
            ORDER BY gameweek
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS attack_strength_5gw,

        AVG(team_goals_conceded) OVER (
            PARTITION BY team_name, season
            ORDER BY gameweek
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS defense_weakness_5gw,
        
        AVG(CASE WHEN home_away = 'home' THEN team_total_points END) OVER (
            PARTITION BY team_name, season
            ORDER BY gameweek
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS home_form_10gw,

        AVG(CASE WHEN home_away = 'away' THEN team_total_points END) OVER (
            PARTITION BY team_name, season
            ORDER BY gameweek
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS away_form_10gw

    FROM team_gameweeks
)

SELECT * FROM team_strength 