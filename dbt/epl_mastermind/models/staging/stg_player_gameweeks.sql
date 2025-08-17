-- Core staging moel for player performance data each gameweek
-- This model is designed to be used in the dbt staging layer

{{ config(
    materialized='view',
    docs={'description': 'Cleaned and standardized player gameweek performance data from FPL API'}
) }}

WITH raw_data AS (
    SELECT * FROM {{ source('fpl_raw', 'player_gameweeks') }}
),

cleaned_data AS (
    SELECT
        -- Player information
        name AS player_name,
        element AS player_id,
        position,
        team AS team_name,
        -- Gameweek context
        season,
        "GW" AS gameweek,
        fixture AS fixture_id,
        opponent_team,
        CASE
            WHEN was_home = 1 THEN 'home'
            WHEN was_home = 0 THEN 'away'
            ELSE 'unknown'
        END as home_away,
        kickoff_time,
        -- Core metrics which will always be available (might want to look into more specific stats like shots, successful dribbles, etc)
        COALESCE(total_points, 0) AS total_points,
        COALESCE(minutes, 0) AS minutes,
        COALESCE(goals_scored, 0) AS goals_scored,
        COALESCE(assists, 0) AS assists,
        COALESCE(clean_sheets, 0) AS clean_sheets,
        COALESCE(goals_conceded, 0) AS goals_conceded,
        COALESCE(own_goals, 0) AS own_goals,
        COALESCE(penalties_saved, 0) AS penalties_saved,
        COALESCE(penalties_missed, 0) AS penalties_missed,
        COALESCE(yellow_cards, 0) AS yellow_cards,
        COALESCE(red_cards, 0) AS red_cards,
        COALESCE(saves, 0) AS saves,
        COALESCE(bonus, 0) AS bonus,
        -- TODO: Add advanced metrics that were added in 22/23 season 
        -- expected_goals,
        -- expected_assists,
        -- expected_goal_involvements,
        -- expected_goals_conceded,
        -- starts,
        
        -- ICT
        COALESCE(influence, 0) AS influence,
        COALESCE(creativity, 0) AS creativity,
        COALESCE(threat, 0) AS threat,
        COALESCE(ict_index, 0) AS ict_index,
        COALESCE(bps, 0) AS bps,
        -- Value from regular draft (could be useful for herd decisions, but less relevant for draft style)
        value AS player_value,
        selected AS ownership_percent,
        transfers_in,
        transfers_out,
        transfers_balance,
        -- Match result context
        team_h_score,
        team_a_score,
        --Data quality flags
        CASE 
            WHEN minutes > 90 THEN true 
            ELSE false
        END as has_invalid_minutes,

        CASE
            WHEN total_points < 0 THEN true 
            ELSE false
        END as has_negative_points,
        -- Derived fields
        CASE
            WHEN minutes >= 60 THEN 'full_game'
            WHEN minutes >= 30 THEN 'parital_game'
            WHEN minutes > 15 THEN 'brief_appearance'
            ELSE 'no_appearance'
        END AS appearance_type,

        CASE 
            WHEN was_home = true AND team_h_score > team_a_score THEN 'win'
            WHEN was_home = false AND team_a_score > team_h_score THEN 'win'
            WHEN team_h_score = team_a_score THEN 'draw'
            ELSE 'loss'
        END AS match_result

    FROM raw_data
    WHERE 
        name IS NOT NULL
        AND position IS NOT NULL
        AND team IS NOT NULL
        AND "GW" IS NOT NULL
        AND season IS NOT NULL
)

SELECT * FROM cleaned_data