-- Comprehensive feature set ofr machine learning model training
-- This model is designed to be used in the dbt marts layer

{{ config(
    materialized='table',
    docs={'description': 'Complete feature set for machine learning model training'}
) }}

WITH base_features_pr AS (
    SELECT
        player_name,
        season,
        gameweek,
        position,
        team_name,
        home_away,
        opponent_team,

        -- target variable, next week's points
        LEAD(total_points) OVER (
            PARTITION BY player_name, season
            ORDER BY gameweek
        ) AS next_gw_points,

        -- Current gw performance
        total_points as current_points,
        minutes as current_minutes,
        goals_scored as current_goals_scored,
        assists as current_assists,

        -- Financial metrics
        player_value,
        ownership_percent,
        transfers_balance,

        match_result,

        -- Fixture difficulty (place holder as we don't have this data right now)
        1.0 AS fixture_difficulty_next, -- TODO: Implement fixture difficulty calculation

        CASE WHEN minutes = 0 THEN 1 ELSE 0 END AS was_benched,
        CASE WHEN appearance_type = 'full_game' THEN 1 ELSE 0 END AS played_full_game
    
    FROM {{ ref('int_player_rolling_stats') }}
),

base_features_rs AS (
    SELECT
        player_name,
        season,
        gameweek,

        -- Form features
        avg_points_5gw,
        avg_points_3gw,
        avg_minutes_5gw,
        avg_goals_5gw,
        avg_assists_5gw,
        points_stddev_5gw,
        blank_rate_5gw,
        big_haul_rate_5gw,
        points_per_90min,
        season_points_to_date,
        season_minutes_to_date,
        games_played_to_date,

        --Season average to current point
        CASE 
            WHEN games_played_to_date > 0 
                THEN season_points_to_date::FLOAT / games_played_to_date
            ELSE 0
        END AS season_avg_points,

        CASE
            WHEN games_played_to_date > 0 
                THEN season_minutes_to_date::FLOAT / games_played_to_date
            ELSE 0
        END AS season_avg_minutes

    FROM {{ ref('int_player_rolling_stats') }}
),

base_features_tp AS (
    SELECT
        team_name, 
        season,
        gameweek,

        -- Team specific features
        team_form_5gw,
        attack_strength_5gw,
        defense_weakness_5gw,
        home_form_10gw,
        away_form_10gw

    FROM {{ ref('int_team_performance') }}
),

base_features_pb AS (
    SELECT
        player_name,
        season,
        gameweek,
        -- Position benchmarks
        points_percentile_in_position,
        avg_position_points,
        p75_position_points,
        p90_position_points

    FROM {{ ref('int_position_benchmarks') }}
),

base_features AS (
    SELECT
        bp.player_name,
        bp.season,
        bp.gameweek,
        bp.position,
        bp.team_name,

        bp.next_gw_points,

        bp.current_points,
        bp.current_minutes,
        bp.current_goals_scored,
        bp.current_assists,

        rf.avg_points_5gw,
        rf.avg_points_3gw,
        rf.avg_minutes_5gw,
        rf.season_avg_points,
        rf.season_avg_minutes,
        COALESCE(rf.points_stddev_5gw, 0) AS consistency_score,
        COALESCE(rf.blank_rate_5gw, 0) AS blank_rate,
        COALESCE(rf.big_haul_rate_5gw, 0) AS big_haul_rate,
        COALESCE(rf.points_per_90min, 0) AS points_per_90,
        rf.games_played_to_date,

        COALESCE(tf.team_form_5gw, 0) AS team_form,
        COALESCE(tf.attack_strength_5gw, 0) AS team_attack,
        COALESCE(tf.defense_weakness_5gw, 0) AS team_defense_weakness,

        COALESCE(pf.points_percentile_in_position, 0.5) AS position_percentile,
        COALESCE(pf.avg_position_points, 2) AS avg_position_points,

        COALESCE(bp.player_value, 5.0) AS player_value,
        COALESCE(bp.ownership_percent, 0.5) as ownership_pct,

        bp.was_benched,
        bp.played_full_game,
        CASE WHEN bp.home_away = 'home' THEN 1 ELSE 0 END AS is_home,

        CASE 
            WHEN rf.avg_points_3gw > rf.avg_points_5gw THEN 1
            ELSE 0
        END AS improving_form,

        CASE 
            WHEN rf.points_stddev_5gw < 2.0 THEN 1
            ELSE 0
        END AS consistent_performer,

        CASE 
            WHEN avg_points_5gw > 0
            THEN player_value / avg_points_5gw
            ELSE 999
        END AS value_per_point,

        CASE
            WHEN position = 'GK' AND avg_points_5gw > 3.0 THEN 1
            WHEN position = 'DEF' AND avg_points_5gw > 4.0 THEN 1
            WHEN position = 'MID' AND avg_points_5gw > 4.0 THEN 1
            WHEN position = 'FWD' AND avg_points_5gw > 5.0 THEN 1
            ELSE 0
        END AS above_position_threshold,

        CASE
            WHEN position = 'GK' THEN 1
            WHEN position = 'DEF' THEN 2
            WHEN position = 'MID' THEN 3
            WHEN position = 'FWD' THEN 4
            ELSE 0
        END AS position_encoded
    
    FROM base_features_pr bp 
    LEFT JOIN base_features_rs rf 
        ON bp.player_name = rf.player_name
        AND bp.season = rf.season
        AND bp.gameweek = rf.gameweek
    LEFT JOIN base_features_tp tf 
        ON bp.team_name = tf.team_name
        AND bp.season = tf.season
        AND bp.gameweek = tf.gameweek
    LEFT JOIN base_features_pb pf 
        ON bp.player_name = pf.player_name
        AND bp.season = pf.season
        AND bp.gameweek = pf.gameweek
)

SELECT *
FROM base_features
WHERE
    games_played_to_date >= 3 -- want three games for rolling stats
    AND next_gw_points IS NOT NULL -- we need a valid target
    AND gameweek <= 37 -- can't predict after the last gameweek
