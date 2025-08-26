-- Team mapping table to resolve team IDs to names
-- Creates a consistent mapping of team_id (1-20) to team_name for each season

{{ config(
    materialized='table',
    docs={'description': 'Team ID to name mapping for each season (1-20 teams per seaason)'}
) }}

WITH team_id_mappping AS (
    
)