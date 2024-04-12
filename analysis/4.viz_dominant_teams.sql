-- Databricks notebook source

CREATE OR REPLACE TEMP VIEW v_dominant_teams
AS
SELECT team_name,
        COUNT(1) as total_races,
        SUM(calculated_Points) as total_points,
        avg(calculated_Points) as avg_points,
        RANK() OVER(ORDER BY avg(calculated_Points) DESC) team_rank
      FROM f1_presentation.calculated_race_results
    GROUP BY team_name
    HAVING count(1) >=100
    ORDER BY avg_points DESC;

-- COMMAND ----------

SELECT team_name,
        race_year,
        COUNT(1) as total_races,
        SUM(calculated_Points) as total_points,
        avg(calculated_Points) as avg_points
      FROM f1_presentation.calculated_race_results
    WHERE team_name IN (SELECT team_name FROM v_dominant_teams WHERE team_rank <=5)
    GROUP BY race_year, team_name
    ORDER BY race_year, avg_points DESC;

-- COMMAND ----------

SELECT team_name,
        race_year,
        COUNT(1) as total_races,
        SUM(calculated_Points) as total_points,
        avg(calculated_Points) as avg_points
      FROM f1_presentation.calculated_race_results
    WHERE team_name IN (SELECT team_name FROM v_dominant_teams WHERE team_rank <=5)
    GROUP BY race_year, team_name
    ORDER BY race_year, avg_points DESC;