-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Report on Dominant Formula 1 Drivers </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------


CREATE OR REPLACE TEMP VIEW v_dominant_drivers
AS
SELECT driver_name,
        COUNT(1) as total_races,
        SUM(calculated_Points) as total_points,
        avg(calculated_Points) as avg_points,
        RANK() OVER(ORDER BY avg(calculated_Points) DESC) AS driver_rank
      FROM f1_presentation.calculated_race_results
    GROUP BY driver_name
    HAVING count(1) >=50
    ORDER BY avg_points DESC;

-- COMMAND ----------

SELECT driver_name,
        race_year,
        COUNT(1) as total_races,
        SUM(calculated_Points) as total_points,
        avg(calculated_Points) as avg_points
      FROM f1_presentation.calculated_race_results
    WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <=10)
    GROUP BY race_year, driver_name
    ORDER BY race_year, avg_points DESC;

-- COMMAND ----------

SELECT driver_name,
        race_year,
        COUNT(1) as total_races,
        SUM(calculated_Points) as total_points,
        avg(calculated_Points) as avg_points
      FROM f1_presentation.calculated_race_results
    WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <=10)
    GROUP BY race_year, driver_name
    ORDER BY race_year, avg_points DESC;

-- COMMAND ----------

SELECT driver_name,
        race_year,
        COUNT(1) as total_races,
        SUM(calculated_Points) as total_points,
        avg(calculated_Points) as avg_points
      FROM f1_presentation.calculated_race_results
    WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <=10)
    GROUP BY race_year, driver_name
    ORDER BY race_year, avg_points DESC;