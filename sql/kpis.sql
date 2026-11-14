-- =============================================================================
-- Workshop 3: Streaming ETL with Apache Kafka and Machine Learning
-- Archivo: sql/kpis.sql
-- Motor: PostgreSQL 16
-- Propósito: Consultas analíticas optimizadas para conectar Power BI / Tableau /
--            Looker Studio directamente al esquema estrella (fact_predictions,
--            dim_country, dim_date) sin depender de archivos CSV.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- KPI 1: Error promedio de predicción (Average Prediction Error)
-- -----------------------------------------------------------------------------
-- Cómo resuelve el KPI:
--   Agrega todas las predicciones válidas registradas en fact_predictions (con
--   integridad verificada frente a dim_country y dim_date) para entregar las
--   métricas globales de desempeño operativo en tarjetas KPI de BI: volumen total,
--   países cubiertos, error promedio almacenado, MAE, RMSE, sesgo medio y rango
--   de error (mínimo / máximo).
-- -----------------------------------------------------------------------------
SELECT
    COUNT(fp.prediction_id)                                             AS total_predictions,
    COUNT(DISTINCT dc.country_id)                                       AS total_countries,
    ROUND(AVG(fp.prediction_error)::numeric, 4)                         AS avg_prediction_error,
    ROUND(AVG(ABS(fp.actual_score - fp.predicted_score))::numeric, 4)   AS mae,
    ROUND(SQRT(AVG(POWER(fp.actual_score - fp.predicted_score, 2)))::numeric, 4) AS rmse,
    ROUND(AVG(fp.predicted_score - fp.actual_score)::numeric, 4)        AS mean_prediction_bias,
    ROUND(MIN(ABS(fp.actual_score - fp.predicted_score))::numeric, 4)   AS min_abs_error,
    ROUND(MAX(ABS(fp.actual_score - fp.predicted_score))::numeric, 4)   AS max_abs_error,
    MIN(dd.event_timestamp)                                             AS first_event_at,
    MAX(dd.event_timestamp)                                             AS last_event_at
FROM fact_predictions fp
INNER JOIN dim_country dc
    ON fp.country_id = dc.country_id
INNER JOIN dim_date dd
    ON fp.date_id = dd.date_id;


-- -----------------------------------------------------------------------------
-- KPI 2: Predicciones por país (Predictions by Country)
-- -----------------------------------------------------------------------------
-- Cómo resuelve el KPI:
--   Agrupa los hechos por la llave primaria de dim_country (óptimo en PostgreSQL)
--   y calcula el volumen de eventos procesados, el puntaje real y predicho
--   promedio, el error medio absoluto (MAE) y la cobertura de años históricos
--   por país. Alimenta gráficos de barras, mapas geográficos y rankings de error.
-- -----------------------------------------------------------------------------
SELECT
    dc.country_id,
    dc.country_name,
    COUNT(fp.prediction_id)                                             AS predictions_count,
    ROUND(AVG(fp.actual_score)::numeric, 4)                             AS avg_actual_score,
    ROUND(AVG(fp.predicted_score)::numeric, 4)                          AS avg_predicted_score,
    ROUND(AVG(fp.prediction_error)::numeric, 4)                         AS avg_prediction_error,
    ROUND(AVG(ABS(fp.actual_score - fp.predicted_score))::numeric, 4)   AS mae,
    MIN(dd.observation_year)                                            AS first_observation_year,
    MAX(dd.observation_year)                                            AS last_observation_year,
    MAX(fp.prediction_timestamp)                                        AS last_prediction_at
FROM fact_predictions fp
INNER JOIN dim_country dc
    ON fp.country_id = dc.country_id
INNER JOIN dim_date dd
    ON fp.date_id = dd.date_id
GROUP BY
    dc.country_id,
    dc.country_name
ORDER BY
    predictions_count DESC,
    mae ASC,
    dc.country_name ASC;


-- -----------------------------------------------------------------------------
-- KPI 3: Puntaje predicho vs. puntaje real (Predicted vs. Actual Score)
-- -----------------------------------------------------------------------------
-- Cómo resuelve el KPI:
--   Expone el detalle a nivel de evento uniendo fact_predictions con dim_country
--   y dim_date para graficar diagramas de dispersión (actual_score vs.
--   predicted_score), calcular la desviación con signo (residual) y clasificar
--   cada predicción por banda de precisión en tablas comparativas de Power BI/Tableau.
-- -----------------------------------------------------------------------------
SELECT
    fp.prediction_id,
    fp.raw_event_id,
    dc.country_name,
    dd.observation_year,
    dd.full_date                                                        AS event_date,
    dd.event_timestamp,
    ROUND(fp.actual_score::numeric, 4)                                  AS actual_score,
    ROUND(fp.predicted_score::numeric, 4)                               AS predicted_score,
    ROUND(fp.prediction_error::numeric, 4)                              AS prediction_error,
    ROUND((fp.predicted_score - fp.actual_score)::numeric, 4)           AS signed_residual,
    CASE
        WHEN ABS(fp.actual_score - fp.predicted_score) <= 0.25 THEN 'High Accuracy (<= 0.25)'
        WHEN ABS(fp.actual_score - fp.predicted_score) <= 0.60 THEN 'Moderate Error (0.25 - 0.60)'
        ELSE 'High Error (> 0.60)'
    END                                                                 AS accuracy_band,
    fp.prediction_timestamp
FROM fact_predictions fp
INNER JOIN dim_country dc
    ON fp.country_id = dc.country_id
INNER JOIN dim_date dd
    ON fp.date_id = dd.date_id
ORDER BY
    dd.event_timestamp DESC,
    fp.prediction_id DESC;


-- -----------------------------------------------------------------------------
-- KPI 4: Tendencias de predicción a lo largo del tiempo (Prediction Trends Over Time)
-- -----------------------------------------------------------------------------
-- Cómo resuelve el KPI:
--   Agrupa las predicciones en ventanas de tiempo por segundo sobre event_timestamp
--   de dim_date (además de mantener fecha/año/mes/día) para visualizar tanto la
--   cadencia en tiempo real del flujo de Kafka durante la simulación como la
--   evolución del error promedio, puntajes medios y volumen acumulado (window function).
-- -----------------------------------------------------------------------------
SELECT
    DATE_TRUNC('second', dd.event_timestamp)                            AS event_time_bucket,
    dd.full_date                                                        AS event_date,
    dd.event_year,
    dd.event_month,
    dd.event_day,
    COUNT(fp.prediction_id)                                             AS predictions_in_bucket,
    SUM(COUNT(fp.prediction_id)) OVER (
        ORDER BY DATE_TRUNC('second', dd.event_timestamp)
    )                                                                   AS cumulative_predictions,
    COUNT(DISTINCT dc.country_id)                                       AS active_countries,
    ROUND(AVG(fp.actual_score)::numeric, 4)                             AS avg_actual_score,
    ROUND(AVG(fp.predicted_score)::numeric, 4)                          AS avg_predicted_score,
    ROUND(AVG(fp.prediction_error)::numeric, 4)                         AS avg_prediction_error,
    ROUND(AVG(ABS(fp.actual_score - fp.predicted_score))::numeric, 4)   AS mae
FROM fact_predictions fp
INNER JOIN dim_country dc
    ON fp.country_id = dc.country_id
INNER JOIN dim_date dd
    ON fp.date_id = dd.date_id
GROUP BY
    DATE_TRUNC('second', dd.event_timestamp),
    dd.full_date,
    dd.event_year,
    dd.event_month,
    dd.event_day
ORDER BY
    event_time_bucket ASC;
