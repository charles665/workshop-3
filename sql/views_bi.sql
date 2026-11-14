-- =============================================================================
-- Workshop 3: Streaming ETL with Apache Kafka and Machine Learning
-- Archivo: sql/views_bi.sql
-- Motor: PostgreSQL 16
-- Propósito: Vistas SQL listas para conectar directamente desde Power BI Desktop
--            (Servidor: 127.0.0.1:5433 | Base de datos: happiness_db) sin
--            necesidad de escribir consultas personalizadas o DAX complejo.
-- =============================================================================

-- 1. Vista para KPI 1: Error promedio de predicción y resumen global
CREATE OR REPLACE VIEW vw_kpi_global_summary AS
SELECT
    COUNT(fp.prediction_id)                                                     AS total_predictions,
    COUNT(DISTINCT dc.country_id)                                               AS total_countries,
    ROUND(AVG(fp.prediction_error)::numeric, 4)                                 AS avg_prediction_error,
    ROUND(AVG(ABS(fp.actual_score - fp.predicted_score))::numeric, 4)           AS mae,
    ROUND(SQRT(AVG(POWER(fp.actual_score - fp.predicted_score, 2)))::numeric, 4) AS rmse,
    ROUND(AVG(fp.actual_score)::numeric, 4)                                     AS avg_actual_score,
    ROUND(AVG(fp.predicted_score)::numeric, 4)                                  AS avg_predicted_score,
    ROUND(AVG(fp.predicted_score - fp.actual_score)::numeric, 4)                AS mean_prediction_bias,
    ROUND(MIN(ABS(fp.actual_score - fp.predicted_score))::numeric, 4)           AS min_abs_error,
    ROUND(MAX(ABS(fp.actual_score - fp.predicted_score))::numeric, 4)           AS max_abs_error,
    MIN(dd.event_timestamp)                                                     AS first_event_at,
    MAX(dd.event_timestamp)                                                     AS last_event_at
FROM fact_predictions fp
INNER JOIN dim_country dc ON fp.country_id = dc.country_id
INNER JOIN dim_date dd    ON fp.date_id = dd.date_id;


-- 2. Vista para KPI 2: Predicciones por país
CREATE OR REPLACE VIEW vw_kpi_predictions_by_country AS
SELECT
    dc.country_id,
    dc.country_name,
    COUNT(fp.prediction_id)                                                     AS predictions_count,
    ROUND(AVG(fp.actual_score)::numeric, 4)                                     AS avg_actual_score,
    ROUND(AVG(fp.predicted_score)::numeric, 4)                                  AS avg_predicted_score,
    ROUND(AVG(fp.prediction_error)::numeric, 4)                                 AS avg_prediction_error,
    ROUND(AVG(ABS(fp.actual_score - fp.predicted_score))::numeric, 4)           AS mae,
    MIN(dd.observation_year)                                                    AS first_observation_year,
    MAX(dd.observation_year)                                                    AS last_observation_year,
    MAX(fp.prediction_timestamp)                                                AS last_prediction_at
FROM fact_predictions fp
INNER JOIN dim_country dc ON fp.country_id = dc.country_id
INNER JOIN dim_date dd    ON fp.date_id = dd.date_id
GROUP BY dc.country_id, dc.country_name;


-- 3. Vista para KPI 3: Puntaje predicho vs. puntaje real (detalle + banda de error)
CREATE OR REPLACE VIEW vw_kpi_predicted_vs_actual AS
SELECT
    fp.prediction_id,
    fp.raw_event_id,
    dc.country_name,
    dd.observation_year,
    dd.full_date                                                                AS event_date,
    dd.event_timestamp,
    ROUND(fp.actual_score::numeric, 4)                                          AS actual_score,
    ROUND(fp.predicted_score::numeric, 4)                                       AS predicted_score,
    ROUND(fp.prediction_error::numeric, 4)                                      AS prediction_error,
    ROUND((fp.predicted_score - fp.actual_score)::numeric, 4)                   AS signed_residual,
    CASE
        WHEN ABS(fp.actual_score - fp.predicted_score) <= 0.25 THEN 'High Accuracy (<= 0.25)'
        WHEN ABS(fp.actual_score - fp.predicted_score) <= 0.60 THEN 'Moderate Error (0.25 - 0.60)'
        ELSE 'High Error (> 0.60)'
    END                                                                         AS accuracy_band,
    fp.prediction_timestamp
FROM fact_predictions fp
INNER JOIN dim_country dc ON fp.country_id = dc.country_id
INNER JOIN dim_date dd    ON fp.date_id = dd.date_id;


-- 4A. Vista para KPI 4: Tendencias en tiempo real del flujo de streaming (por segundo)
CREATE OR REPLACE VIEW vw_kpi_trends_over_time AS
SELECT
    DATE_TRUNC('second', dd.event_timestamp)                                    AS event_time_bucket,
    dd.full_date                                                                AS event_date,
    COUNT(fp.prediction_id)                                                     AS predictions_in_bucket,
    SUM(COUNT(fp.prediction_id)) OVER (
        ORDER BY DATE_TRUNC('second', dd.event_timestamp)
    )                                                                           AS cumulative_predictions,
    COUNT(DISTINCT dc.country_id)                                               AS active_countries,
    ROUND(AVG(fp.actual_score)::numeric, 4)                                     AS avg_actual_score,
    ROUND(AVG(fp.predicted_score)::numeric, 4)                                  AS avg_predicted_score,
    ROUND(AVG(fp.prediction_error)::numeric, 4)                                 AS avg_prediction_error,
    ROUND(AVG(ABS(fp.actual_score - fp.predicted_score))::numeric, 4)           AS mae
FROM fact_predictions fp
INNER JOIN dim_country dc ON fp.country_id = dc.country_id
INNER JOIN dim_date dd    ON fp.date_id = dd.date_id
GROUP BY DATE_TRUNC('second', dd.event_timestamp), dd.full_date;


-- 4B. Vista complementaria para KPI 4: Tendencia por año histórico observado (2015-2019)
CREATE OR REPLACE VIEW vw_kpi_trends_by_year AS
SELECT
    dd.observation_year,
    COUNT(fp.prediction_id)                                                     AS predictions_count,
    COUNT(DISTINCT dc.country_id)                                               AS active_countries,
    ROUND(AVG(fp.actual_score)::numeric, 4)                                     AS avg_actual_score,
    ROUND(AVG(fp.predicted_score)::numeric, 4)                                  AS avg_predicted_score,
    ROUND(AVG(fp.prediction_error)::numeric, 4)                                 AS avg_prediction_error,
    ROUND(AVG(ABS(fp.actual_score - fp.predicted_score))::numeric, 4)           AS mae
FROM fact_predictions fp
INNER JOIN dim_country dc ON fp.country_id = dc.country_id
INNER JOIN dim_date dd    ON fp.date_id = dd.date_id
GROUP BY dd.observation_year;


-- 5. Vista de auditoría de calidad y resiliencia del consumidor (tabla cruda)
CREATE OR REPLACE VIEW vw_streaming_audit AS
SELECT
    processing_status,
    COUNT(*)                                                                    AS events_count,
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ())::numeric, 2)               AS pct_of_total,
    MIN(received_at)                                                            AS first_received_at,
    MAX(received_at)                                                            AS last_received_at
FROM raw_happiness_events
GROUP BY processing_status;
