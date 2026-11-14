-- =============================================================================
-- Workshop 3: Streaming ETL with Apache Kafka and Machine Learning
-- Archivo: sql/create_tables.sql
-- Motor: PostgreSQL 16
-- Propósito: Crea la tabla cruda de trazabilidad (raw_happiness_events) y el
--            esquema estrella analítico (dim_country, dim_date, fact_predictions).
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Tabla Cruda / Staging: raw_happiness_events
--    Preserva cada mensaje exactamente como llega desde Apache Kafka (antes de
--    validar o predecir) para auditoría, depuración y reprocesamiento futuro.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_happiness_events (
    raw_event_id       BIGSERIAL PRIMARY KEY,
    event_id           VARCHAR(128),
    kafka_topic        VARCHAR(128) NOT NULL,
    kafka_partition    INTEGER      NOT NULL,
    kafka_offset       BIGINT       NOT NULL,
    event_key          VARCHAR(255),
    raw_message        TEXT         NOT NULL,
    raw_payload        JSONB,
    processing_status  VARCHAR(32)  NOT NULL DEFAULT 'PENDING'
        CONSTRAINT chk_raw_processing_status CHECK (
            processing_status IN (
                'PENDING',
                'VALID',
                'INVALID_SCHEMA',
                'INVALID_VALUES',
                'PREDICTION_ERROR'
            )
        ),
    error_message      TEXT,
    received_at        TIMESTAMP    NOT NULL DEFAULT NOW(),
    processed_at       TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_raw_events_status
    ON raw_happiness_events (processing_status);

CREATE INDEX IF NOT EXISTS idx_raw_events_event_id
    ON raw_happiness_events (event_id);

CREATE INDEX IF NOT EXISTS idx_raw_events_kafka_pos
    ON raw_happiness_events (kafka_topic, kafka_partition, kafka_offset);


-- -----------------------------------------------------------------------------
-- 2. Dimensión País: dim_country
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_country (
    country_id    SERIAL       PRIMARY KEY,
    country_name  VARCHAR(150) NOT NULL UNIQUE,
    created_at    TIMESTAMP    NOT NULL DEFAULT NOW()
);


-- -----------------------------------------------------------------------------
-- 3. Dimensión Tiempo: dim_date
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_date (
    date_id           SERIAL    PRIMARY KEY,
    event_timestamp   TIMESTAMP NOT NULL,
    full_date         DATE      NOT NULL,
    event_year        INTEGER   NOT NULL,
    event_month       INTEGER   NOT NULL,
    event_day         INTEGER   NOT NULL,
    observation_year  INTEGER   NOT NULL,
    CONSTRAINT uq_dim_date_ts_obs UNIQUE (event_timestamp, observation_year)
);

CREATE INDEX IF NOT EXISTS idx_dim_date_full_date
    ON dim_date (full_date);


-- -----------------------------------------------------------------------------
-- 4. Tabla de Hechos: fact_predictions
--    Almacena las predicciones en tiempo real vinculadas al evento crudo
--    original mediante raw_event_id y a las dimensiones dim_country / dim_date.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_predictions (
    prediction_id         BIGSERIAL        PRIMARY KEY,
    raw_event_id          BIGINT           NOT NULL UNIQUE
        REFERENCES raw_happiness_events (raw_event_id) ON DELETE RESTRICT,
    country_id            INTEGER          NOT NULL
        REFERENCES dim_country (country_id) ON DELETE RESTRICT,
    date_id               INTEGER          NOT NULL
        REFERENCES dim_date (date_id) ON DELETE RESTRICT,
    actual_score          DOUBLE PRECISION NOT NULL,
    predicted_score       DOUBLE PRECISION NOT NULL,
    prediction_error      DOUBLE PRECISION NOT NULL,
    prediction_timestamp  TIMESTAMP        NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_predictions_country_id
    ON fact_predictions (country_id);

CREATE INDEX IF NOT EXISTS idx_fact_predictions_date_id
    ON fact_predictions (date_id);

CREATE INDEX IF NOT EXISTS idx_fact_predictions_timestamp
    ON fact_predictions (prediction_timestamp);
