CREATE TABLE IF NOT EXISTS regulations (
    id              SERIAL PRIMARY KEY,
    created_at      DATE,
    update_at       TIMESTAMP,
    is_active       BOOLEAN DEFAULT TRUE,
    title           VARCHAR(65),
    gtype           VARCHAR(50),
    entity          VARCHAR(255),
    external_link   TEXT,
    rtype_id        INTEGER,
    summary         TEXT,
    classification_id INTEGER
);

CREATE TABLE IF NOT EXISTS regulations_component (
    id              SERIAL PRIMARY KEY,
    regulations_id  INTEGER REFERENCES regulations(id),
    components_id   INTEGER
);

CREATE INDEX IF NOT EXISTS idx_regulations_entity 
    ON regulations(entity);

CREATE INDEX IF NOT EXISTS idx_regulations_title_date 
    ON regulations(title, created_at, external_link);