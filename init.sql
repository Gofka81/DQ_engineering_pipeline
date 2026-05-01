-- Create backend database (runs in the default 'prefect' DB context)
SELECT 'CREATE DATABASE backend'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'backend')\gexec

\c backend

-- Users table
CREATE TABLE IF NOT EXISTS users (
    id              SERIAL PRIMARY KEY,
    username        VARCHAR(50) UNIQUE NOT NULL,
    email           VARCHAR(255) UNIQUE NOT NULL,
    hashed_password VARCHAR(255) NOT NULL,
    disabled        BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_users_username ON users (username);

-- Files table
CREATE TABLE IF NOT EXISTS files (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id           INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    original_filename VARCHAR(255) NOT NULL,
    minio_raw_path    VARCHAR(512) NOT NULL,
    file_size         BIGINT NOT NULL,
    content_type      VARCHAR(100) DEFAULT 'text/csv',
    has_header        BOOLEAN NOT NULL DEFAULT TRUE,
    uploaded_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_files_user_id ON files (user_id);

-- Run status enum
DO $$ BEGIN
    CREATE TYPE run_status AS ENUM (
        'PENDING',
        'ANALYZING',
        'AWAITING_REVIEW',
        'TRANSFORMING',
        'COMPLETED',
        'FAILED'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Runs table
CREATE TABLE IF NOT EXISTS runs (
    id                        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    file_id                   UUID NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    status                    run_status DEFAULT 'PENDING' NOT NULL,
    dq_scores_before          JSONB,
    dq_scores_after           JSONB,
    recommendations_generated JSONB,
    recommendations_approved  JSONB,
    minio_curated_path        VARCHAR(512),
    created_at                TIMESTAMPTZ DEFAULT NOW(),
    completed_at              TIMESTAMPTZ,
    error_message             VARCHAR(2000)
);

CREATE INDEX IF NOT EXISTS idx_runs_file_id ON runs (file_id);
CREATE INDEX IF NOT EXISTS idx_runs_status ON runs (status);
