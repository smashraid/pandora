CREATE TABLE IF NOT EXISTS loan_applications (
    id VARCHAR(64) PRIMARY KEY,
    applicant_email VARCHAR(255) NOT NULL,
    requested_amount_cents BIGINT NOT NULL,
    priority VARCHAR(32) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS application_documents (
    id VARCHAR(64) PRIMARY KEY,
    application_id VARCHAR(64) NOT NULL REFERENCES loan_applications(id) ON DELETE CASCADE,
    document_type VARCHAR(64) NOT NULL,
    s3_url TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS processing_tasks (
    task_id VARCHAR(64) PRIMARY KEY,
    application_id VARCHAR(64) NOT NULL REFERENCES loan_applications(id) ON DELETE CASCADE,
    status VARCHAR(32) NOT NULL,
    current_stage VARCHAR(64) NOT NULL,
    progress_percentage INT NOT NULL DEFAULT 0,
    status_message TEXT,
    error_details TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tasks_application_id ON processing_tasks(application_id);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON processing_tasks(status);