package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/smashraid/pandora/internal/domain"
	"github.com/smashraid/pandora/internal/ports/outbound"
)

var _ outbound.TaskRepository = (*PostgresTaskRepository)(nil)

type PostgresTaskRepository struct {
	db *sql.DB
}

func NewPostgresTaskRepository(db *sql.DB) *PostgresTaskRepository {
	return &PostgresTaskRepository{db: db}
}

func (r *PostgresTaskRepository) SaveApplication(ctx context.Context, app *domain.LoanApplication) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	queryApp := `
		INSERT INTO loan_applications (id, applicant_email, requested_amount_cents, priority, created_at)
		VALUES ($1, $2, $3, $4, $5)
	`
	_, err = tx.ExecContext(ctx, queryApp, app.ID, app.ApplicantEmail, app.RequestedAmountCents, string(app.Priority), app.CreatedAt)
	if err != nil {
		return fmt.Errorf("failed to insert loan_application: %w", err)
	}

	queryDoc := `
		INSERT INTO application_documents (id, application_id, document_type, s3_url)
		VALUES ($1, $2, $3, $4)
	`
	for _, doc := range app.Documents {
		_, err = tx.ExecContext(ctx, queryDoc, doc.ID, app.ID, doc.Type, doc.S3URL)
		if err != nil {
			return fmt.Errorf("failed to insert application_document: %w", err)
		}
	}

	return tx.Commit()
}

func (r *PostgresTaskRepository) GetApplicationByID(ctx context.Context, appID string) (*domain.LoanApplication, error) {
	queryApp := `
		SELECT id, applicant_email, requested_amount_cents, priority, created_at
		FROM loan_applications
		WHERE id = $1
	`
	row := r.db.QueryRowContext(ctx, queryApp, appID)

	var app domain.LoanApplication
	var priority string
	err := row.Scan(&app.ID, &app.ApplicantEmail, &app.RequestedAmountCents, &priority, &app.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrTaskNotFound
	} else if err != nil {
		return nil, fmt.Errorf("failed to query application: %w", err)
	}
	app.Priority = domain.Priority(priority)

	queryDocs := `
		SELECT id, document_type, s3_url
		FROM application_documents
		WHERE application_id = $1
	`
	rows, err := r.db.QueryContext(ctx, queryDocs, appID)
	if err != nil {
		return nil, fmt.Errorf("failed to query documents: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var doc domain.Document
		if err := rows.Scan(&doc.ID, &doc.Type, &doc.S3URL); err != nil {
			return nil, fmt.Errorf("failed to scan document: %w", err)
		}
		app.Documents = append(app.Documents, doc)
	}

	return &app, nil
}

func (r *PostgresTaskRepository) CreateTask(ctx context.Context, task *domain.ProcessingTask) error {
	query := `
		INSERT INTO processing_tasks (
			task_id, application_id, status, current_stage,
			progress_percentage, status_message, error_details, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`
	_, err := r.db.ExecContext(
		ctx, query,
		task.TaskID, task.ApplicationID, string(task.Status), string(task.CurrentStage),
		task.ProgressPercentage, task.StatusMessage, task.ErrorDetails, task.CreatedAt, task.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to insert processing_task: %w", err)
	}
	return nil
}

func (r *PostgresTaskRepository) GetTaskByID(ctx context.Context, taskID string) (*domain.ProcessingTask, error) {
	query := `
		SELECT task_id, application_id, status, current_stage,
		       progress_percentage, status_message, error_details, created_at, updated_at
		FROM processing_tasks
		WHERE task_id = $1
	`
	row := r.db.QueryRowContext(ctx, query, taskID)

	var task domain.ProcessingTask
	var status, stage string
	var statusMsg, errDetails sql.NullString

	err := row.Scan(
		&task.TaskID, &task.ApplicationID, &status, &stage,
		&task.ProgressPercentage, &statusMsg, &errDetails, &task.CreatedAt, &task.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrTaskNotFound
	} else if err != nil {
		return nil, fmt.Errorf("failed to scan task: %w", err)
	}

	task.Status = domain.TaskStatus(status)
	task.CurrentStage = domain.ProcessingStage(stage)
	if statusMsg.Valid {
		task.StatusMessage = statusMsg.String
	}
	if errDetails.Valid {
		task.ErrorDetails = errDetails.String
	}

	return &task, nil
}

func (r *PostgresTaskRepository) UpdateTask(ctx context.Context, task *domain.ProcessingTask) error {
	query := `
		UPDATE processing_tasks
		SET status = $1, current_stage = $2, progress_percentage = $3,
		    status_message = $4, error_details = $5, updated_at = $6
		WHERE task_id = $7
	`
	res, err := r.db.ExecContext(
		ctx, query,
		string(task.Status), string(task.CurrentStage), task.ProgressPercentage,
		task.StatusMessage, task.ErrorDetails, task.UpdatedAt, task.TaskID,
	)
	if err != nil {
		return fmt.Errorf("failed to update task: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check rows affected: %w", err)
	}
	if rows == 0 {
		return domain.ErrTaskNotFound
	}

	return nil
}
