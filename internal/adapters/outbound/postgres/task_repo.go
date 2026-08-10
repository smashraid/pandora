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

func (r *PostgresTaskRepository) SaveApplication(ctx context.Context, app *domain.LoanApplication) (err error) {
	tx, beginErr := r.db.BeginTx(ctx, nil)
	if beginErr != nil {
		return fmt.Errorf("failed to begin transaction: %w", beginErr)
	}

	defer func() {
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			if err != nil {
				// Join the original execution error with the rollback failure error
				err = errors.Join(err, fmt.Errorf("rollback failed: %w", rbErr))
			} else {
				// If queries succeeded but commit failed or didn't run, report the rollback failure
				err = fmt.Errorf("rollback failed: %w", rbErr)
			}
		}
	}()

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

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (r *PostgresTaskRepository) GetApplicationByID(ctx context.Context, appID string) (app *domain.LoanApplication, err error) {
	queryApp := `
		SELECT id, applicant_email, requested_amount_cents, priority, created_at
		FROM loan_applications
		WHERE id = $1
	`
	row := r.db.QueryRowContext(ctx, queryApp, appID)

	var fetchedApp domain.LoanApplication
	var priority string
	scanErr := row.Scan(&fetchedApp.ID, &fetchedApp.ApplicantEmail, &fetchedApp.RequestedAmountCents, &priority, &fetchedApp.CreatedAt)
	if errors.Is(scanErr, sql.ErrNoRows) {
		return nil, domain.ErrTaskNotFound
	} else if scanErr != nil {
		return nil, fmt.Errorf("failed to query application: %w", scanErr)
	}
	fetchedApp.Priority = domain.Priority(priority)

	queryDocs := `
		SELECT id, document_type, s3_url
		FROM application_documents
		WHERE application_id = $1
	`
	rows, queryErr := r.db.QueryContext(ctx, queryDocs, appID)
	if queryErr != nil {
		return nil, fmt.Errorf("failed to query documents: %w", queryErr)
	}

	// Capture closeErr in defer and join/assign to named return `err`
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			if err != nil {
				err = errors.Join(err, fmt.Errorf("failed to close rows: %w", closeErr))
			} else {
				err = fmt.Errorf("failed to close rows: %w", closeErr)
			}
		}
	}()

	for rows.Next() {
		var doc domain.Document
		if err = rows.Scan(&doc.ID, &doc.Type, &doc.S3URL); err != nil {
			return nil, fmt.Errorf("failed to scan document: %w", err)
		}
		fetchedApp.Documents = append(fetchedApp.Documents, doc)
	}

	// Check if any error occurred during iteration
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error during rows iteration: %w", err)
	}

	return &fetchedApp, nil
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
