package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"github.com/smashraid/pandora/internal/adapters/outbound/postgres"
	"github.com/smashraid/pandora/internal/domain"
)

func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()

	connStr := os.Getenv("TEST_POSTGRES_URL")
	if connStr == "" {
		connStr = "postgres://admin:admin@localhost:5432/loanflow_test?sslmode=disable"
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Skipf("skipping test: unable to connect to Postgres: %v", err)
	}

	if err := db.Ping(); err != nil {
		t.Skipf("skipping test: Postgres DB not reachable at %s: %v", connStr, err)
	}

	// Truncate tables for a clean slate between test runs
	_, _ = db.Exec("TRUNCATE TABLE processing_tasks, application_documents, loan_applications CASCADE;")

	t.Cleanup(func() {
		db.Close()
	})

	return db
}

func TestPostgresTaskRepository_SaveAndGetApplication(t *testing.T) {
	db := setupTestDB(t)
	repo := postgres.NewPostgresTaskRepository(db)
	ctx := context.Background()

	app := &domain.LoanApplication{
		ID:                   "app-pg-001",
		ApplicantEmail:       "pg.test@example.com",
		RequestedAmountCents: 5000000,
		Priority:             domain.PriorityHigh,
		Documents: []domain.Document{
			{ID: "doc-1", Type: "PAYSLIP", S3URL: "https://s3.amazonaws.com/b/payslip.pdf"},
		},
		CreatedAt: time.Now().UTC().Truncate(time.Millisecond),
	}

	// Save Application
	if err := repo.SaveApplication(ctx, app); err != nil {
		t.Fatalf("expected no error saving application, got: %v", err)
	}

	// Fetch Application
	fetchedApp, err := repo.GetApplicationByID(ctx, "app-pg-001")
	if err != nil {
		t.Fatalf("expected application to exist, got: %v", err)
	}

	if fetchedApp.ApplicantEmail != app.ApplicantEmail {
		t.Errorf("expected email %s, got %s", app.ApplicantEmail, fetchedApp.ApplicantEmail)
	}
	if len(fetchedApp.Documents) != 1 || fetchedApp.Documents[0].ID != "doc-1" {
		t.Errorf("expected document doc-1 attached to application")
	}

	// Get Application NotFound
	_, err = repo.GetApplicationByID(ctx, "non-existent")
	if !errors.Is(err, domain.ErrTaskNotFound) {
		t.Errorf("expected domain.ErrTaskNotFound, got: %v", err)
	}
}
