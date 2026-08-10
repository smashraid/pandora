package inbound

import (
	"context"

	"github.com/smashraid/pandora/internal/domain"
)

type SubmitLoanUseCase interface {
	SubmitApplication(ctx context.Context, app *domain.LoanApplication) (*domain.ProcessingTask, error)
}
