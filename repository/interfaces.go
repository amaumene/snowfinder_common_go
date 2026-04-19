package repository

import (
	"context"

	"github.com/amaumene/snowfinder_common/models"
)

// Writer provides the write operations used by the scraper.
// It intentionally does not embed a Reader interface: the scraper holds a
// concrete *WriterRepository and calls read methods directly. Keeping the
// interface narrow makes it easier to mock in tests and avoids coupling the
// write path to the full read surface.
type Writer interface {
	SaveResort(ctx context.Context, resort *models.Resort) error
	SaveSnowDepthReadings(ctx context.Context, readings []models.SnowDepthReading) error
	SaveDailySnowfall(ctx context.Context, snowfalls []models.DailySnowfall) error
	SaveFailedScrapeAttempt(ctx context.Context, resortURL, errorMessage string) error
	MarkFailedAttemptRetried(ctx context.Context, id string) error
	GetPendingFailedScrapeAttempts(ctx context.Context) ([]models.FailedScrapeAttempt, error)
}
