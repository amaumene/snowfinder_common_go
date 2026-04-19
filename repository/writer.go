package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/amaumene/snowfinder_common/models"
	"github.com/google/uuid"
)

const batchChunkSize = 500

// WriterRepository provides full read-write database access.
type WriterRepository struct {
	*ReaderRepository
}

// NewWriter creates a new read-write repository.
func NewWriter(db *sql.DB) *WriterRepository {
	return &WriterRepository{
		ReaderRepository: NewReader(db),
	}
}

// SaveResort upserts a resort record into the database.
// It mutates the caller's *models.Resort as a side effect: if the resort already
// exists under a different slug (due to scoping), both ID and Slug fields are
// updated to reflect the persisted values.
func (r *WriterRepository) SaveResort(ctx context.Context, resort *models.Resort) error {
	if resort == nil {
		return errors.New("nil resort")
	}
	if strings.TrimSpace(resort.Slug) == "" {
		return errors.New("resort slug is required")
	}
	if strings.TrimSpace(resort.Name) == "" {
		return errors.New("resort name is required")
	}

	resolvedID := resort.ID
	if resolvedID == "" {
		resolvedID = uuid.New().String()
	}

	persistedRecord, err := r.resolveResortRecord(ctx, resort)
	if err != nil {
		return fmt.Errorf("resolve resort identity: %w", err)
	}

	if persistedRecord.ID != "" {
		resolvedID = persistedRecord.ID
	}
	resolvedSlug := persistedRecord.Slug

	// Use RETURNING id to atomically get the persisted id, avoiding the
	// select-then-upsert race that the previous two-phase flow had.
	query := `
		INSERT INTO resorts (
			id, slug, name, prefecture, region,
			top_elevation_m, base_elevation_m, vertical_m,
			num_courses, longest_course_km, steepest_course_deg
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (slug) DO UPDATE SET
			name = EXCLUDED.name,
			prefecture = EXCLUDED.prefecture,
			region = EXCLUDED.region,
			top_elevation_m = EXCLUDED.top_elevation_m,
			base_elevation_m = EXCLUDED.base_elevation_m,
			vertical_m = EXCLUDED.vertical_m,
			num_courses = EXCLUDED.num_courses,
			longest_course_km = EXCLUDED.longest_course_km,
			steepest_course_deg = EXCLUDED.steepest_course_deg,
			last_updated = datetime('now')
		RETURNING id
	`

	var persistedID string
	err = r.db.QueryRowContext(ctx, query,
		resolvedID, resolvedSlug, resort.Name, resort.Prefecture, resort.Region,
		resort.TopElevationM, resort.BaseElevationM, resort.VerticalM,
		resort.NumCourses, resort.LongestCourseKM, resort.SteepestCourseDeg,
	).Scan(&persistedID)

	if err != nil {
		return fmt.Errorf("save resort: %w", err)
	}

	// Side effect: mutate the caller's resort to reflect the persisted ID and slug.
	resort.ID = persistedID
	resort.Slug = resolvedSlug

	return nil
}

func (r *WriterRepository) resolveResortRecord(ctx context.Context, resort *models.Resort) (*resortIdentityRecord, error) {
	existingBySlug, err := r.getResortIdentityRecordBySlug(ctx, resort.Slug)
	if err != nil {
		return nil, err
	}

	scopedSlug := scopedResortSlug(resort.Slug, resort.Prefecture, resort.Region)
	var existingByScopedSlug *resortIdentityRecord
	if scopedSlug != resort.Slug {
		existingByScopedSlug, err = r.getResortIdentityRecordBySlug(ctx, scopedSlug)
		if err != nil {
			return nil, err
		}
	}

	return resolvePersistedResortRecordOrError(resort, existingBySlug, existingByScopedSlug)
}

func (r *WriterRepository) getResortIdentityRecordBySlug(ctx context.Context, slug string) (*resortIdentityRecord, error) {
	query := `
		SELECT id, slug, name, prefecture, region
		FROM resorts
		WHERE slug = ?
	`

	var record resortIdentityRecord
	err := r.db.QueryRowContext(ctx, query, slug).Scan(
		&record.ID,
		&record.Slug,
		&record.Name,
		&record.Prefecture,
		&record.Region,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}

		return nil, fmt.Errorf("get resort by slug %q: %w", slug, err)
	}

	return &record, nil
}

// SaveSnowDepthReadings upserts a batch of snow depth readings.
// Readings are written in chunks of batchChunkSize, each in its own transaction.
// Partial writes across chunks are safe: the upserts are idempotent, so
// re-running after a mid-batch failure will complete the remaining chunks
// without duplicating already-written rows.
func (r *WriterRepository) SaveSnowDepthReadings(ctx context.Context, readings []models.SnowDepthReading) error {
	if len(readings) == 0 {
		return nil
	}

	query := `
		INSERT INTO snow_depth_readings (resort_id, date, depth_cm)
		VALUES (?, ?, ?)
		ON CONFLICT (resort_id, date) DO UPDATE SET
			depth_cm = EXCLUDED.depth_cm
	`

	for start := 0; start < len(readings); start += batchChunkSize {
		end := start + batchChunkSize
		if end > len(readings) {
			end = len(readings)
		}
		if err := r.saveSnowDepthChunk(ctx, query, readings[start:end]); err != nil {
			return err
		}
	}

	return nil
}

// saveSnowDepthChunk writes a single chunk of snow depth readings in one transaction.
// defer tx.Rollback() is scoped to this function, not the enclosing loop.
func (r *WriterRepository) saveSnowDepthChunk(ctx context.Context, query string, chunk []models.SnowDepthReading) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	for _, reading := range chunk {
		if _, err := tx.ExecContext(ctx, query, reading.ResortID, reading.Date.UTC().Format("2006-01-02"), reading.DepthCM); err != nil {
			return fmt.Errorf("save reading: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit save readings: %w", err)
	}
	return nil
}

// SaveFailedScrapeAttempt records a new failed scrape attempt for the given URL.
func (r *WriterRepository) SaveFailedScrapeAttempt(ctx context.Context, resortURL, errorMessage string) error {
	query := `
		INSERT INTO failed_scrape_attempts (id, resort_url, error_message, failed_at, retried)
		VALUES (?, ?, ?, datetime('now'), FALSE)
	`

	if _, err := r.db.ExecContext(ctx, query, uuid.New().String(), resortURL, errorMessage); err != nil {
		return fmt.Errorf("save failed scrape attempt: %w", err)
	}

	return nil
}

// MarkFailedAttemptRetried marks the failed scrape attempt with the given ID as retried.
// Returns nil if no row was updated (idempotent: callers may retry the same ID).
func (r *WriterRepository) MarkFailedAttemptRetried(ctx context.Context, id string) error {
	query := `
		UPDATE failed_scrape_attempts
		SET retried = TRUE, retried_at = datetime('now')
		WHERE id = ?
	`

	result, err := r.db.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("mark failed attempt retried: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("mark failed attempt retried: rows affected: %w", err)
	}
	// 0 rows affected is treated as success (idempotent).
	if rowsAffected > 1 {
		return fmt.Errorf("mark failed attempt retried: affected %d rows, want 0 or 1", rowsAffected)
	}

	return nil
}

// SaveDailySnowfall upserts a batch of daily snowfall records.
// Records are written in chunks of batchChunkSize, each in its own transaction.
// Partial writes across chunks are safe: the upserts are idempotent, so
// re-running after a mid-batch failure will complete the remaining chunks
// without duplicating already-written rows.
func (r *WriterRepository) SaveDailySnowfall(ctx context.Context, snowfalls []models.DailySnowfall) error {
	if len(snowfalls) == 0 {
		return nil
	}

	query := `
		INSERT INTO daily_snowfall (resort_id, date, snowfall_cm)
		VALUES (?, ?, ?)
		ON CONFLICT (resort_id, date) DO UPDATE SET
			snowfall_cm = EXCLUDED.snowfall_cm
	`

	for start := 0; start < len(snowfalls); start += batchChunkSize {
		end := start + batchChunkSize
		if end > len(snowfalls) {
			end = len(snowfalls)
		}
		if err := r.saveDailySnowfallChunk(ctx, query, snowfalls[start:end]); err != nil {
			return err
		}
	}

	return nil
}

// saveDailySnowfallChunk writes a single chunk of daily snowfall records in one transaction.
// defer tx.Rollback() is scoped to this function, not the enclosing loop.
func (r *WriterRepository) saveDailySnowfallChunk(ctx context.Context, query string, chunk []models.DailySnowfall) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	for _, sf := range chunk {
		if _, err := tx.ExecContext(ctx, query, sf.ResortID, sf.Date.UTC().Format("2006-01-02"), sf.SnowfallCM); err != nil {
			return fmt.Errorf("save snowfall: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit save snowfall: %w", err)
	}
	return nil
}
