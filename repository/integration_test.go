package repository

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/amaumene/snowfinder_common/models"
	"github.com/google/uuid"
)

// insertResort inserts a resort directly into the DB for test setup.
func insertResort(t *testing.T, repo *WriterRepository, slug, name, prefecture, region string) string {
	t.Helper()
	resort := &models.Resort{
		Slug:       slug,
		Name:       name,
		Prefecture: prefecture,
		Region:     region,
	}
	if err := repo.SaveResort(context.Background(), resort); err != nil {
		t.Fatalf("insertResort(%q): %v", slug, err)
	}
	return resort.ID
}

// insertSnowfall inserts daily_snowfall rows directly for test setup.
func insertSnowfall(t *testing.T, repo *WriterRepository, resortID string, entries []models.DailySnowfall) {
	t.Helper()
	if err := repo.SaveDailySnowfall(context.Background(), entries); err != nil {
		t.Fatalf("insertSnowfall: %v", err)
	}
}

// --- GetSnowiestResortsForDateRange ---

func TestGetSnowiestResortsForDateRange_CrossYearRange(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	repo := NewWriter(db)
	reader := NewReader(db)

	resortID := insertResort(t, repo, "hakuba-47", "Hakuba 47", "nagano", "hakuba")

	// Seed snowfall for Dec 28 - Jan 3 across two winters.
	// Use leap years (2000-01 and 2004-05) so that Dec 28 = DOY 363 in both
	// years, matching the DOY reference year (2000) used by mmddToDOY.
	var snowfalls []models.DailySnowfall
	for _, dateStr := range []string{
		"2000-12-28", "2000-12-29", "2000-12-30", "2000-12-31",
		"2001-01-01", "2001-01-02", "2001-01-03",
		"2004-12-28", "2004-12-29", "2004-12-30", "2004-12-31",
		"2005-01-01", "2005-01-02", "2005-01-03",
	} {
		d, _ := time.Parse("2006-01-02", dateStr)
		snowfalls = append(snowfalls, models.DailySnowfall{
			ResortID:   resortID,
			Date:       d,
			SnowfallCM: 10,
		})
	}
	insertSnowfall(t, repo, resortID, snowfalls)

	results, err := reader.GetSnowiestResortsForDateRange(context.Background(), "12-28", "01-03", "", 10)
	if err != nil {
		t.Fatalf("GetSnowiestResortsForDateRange() error = %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("got %d results, want 1", len(results))
	}
	if results[0].ResortID != resortID {
		t.Fatalf("result resort_id = %q, want %q", results[0].ResortID, resortID)
	}
	// 7 days × 10 cm = 70 cm per year, averaged over 2 years = 70
	if results[0].TotalSnowfall == nil || *results[0].TotalSnowfall != 70 {
		var got int
		if results[0].TotalSnowfall != nil {
			got = *results[0].TotalSnowfall
		}
		t.Fatalf("avg snowfall = %d, want 70", got)
	}
	if results[0].YearsWithData == nil || *results[0].YearsWithData != 2 {
		t.Fatalf("years_with_data = %v, want 2", results[0].YearsWithData)
	}
}

func TestGetSnowiestResortsForDateRange_InvalidDates(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	reader := NewReader(db)

	_, err := reader.GetSnowiestResortsForDateRange(context.Background(), "bad", "01-03", "", 10)
	if err == nil {
		t.Fatal("expected error for invalid start date")
	}

	_, err = reader.GetSnowiestResortsForDateRange(context.Background(), "12-28", "bad", "", 10)
	if err == nil {
		t.Fatal("expected error for invalid end date")
	}
}

func TestGetSnowiestResortsForDateRange_ZeroLimitReturnsError(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	reader := NewReader(db)

	_, err := reader.GetSnowiestResortsForDateRange(context.Background(), "12-01", "12-31", "", 0)
	if err == nil {
		t.Fatal("expected error for zero limit")
	}
}

// --- GetSnowiestResortsForWeek ---

func TestGetSnowiestResortsForWeek_HappyPath(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	repo := NewWriter(db)
	reader := NewReader(db)

	resortID := insertResort(t, repo, "niseko-hirafu", "Niseko Grand Hirafu", "hokkaido", "niseko")

	weekStart := time.Date(2023, 1, 10, 0, 0, 0, 0, time.UTC)
	var snowfalls []models.DailySnowfall
	for i := 0; i < 7; i++ {
		snowfalls = append(snowfalls, models.DailySnowfall{
			ResortID:   resortID,
			Date:       weekStart.AddDate(0, 0, i),
			SnowfallCM: 20,
		})
	}
	insertSnowfall(t, repo, resortID, snowfalls)

	results, err := reader.GetSnowiestResortsForWeek(context.Background(), weekStart, "", 10)
	if err != nil {
		t.Fatalf("GetSnowiestResortsForWeek() error = %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("got %d results, want 1", len(results))
	}
	if results[0].ResortID != resortID {
		t.Fatalf("result resort_id = %q, want %q", results[0].ResortID, resortID)
	}
	// 7 days × 20 cm = 140 cm
	if results[0].TotalSnowfall == nil || *results[0].TotalSnowfall != 140 {
		t.Fatalf("avg snowfall = %v, want 140", results[0].TotalSnowfall)
	}
}

func TestGetSnowiestResortsForWeek_PrefectureFilter(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	repo := NewWriter(db)
	reader := NewReader(db)

	id1 := insertResort(t, repo, "resort-nagano", "Nagano Resort", "nagano", "hakuba")
	id2 := insertResort(t, repo, "resort-hokkaido", "Hokkaido Resort", "hokkaido", "niseko")

	weekStart := time.Date(2023, 2, 1, 0, 0, 0, 0, time.UTC)
	for _, id := range []string{id1, id2} {
		var snowfalls []models.DailySnowfall
		for i := 0; i < 7; i++ {
			snowfalls = append(snowfalls, models.DailySnowfall{
				ResortID:   id,
				Date:       weekStart.AddDate(0, 0, i),
				SnowfallCM: 15,
			})
		}
		insertSnowfall(t, repo, id, snowfalls)
	}

	results, err := reader.GetSnowiestResortsForWeek(context.Background(), weekStart, "nagano", 10)
	if err != nil {
		t.Fatalf("GetSnowiestResortsForWeek() error = %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("got %d results, want 1 (prefecture filter)", len(results))
	}
	if results[0].ResortID != id1 {
		t.Fatalf("result resort_id = %q, want %q", results[0].ResortID, id1)
	}
}

// --- SaveResort identity resolution ---

func TestSaveResort_ExistingBySlugSameIdentityReusesID(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	repo := NewWriter(db)

	// First save
	r1 := &models.Resort{Slug: "mount-foo", Name: "Mount Foo", Prefecture: "nagano", Region: "north"}
	if err := repo.SaveResort(context.Background(), r1); err != nil {
		t.Fatalf("first SaveResort: %v", err)
	}
	firstID := r1.ID

	// Second save with same slug + same identity
	r2 := &models.Resort{Slug: "mount-foo", Name: "Mount Foo Updated", Prefecture: "nagano", Region: "north"}
	if err := repo.SaveResort(context.Background(), r2); err != nil {
		t.Fatalf("second SaveResort: %v", err)
	}

	if r2.ID != firstID {
		t.Fatalf("second save got id %q, want %q (should reuse existing)", r2.ID, firstID)
	}

	// Verify only one row in DB
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM resorts WHERE slug = 'mount-foo'").Scan(&count); err != nil {
		t.Fatalf("count resorts: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 resort row, got %d", count)
	}
}

func TestSaveResort_ExistingBySlugDifferentIdentityScopesSlug(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	repo := NewWriter(db)

	// First resort: nagano/north
	r1 := &models.Resort{Slug: "mount-foo", Name: "Mount Foo Nagano", Prefecture: "nagano", Region: "north"}
	if err := repo.SaveResort(context.Background(), r1); err != nil {
		t.Fatalf("first SaveResort: %v", err)
	}

	// Second resort: different prefecture — should get scoped slug
	r2 := &models.Resort{Slug: "mount-foo", Name: "Mount Foo Gifu", Prefecture: "gifu", Region: "west"}
	if err := repo.SaveResort(context.Background(), r2); err != nil {
		t.Fatalf("second SaveResort: %v", err)
	}

	if r2.Slug == "mount-foo" {
		t.Fatalf("expected scoped slug, got %q", r2.Slug)
	}
	if r2.ID == r1.ID {
		t.Fatalf("expected different IDs for different resorts")
	}

	// Verify two rows in DB
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM resorts").Scan(&count); err != nil {
		t.Fatalf("count resorts: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 resort rows, got %d", count)
	}
}

func TestSaveResort_EmptySlugReturnsError(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	repo := NewWriter(db)

	r := &models.Resort{Slug: "", Name: "No Slug Resort", Prefecture: "nagano", Region: "north"}
	err := repo.SaveResort(context.Background(), r)
	if err == nil {
		t.Fatal("expected error for empty slug")
	}
}

func TestSaveResort_EmptyNameReturnsError(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	repo := NewWriter(db)

	r := &models.Resort{Slug: "some-slug", Name: "", Prefecture: "nagano", Region: "north"}
	err := repo.SaveResort(context.Background(), r)
	if err == nil {
		t.Fatal("expected error for empty name")
	}
}

func TestSaveResort_NilReturnsError(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	repo := NewWriter(db)

	err := repo.SaveResort(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error for nil resort")
	}
}

// --- SaveSnowDepthReadings chunking ---

func TestSaveSnowDepthReadings_ChunkingBoundary(t *testing.T) {
	t.Parallel()

	for _, count := range []int{500, 501, 1000} {
		count := count
		t.Run(fmt.Sprintf("count=%d", count), func(t *testing.T) {
			t.Parallel()

			db := newTestDB(t)
			repo := NewWriter(db)
			resortID := insertResort(t, repo, fmt.Sprintf("resort-%d", count), fmt.Sprintf("Resort %d", count), "nagano", "north")

			base := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
			readings := make([]models.SnowDepthReading, count)
			for i := range readings {
				readings[i] = models.SnowDepthReading{
					ResortID: resortID,
					Date:     base.AddDate(0, 0, i),
					DepthCM:  i + 1,
				}
			}

			if err := repo.SaveSnowDepthReadings(context.Background(), readings); err != nil {
				t.Fatalf("SaveSnowDepthReadings(%d) error = %v", count, err)
			}

			var saved int
			if err := db.QueryRow("SELECT COUNT(*) FROM snow_depth_readings WHERE resort_id = ?", resortID).Scan(&saved); err != nil {
				t.Fatalf("count readings: %v", err)
			}
			if saved != count {
				t.Fatalf("saved %d readings, want %d", saved, count)
			}
		})
	}
}

// --- SaveDailySnowfall chunking ---

func TestSaveDailySnowfall_ChunkingBoundary(t *testing.T) {
	t.Parallel()

	for _, count := range []int{500, 501, 1000} {
		count := count
		t.Run(fmt.Sprintf("count=%d", count), func(t *testing.T) {
			t.Parallel()

			db := newTestDB(t)
			repo := NewWriter(db)
			resortID := insertResort(t, repo, fmt.Sprintf("sf-resort-%d", count), fmt.Sprintf("SF Resort %d", count), "nagano", "north")

			base := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
			snowfalls := make([]models.DailySnowfall, count)
			for i := range snowfalls {
				snowfalls[i] = models.DailySnowfall{
					ResortID:   resortID,
					Date:       base.AddDate(0, 0, i),
					SnowfallCM: i + 1,
				}
			}

			if err := repo.SaveDailySnowfall(context.Background(), snowfalls); err != nil {
				t.Fatalf("SaveDailySnowfall(%d) error = %v", count, err)
			}

			var saved int
			if err := db.QueryRow("SELECT COUNT(*) FROM daily_snowfall WHERE resort_id = ?", resortID).Scan(&saved); err != nil {
				t.Fatalf("count snowfalls: %v", err)
			}
			if saved != count {
				t.Fatalf("saved %d snowfalls, want %d", saved, count)
			}
		})
	}
}

// --- MarkFailedAttemptRetried idempotency ---

func TestMarkFailedAttemptRetried_ZeroRowsAffectedReturnsNil(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	repo := NewWriter(db)

	// Use a non-existent ID — should return nil (idempotent)
	err := repo.MarkFailedAttemptRetried(context.Background(), uuid.New().String())
	if err != nil {
		t.Fatalf("MarkFailedAttemptRetried() with unknown id error = %v, want nil", err)
	}
}

func TestMarkFailedAttemptRetried_MarksExistingAttempt(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	repo := NewWriter(db)

	// Insert a failed attempt
	if err := repo.SaveFailedScrapeAttempt(context.Background(), "https://example.com/resort", "timeout"); err != nil {
		t.Fatalf("SaveFailedScrapeAttempt: %v", err)
	}

	// Fetch the ID
	var id string
	if err := db.QueryRow("SELECT id FROM failed_scrape_attempts LIMIT 1").Scan(&id); err != nil {
		t.Fatalf("fetch attempt id: %v", err)
	}

	// Mark as retried
	if err := repo.MarkFailedAttemptRetried(context.Background(), id); err != nil {
		t.Fatalf("MarkFailedAttemptRetried: %v", err)
	}

	// Verify
	var retried bool
	if err := db.QueryRow("SELECT retried FROM failed_scrape_attempts WHERE id = ?", id).Scan(&retried); err != nil {
		t.Fatalf("fetch retried: %v", err)
	}
	if !retried {
		t.Fatal("expected retried = true")
	}

	// Calling again is idempotent
	if err := repo.MarkFailedAttemptRetried(context.Background(), id); err != nil {
		t.Fatalf("second MarkFailedAttemptRetried: %v", err)
	}
}
