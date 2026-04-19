package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/amaumene/snowfinder_common/models"
)

// ReaderRepository provides read-only database access.
type ReaderRepository struct {
	db *sql.DB
}

// NewReader creates a new read-only repository.
func NewReader(db *sql.DB) *ReaderRepository {
	return &ReaderRepository{db: db}
}

// doyToMMDD converts a day-of-year integer (1-366) to "MM-DD" string.
// Returns an error if doy is outside the valid range [1, 366].
func doyToMMDD(doy int) (string, error) {
	if doy < 1 || doy > 366 {
		return "", fmt.Errorf("day-of-year %d out of range [1, 366]", doy)
	}
	t := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, doy-1)
	return t.Format("01-02"), nil
}

// mmddToDOY converts an "MM-DD" string to a day-of-year integer.
func mmddToDOY(mmdd string) (int, error) {
	t, err := time.Parse("01-02", mmdd)
	if err != nil {
		return 0, fmt.Errorf("parse MM-DD %q: %w", mmdd, err)
	}
	// time.Parse uses year 0000; set to 2000 for consistent YearDay
	t = time.Date(2000, t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	return t.YearDay(), nil
}

// getResort is the shared implementation for fetching a single resort row.
// whereClause must be a hardcoded column predicate (e.g. "slug = ?" or "id = ?");
// arg is the corresponding bind value.
func (r *ReaderRepository) getResort(ctx context.Context, whereClause string, arg any) (*models.Resort, error) {
	query := fmt.Sprintf(`
		SELECT id, slug, name, prefecture, region,
			   top_elevation_m, base_elevation_m, vertical_m,
			   num_courses, longest_course_km, steepest_course_deg,
			   last_updated
		FROM resorts
		WHERE %s
	`, whereClause)

	var resort models.Resort
	err := r.db.QueryRowContext(ctx, query, arg).Scan(
		&resort.ID, &resort.Slug, &resort.Name, &resort.Prefecture, &resort.Region,
		&resort.TopElevationM, &resort.BaseElevationM, &resort.VerticalM,
		&resort.NumCourses, &resort.LongestCourseKM, &resort.SteepestCourseDeg,
		&resort.LastUpdated,
	)
	if err != nil {
		return nil, err
	}
	return &resort, nil
}

// GetResortBySlug returns the resort with the given URL slug.
// Returns sql.ErrNoRows (wrapped) if no matching resort exists.
func (r *ReaderRepository) GetResortBySlug(ctx context.Context, slug string) (*models.Resort, error) {
	// SAFETY: whereClause is hardcoded, not user-supplied
	resort, err := r.getResort(ctx, "slug = ?", slug)
	if err != nil {
		return nil, fmt.Errorf("get resort by slug: %w", err)
	}
	return resort, nil
}

// GetResortByID returns the resort with the given UUID.
// Returns sql.ErrNoRows (wrapped) if no matching resort exists.
func (r *ReaderRepository) GetResortByID(ctx context.Context, id string) (*models.Resort, error) {
	// SAFETY: whereClause is hardcoded, not user-supplied
	resort, err := r.getResort(ctx, "id = ?", id)
	if err != nil {
		return nil, fmt.Errorf("get resort by id: %w", err)
	}
	return resort, nil
}

// GetSnowiestResortsForWeek queries the snowiest resorts for the 7-day window
// starting at weekStart (inclusive). prefecture is optional; pass "" for all.
func (r *ReaderRepository) GetSnowiestResortsForWeek(ctx context.Context, weekStart time.Time, prefecture string, limit int) ([]models.WeeklyResortStats, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be positive: %d", limit)
	}

	startTime := time.Date(2000, weekStart.Month(), weekStart.Day(), 0, 0, 0, 0, time.UTC)
	startDOY := startTime.YearDay()
	endDOY := startTime.AddDate(0, 0, 6).YearDay()
	startMonth := int(startTime.Month())

	return r.querySnowiestResorts(ctx, startDOY, endDOY, startMonth, prefecture, limit)
}

// GetSnowiestResortsForDateRange queries the snowiest resorts for a seasonal
// MM-DD range (e.g. "12-15" to "01-15"). Handles cross-year boundaries.
// prefecture is optional; pass "" for all.
func (r *ReaderRepository) GetSnowiestResortsForDateRange(ctx context.Context, startMMDD, endMMDD, prefecture string, limit int) ([]models.WeeklyResortStats, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be positive: %d", limit)
	}

	startTime, err := time.Parse("01-02", startMMDD)
	if err != nil {
		return nil, fmt.Errorf("parse start date: %w", err)
	}
	if _, err := time.Parse("01-02", endMMDD); err != nil {
		return nil, fmt.Errorf("parse end date: %w", err)
	}

	startDOY, err := mmddToDOY(startMMDD)
	if err != nil {
		return nil, fmt.Errorf("parse start date: %w", err)
	}
	endDOY, err := mmddToDOY(endMMDD)
	if err != nil {
		return nil, fmt.Errorf("parse end date: %w", err)
	}
	startMonth := int(startTime.Month())

	return r.querySnowiestResorts(ctx, startDOY, endDOY, startMonth, prefecture, limit)
}

// querySnowiestResorts is the shared implementation for both GetSnowiestResortsForWeek
// and GetSnowiestResortsForDateRange.
func (r *ReaderRepository) querySnowiestResorts(ctx context.Context, startDOY, endDOY, startMonth int, prefecture string, limit int) ([]models.WeeklyResortStats, error) {
	var dateFilter string
	groupYearExpr := "CAST(strftime('%Y', substr(date, 1, 19)) AS INTEGER)"
	if startDOY <= endDOY {
		dateFilter = "CAST(strftime('%j', substr(date, 1, 19)) AS INTEGER) >= ? AND CAST(strftime('%j', substr(date, 1, 19)) AS INTEGER) <= ?"
	} else {
		// Cross-year boundary (e.g., Dec 15 to Jan 15)
		dateFilter = "(CAST(strftime('%j', substr(date, 1, 19)) AS INTEGER) >= ? OR CAST(strftime('%j', substr(date, 1, 19)) AS INTEGER) <= ?)"
		groupYearExpr = fmt.Sprintf("CASE WHEN CAST(strftime('%%m', substr(date, 1, 19)) AS INTEGER) >= %d THEN CAST(strftime('%%Y', substr(date, 1, 19)) AS INTEGER) ELSE CAST(strftime('%%Y', substr(date, 1, 19)) AS INTEGER) - 1 END", startMonth)
	}

	args := []any{startDOY, endDOY}

	// Optional prefecture filter — appended as the next positional parameter
	prefectureClause := ""
	if prefecture != "" {
		args = append(args, prefecture)
		prefectureClause = "AND r.prefecture = ?"
	}

	// LIMIT is always the last parameter
	args = append(args, limit)

	// SAFETY: dateFilter and prefectureClause are hardcoded, not user-supplied
	query := fmt.Sprintf(`
		WITH range_data AS (
			SELECT
				resort_id,
				%s as year,
				SUM(snowfall_cm) as total_snowfall
			FROM daily_snowfall
			WHERE %s
			GROUP BY resort_id, year
		),
		avg_range_data AS (
			SELECT
				resort_id,
				AVG(total_snowfall) as avg_snowfall,
				COUNT(*) as years_with_data
			FROM range_data
			GROUP BY resort_id
		)
		SELECT
			r.id,
			r.name,
			r.prefecture,
			CAST(ROUND(ard.avg_snowfall) AS INTEGER) as avg_snowfall,
			ard.years_with_data,
			r.top_elevation_m,
			r.base_elevation_m,
			r.vertical_m,
			r.num_courses,
			r.longest_course_km
		FROM avg_range_data ard
		JOIN resorts r ON r.id = ard.resort_id
		WHERE ard.years_with_data >= 1
		%s
		ORDER BY ard.avg_snowfall DESC
		LIMIT ?
	`, groupYearExpr, dateFilter, prefectureClause)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query snowiest resorts: %w", err)
	}
	defer rows.Close()

	return scanWeeklyResortStats(rows)
}

// scanWeeklyResortStats scans sql rows into a slice of WeeklyResortStats.
func scanWeeklyResortStats(rows *sql.Rows) ([]models.WeeklyResortStats, error) {
	results := []models.WeeklyResortStats{}
	for rows.Next() {
		var stat models.WeeklyResortStats
		if err := rows.Scan(&stat.ResortID, &stat.Name, &stat.Prefecture, &stat.TotalSnowfall, &stat.YearsWithData,
			&stat.TopElevationM, &stat.BaseElevationM, &stat.VerticalM, &stat.NumCourses, &stat.LongestCourseKM); err != nil {
			return nil, fmt.Errorf("scan result: %w", err)
		}
		results = append(results, stat)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rows: %w", err)
	}
	return results, nil
}

// GetAllResortsWithPeaks returns all resorts that have at least one peak period,
// with their associated peak periods pre-loaded. Results are ordered by prefecture,
// resort name, and peak rank.
func (r *ReaderRepository) GetAllResortsWithPeaks(ctx context.Context) ([]models.ResortWithPeaks, error) {
	// Single JOIN query to fetch resorts and their peaks together
	query := `
		SELECT r.id, r.slug, r.name, r.prefecture, r.region,
			   r.top_elevation_m, r.base_elevation_m, r.vertical_m,
			   r.num_courses, r.longest_course_km, r.steepest_course_deg,
			   r.last_updated,
			   p.id, p.peak_rank, p.start_doy, p.end_doy, p.center_doy,
			   p.avg_daily_snowfall, p.total_period_snowfall, p.prominence_score,
			   p.years_of_data, p.confidence_level, p.reliability_score,
			   p.winters_present, p.total_winters, p.regional_consistency,
			   p.calculated_at
		FROM resorts r
		INNER JOIN resort_peak_periods p ON r.id = p.resort_id
		ORDER BY r.prefecture, r.name, p.peak_rank
	`

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query resorts with peaks: %w", err)
	}
	defer rows.Close()

	// Rows are ordered by resort; use a last-seen-id sentinel instead of a map
	// to avoid a second pass and an allocation for the ordering slice.
	var results []models.ResortWithPeaks
	var lastID string

	for rows.Next() {
		var resort models.Resort
		var peak models.PeakPeriod
		var startDOY, endDOY, centerDOY int
		if err := rows.Scan(
			&resort.ID, &resort.Slug, &resort.Name, &resort.Prefecture, &resort.Region,
			&resort.TopElevationM, &resort.BaseElevationM, &resort.VerticalM,
			&resort.NumCourses, &resort.LongestCourseKM, &resort.SteepestCourseDeg,
			&resort.LastUpdated,
			&peak.ID, &peak.PeakRank, &startDOY, &endDOY, &centerDOY,
			&peak.AvgDailySnowfall, &peak.TotalPeriodSnowfall, &peak.ProminenceScore,
			&peak.YearsOfData, &peak.ConfidenceLevel, &peak.ReliabilityScore,
			&peak.WintersPresent, &peak.TotalWinters, &peak.RegionalConsistency,
			&peak.CalculatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan resort with peak: %w", err)
		}

		var convErr error
		peak.StartDate, convErr = doyToMMDD(startDOY)
		if convErr != nil {
			return nil, fmt.Errorf("convert start_doy: %w", convErr)
		}
		peak.EndDate, convErr = doyToMMDD(endDOY)
		if convErr != nil {
			return nil, fmt.Errorf("convert end_doy: %w", convErr)
		}
		peak.CenterDate, convErr = doyToMMDD(centerDOY)
		if convErr != nil {
			return nil, fmt.Errorf("convert center_doy: %w", convErr)
		}
		peak.ResortID = resort.ID

		if resort.ID != lastID {
			results = append(results, models.ResortWithPeaks{
				Resort: resort,
				Peaks:  []models.PeakPeriod{},
			})
			lastID = resort.ID
		}
		results[len(results)-1].Peaks = append(results[len(results)-1].Peaks, peak)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rows: %w", err)
	}

	return results, nil
}

// GetPendingFailedScrapeAttempts returns all failed scrape attempts that have not
// yet been retried, ordered by failure time ascending.
func (r *ReaderRepository) GetPendingFailedScrapeAttempts(ctx context.Context) ([]models.FailedScrapeAttempt, error) {
	query := `
		SELECT id, resort_url, error_message, failed_at, retried, retried_at
		FROM failed_scrape_attempts
		WHERE retried = FALSE
		ORDER BY failed_at ASC
	`

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query failed scrape attempts: %w", err)
	}
	defer rows.Close()

	var attempts []models.FailedScrapeAttempt
	for rows.Next() {
		var a models.FailedScrapeAttempt
		if err := rows.Scan(&a.ID, &a.ResortURL, &a.ErrorMessage, &a.FailedAt, &a.Retried, &a.RetriedAt); err != nil {
			return nil, fmt.Errorf("scan failed scrape attempt: %w", err)
		}
		attempts = append(attempts, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rows: %w", err)
	}

	return attempts, nil
}

// GetPeakPeriodsForResort returns all peak periods for the given resort,
// ordered by peak rank ascending.
func (r *ReaderRepository) GetPeakPeriodsForResort(ctx context.Context, resortID string) ([]models.PeakPeriod, error) {
	query := `
		SELECT id, resort_id, peak_rank, start_doy, end_doy, center_doy,
			   avg_daily_snowfall, total_period_snowfall, prominence_score,
			   years_of_data, confidence_level, reliability_score,
			   winters_present, total_winters, regional_consistency,
			   calculated_at
		FROM resort_peak_periods
		WHERE resort_id = ?
		ORDER BY peak_rank
	`

	rows, err := r.db.QueryContext(ctx, query, resortID)
	if err != nil {
		return nil, fmt.Errorf("query peak periods: %w", err)
	}
	defer rows.Close()

	var peaks []models.PeakPeriod
	for rows.Next() {
		var peak models.PeakPeriod
		var startDOY, endDOY, centerDOY int
		if err := rows.Scan(
			&peak.ID, &peak.ResortID, &peak.PeakRank, &startDOY, &endDOY, &centerDOY,
			&peak.AvgDailySnowfall, &peak.TotalPeriodSnowfall, &peak.ProminenceScore,
			&peak.YearsOfData, &peak.ConfidenceLevel, &peak.ReliabilityScore,
			&peak.WintersPresent, &peak.TotalWinters, &peak.RegionalConsistency,
			&peak.CalculatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan peak period: %w", err)
		}
		var convErr error
		peak.StartDate, convErr = doyToMMDD(startDOY)
		if convErr != nil {
			return nil, fmt.Errorf("convert start_doy: %w", convErr)
		}
		peak.EndDate, convErr = doyToMMDD(endDOY)
		if convErr != nil {
			return nil, fmt.Errorf("convert end_doy: %w", convErr)
		}
		peak.CenterDate, convErr = doyToMMDD(centerDOY)
		if convErr != nil {
			return nil, fmt.Errorf("convert center_doy: %w", convErr)
		}
		peaks = append(peaks, peak)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rows: %w", err)
	}

	return peaks, nil
}
