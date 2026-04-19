package repository

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/amaumene/snowfinder_common/models"
)

func TestPredictionRepositorySavePredictions_CommitsTransaction(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	repo := NewPredictionRepository(db)

	predictions := &models.PredictionData{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Resorts: map[string]models.Prediction{
			"resort-1": {Name: "One"},
			"resort-2": {Name: "Two"},
		},
	}

	if err := repo.SavePredictions(context.Background(), predictions); err != nil {
		t.Fatalf("SavePredictions() error = %v", err)
	}

	// Verify both resorts were saved
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM predictions").Scan(&count); err != nil {
		t.Fatalf("count predictions: %v", err)
	}
	if count != len(predictions.Resorts) {
		t.Fatalf("saved %d predictions, want %d", count, len(predictions.Resorts))
	}
}

func TestPredictionRepositorySavePredictions_UpsertOnConflict(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	repo := NewPredictionRepository(db)

	predictions := &models.PredictionData{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Resorts: map[string]models.Prediction{
			"resort-1": {Name: "One"},
		},
	}

	// Save once
	if err := repo.SavePredictions(context.Background(), predictions); err != nil {
		t.Fatalf("first SavePredictions() error = %v", err)
	}

	// Save again (upsert)
	if err := repo.SavePredictions(context.Background(), predictions); err != nil {
		t.Fatalf("second SavePredictions() error = %v", err)
	}

	// Should still be only 1 row
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM predictions").Scan(&count); err != nil {
		t.Fatalf("count predictions: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 prediction after upsert, got %d", count)
	}
}

func TestPredictionRepositorySavePredictions_RejectsInvalidJSON(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	repo := NewPredictionRepository(db)

	predictions := &models.PredictionData{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Resorts: map[string]models.Prediction{
			"resort-1": {
				HourlySnowfall: []float64{math.NaN()},
			},
		},
	}

	err := repo.SavePredictions(context.Background(), predictions)
	if err == nil {
		t.Fatal("expected error for NaN in JSON")
	}
}

func TestPredictionRepositorySavePredictions_NilData(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	repo := NewPredictionRepository(db)

	err := repo.SavePredictions(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error for nil prediction data")
	}
}

func TestPredictionRepositorySavePredictions_EmptyResorts(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	repo := NewPredictionRepository(db)

	predictions := &models.PredictionData{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Resorts:     map[string]models.Prediction{},
	}

	if err := repo.SavePredictions(context.Background(), predictions); err != nil {
		t.Fatalf("SavePredictions() with empty resorts error = %v", err)
	}

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM predictions").Scan(&count); err != nil {
		t.Fatalf("count predictions: %v", err)
	}
	if count != 0 {
		t.Fatalf("saved %d predictions, want 0", count)
	}
}

func TestPredictionRepositorySavePredictions_InvalidGeneratedAt(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	repo := NewPredictionRepository(db)

	predictions := &models.PredictionData{
		GeneratedAt: "not-a-date",
		Resorts: map[string]models.Prediction{
			"resort-1": {Name: "One"},
		},
	}

	err := repo.SavePredictions(context.Background(), predictions)
	if err == nil {
		t.Fatal("expected error for invalid GeneratedAt")
	}
}

func TestPredictionRepositorySavePredictions_RollsBackOnMidTransactionFailure(t *testing.T) {
	t.Parallel()

	db := newTestDBWithPredictionsSchema(t, `CREATE TABLE predictions (
		resort_id TEXT PRIMARY KEY CHECK (resort_id != 'bad-resort'),
		prediction_data BLOB NOT NULL,
		generated_at DATETIME NOT NULL
	)`)
	repo := NewPredictionRepository(db)

	predictions := &models.PredictionData{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Resorts: map[string]models.Prediction{
			"good-resort": {Name: "Good"},
			"bad-resort":  {Name: "Bad"},
		},
	}

	err := repo.SavePredictions(context.Background(), predictions)
	if err == nil {
		t.Fatal("expected error for failing insert")
	}

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM predictions").Scan(&count); err != nil {
		t.Fatalf("count predictions: %v", err)
	}
	if count != 0 {
		t.Fatalf("saved %d predictions after rollback, want 0", count)
	}
}

func TestPredictionRepositoryLoadGlobalParams_MissingRowReturnsZeroValue(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	repo := NewPredictionRepository(db)

	params, err := repo.LoadGlobalParams(context.Background())
	if err != nil {
		t.Fatalf("LoadGlobalParams() error = %v", err)
	}
	// Zero-value GlobalParams — no numeric fields set
	if params.BlendW0 != 0 || params.BlendDecay != 0 || params.BlendWeights != nil {
		t.Fatalf("LoadGlobalParams() = %+v, want zero value", params)
	}
}
