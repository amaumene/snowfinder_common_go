package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/amaumene/snowfinder_common/models"
)

// globalParamsID is the fixed row ID for the single global-params record.
// The table is designed to hold exactly one row; there is no LoadPredictions
// method on this repository because predictions are read by the web service
// from its own in-process store, not from this repo.
const globalParamsID = 1

// PredictionRepository provides access to prediction-related tables.
type PredictionRepository struct {
	db *sql.DB
}

// NewPredictionRepository creates a new prediction repository.
func NewPredictionRepository(db *sql.DB) *PredictionRepository {
	return &PredictionRepository{
		db: db,
	}
}

// LoadPredictionConfig loads per-resort config from prediction_config table.
func (r *PredictionRepository) LoadPredictionConfig(ctx context.Context) (map[string]models.PredictorResortConfig, error) {
	rows, err := r.db.QueryContext(ctx, "SELECT resort_id, config_data FROM prediction_config")
	if err != nil {
		return nil, fmt.Errorf("query prediction_config: %w", err)
	}
	defer rows.Close()

	resorts := make(map[string]models.PredictorResortConfig)
	for rows.Next() {
		var resortID string
		var configData []byte
		if err := rows.Scan(&resortID, &configData); err != nil {
			return nil, fmt.Errorf("scan prediction_config: %w", err)
		}
		var cfg models.PredictorResortConfig
		if err := json.Unmarshal(configData, &cfg); err != nil {
			return nil, fmt.Errorf("unmarshal config for %s: %w", resortID, err)
		}
		resorts[resortID] = cfg
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate prediction_config rows: %w", err)
	}

	return resorts, nil
}

// LoadGlobalParams loads global predictor parameters.
// Returns a zero-value GlobalParams (not an error) when no row exists.
func (r *PredictionRepository) LoadGlobalParams(ctx context.Context) (models.GlobalParams, error) {
	var paramsData []byte
	err := r.db.QueryRowContext(ctx,
		"SELECT params_data FROM prediction_global_params WHERE id = ?",
		globalParamsID,
	).Scan(&paramsData)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return models.GlobalParams{}, nil
		}
		return models.GlobalParams{}, fmt.Errorf("query global_params: %w", err)
	}
	var params models.GlobalParams
	if err := json.Unmarshal(paramsData, &params); err != nil {
		return models.GlobalParams{}, fmt.Errorf("unmarshal global_params: %w", err)
	}
	return params, nil
}

// SavePredictions upserts all predictions using INSERT ON CONFLICT.
func (r *PredictionRepository) SavePredictions(ctx context.Context, predictions *models.PredictionData) error {
	if predictions == nil {
		return errors.New("nil prediction data")
	}
	if len(predictions.Resorts) == 0 {
		return nil
	}
	if _, err := time.Parse(time.RFC3339, predictions.GeneratedAt); err != nil {
		return fmt.Errorf("invalid generated_at %q: %w", predictions.GeneratedAt, err)
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin prediction transaction: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	query := `INSERT INTO predictions (resort_id, prediction_data, generated_at)
		VALUES (?, ?, ?)
		ON CONFLICT (resort_id) DO UPDATE
		SET prediction_data = EXCLUDED.prediction_data,
		    generated_at = EXCLUDED.generated_at`

	for resortID, pred := range predictions.Resorts {
		predJSON, err := json.Marshal(pred)
		if err != nil {
			return fmt.Errorf("marshal prediction for %s: %w", resortID, err)
		}
		if _, err := tx.ExecContext(ctx, query, resortID, predJSON, predictions.GeneratedAt); err != nil {
			return fmt.Errorf("saving prediction for resort %s: %w", resortID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit predictions: %w", err)
	}

	return nil
}
