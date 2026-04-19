package repository

import (
	"database/sql"
	"strings"
	"testing"
	_ "embed"

	_ "modernc.org/sqlite"
)

// Migration SQL files from the scraper package, embedded at test compile time.
// These are the canonical schema definitions; keeping them here avoids
// duplicating CREATE TABLE statements in individual test files.
//
//go:embed testdata/schema.sql
var testSchema string

// newTestDB opens an in-memory SQLite database and applies the full production
// schema. It registers a cleanup function to close the DB when the test ends.
func newTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	// Apply each statement individually (the schema file uses ";" as separator).
	for _, stmt := range splitSQL(testSchema) {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("apply schema statement %q: %v", stmt[:min(len(stmt), 80)], err)
		}
	}

	return db
}

// splitSQL splits a SQL file into individual statements, stripping goose
// directives and blank lines.
func splitSQL(src string) []string {
	var stmts []string
	for _, raw := range strings.Split(src, ";") {
		var lines []string
		for _, line := range strings.Split(raw, "\n") {
			trimmed := strings.TrimSpace(line)
			if trimmed == "" || strings.HasPrefix(trimmed, "--") {
				continue
			}
			lines = append(lines, line)
		}
		if stmt := strings.TrimSpace(strings.Join(lines, "\n")); stmt != "" {
			stmts = append(stmts, stmt)
		}
	}
	return stmts
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// newTestDBWithPredictionsSchema opens an in-memory SQLite database and applies
// a custom schema. Used by tests that need a non-standard predictions table
// (e.g. with a CHECK constraint to trigger rollback).
func newTestDBWithPredictionsSchema(t *testing.T, schema string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("create predictions table: %v", err)
	}

	return db
}
