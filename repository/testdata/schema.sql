-- Combined schema for test databases.
-- Derived from snowfinder_scraper/migrations/ (all Up sections, goose directives stripped).

CREATE TABLE resorts (
    id TEXT PRIMARY KEY,
    slug TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    prefecture TEXT,
    region TEXT,
    top_elevation_m INTEGER,
    base_elevation_m INTEGER,
    vertical_m INTEGER,
    num_courses INTEGER,
    longest_course_km REAL,
    steepest_course_deg REAL,
    latitude REAL,
    longitude REAL,
    last_updated TEXT DEFAULT (datetime('now')),
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX idx_resorts_prefecture ON resorts (prefecture);

CREATE TABLE snow_depth_readings (
    resort_id TEXT NOT NULL REFERENCES resorts(id) ON DELETE CASCADE,
    date TEXT NOT NULL,
    depth_cm INTEGER NOT NULL,
    created_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (resort_id, date)
);

CREATE TABLE daily_snowfall (
    resort_id TEXT NOT NULL REFERENCES resorts(id) ON DELETE CASCADE,
    date TEXT NOT NULL,
    snowfall_cm INTEGER NOT NULL,
    created_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (resort_id, date)
);

CREATE INDEX idx_daily_snowfall_date ON daily_snowfall (date);

CREATE TABLE resort_peak_periods (
    id TEXT PRIMARY KEY,
    resort_id TEXT NOT NULL REFERENCES resorts(id) ON DELETE CASCADE,
    peak_rank INTEGER NOT NULL,
    start_doy INTEGER NOT NULL,
    end_doy INTEGER NOT NULL,
    center_doy INTEGER NOT NULL,
    avg_daily_snowfall REAL NOT NULL,
    total_period_snowfall REAL NOT NULL,
    prominence_score REAL,
    years_of_data INTEGER NOT NULL,
    confidence_level TEXT NOT NULL,
    calculated_at TEXT DEFAULT (datetime('now')),
    reliability_score REAL NOT NULL DEFAULT 0.0,
    winters_present INTEGER NOT NULL DEFAULT 0,
    total_winters INTEGER NOT NULL DEFAULT 0,
    regional_consistency REAL NOT NULL DEFAULT 0.0,
    UNIQUE (resort_id, peak_rank)
);

CREATE TABLE web_resort_data (
    resort_id TEXT NOT NULL REFERENCES resorts(id) ON DELETE CASCADE,
    data_type TEXT NOT NULL,
    data TEXT NOT NULL,
    updated_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (resort_id, data_type)
);

CREATE TABLE predictions (
    resort_id TEXT PRIMARY KEY REFERENCES resorts(id) ON DELETE CASCADE,
    prediction_data TEXT NOT NULL,
    generated_at TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE prediction_config (
    resort_id TEXT PRIMARY KEY REFERENCES resorts(id) ON DELETE CASCADE,
    config_data TEXT NOT NULL,
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE prediction_global_params (
    id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    params_data TEXT NOT NULL,
    jma_office_codes TEXT,
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE failed_scrape_attempts (
    id TEXT PRIMARY KEY,
    resort_url TEXT NOT NULL,
    error_message TEXT NOT NULL,
    failed_at TEXT DEFAULT (datetime('now')),
    retried INTEGER DEFAULT 0,
    retried_at TEXT
);

CREATE TABLE msm_historical (
    resort_id TEXT NOT NULL REFERENCES resorts(id) ON DELETE CASCADE,
    timestamp TEXT NOT NULL,
    temp_surface_k REAL,
    rh_surface_pct REAL,
    u_wind_ms REAL,
    v_wind_ms REAL,
    cloud_total_pct REAL,
    cloud_low_pct REAL,
    precip_mm REAL,
    temp_925hpa_k REAL,
    temp_850hpa_k REAL,
    rh_850hpa_pct REAL,
    PRIMARY KEY (resort_id, timestamp)
);
