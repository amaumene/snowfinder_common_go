package repository

import (
	"testing"
)

func TestDoyToMMDD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		doy     int
		want    string
		wantErr bool
	}{
		{name: "first day of year", doy: 1, want: "01-01"},
		{name: "last day of February non-leap", doy: 59, want: "02-28"},
		{name: "leap day (year 2000 is leap)", doy: 60, want: "02-29"},
		{name: "last day of year", doy: 366, want: "12-31"},
		{name: "invalid: zero", doy: 0, wantErr: true},
		{name: "invalid: 367", doy: 367, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := doyToMMDD(tc.doy)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("doyToMMDD(%d) = %q, want error", tc.doy, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("doyToMMDD(%d) unexpected error: %v", tc.doy, err)
			}
			if got != tc.want {
				t.Fatalf("doyToMMDD(%d) = %q, want %q", tc.doy, got, tc.want)
			}
		})
	}
}

func TestMmddToDOY(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mmdd    string
		want    int
		wantErr bool
	}{
		{name: "January 1st", mmdd: "01-01", want: 1},
		{name: "leap day", mmdd: "02-29", want: 60},
		{name: "last day of year", mmdd: "12-31", want: 366},
		{name: "invalid month 13", mmdd: "13-01", wantErr: true},
		{name: "non-numeric", mmdd: "abc", wantErr: true},
		{name: "empty string", mmdd: "", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := mmddToDOY(tc.mmdd)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("mmddToDOY(%q) = %d, want error", tc.mmdd, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("mmddToDOY(%q) unexpected error: %v", tc.mmdd, err)
			}
			if got != tc.want {
				t.Fatalf("mmddToDOY(%q) = %d, want %d", tc.mmdd, got, tc.want)
			}
		})
	}
}

func TestDoyToMMDD_RoundTrip(t *testing.T) {
	t.Parallel()

	const input = "02-29"
	doy, err := mmddToDOY(input)
	if err != nil {
		t.Fatalf("mmddToDOY(%q) unexpected error: %v", input, err)
	}
	got, err := doyToMMDD(doy)
	if err != nil {
		t.Fatalf("doyToMMDD(%d) unexpected error: %v", doy, err)
	}
	if got != input {
		t.Fatalf("round-trip %q → %d → %q, want %q", input, doy, got, input)
	}
}
