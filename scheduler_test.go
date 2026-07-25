package main

import (
	"testing"
	"time"
)

func TestNextScheduleTime(t *testing.T) {
	start := time.Date(2026, time.June, 18, 9, 30, 15, 0, time.UTC)
	tests := []struct {
		recurring string
		want      time.Time
	}{
		{"none", start},
		{"daily", start.Add(24 * time.Hour)},
		{"weekly", start.Add(7 * 24 * time.Hour)},
		{"monthly", start.AddDate(0, 1, 0)},
		{"yearly", start.AddDate(1, 0, 0)},
	}
	for _, test := range tests {
		if got := nextScheduleTime(start, test.recurring); !got.Equal(test.want) {
			t.Fatalf("%s recurrence: got %s, want %s", test.recurring, got, test.want)
		}
	}
}

func TestValidRecurrence(t *testing.T) {
	for _, value := range []string{"none", "daily", "weekly", "monthly", "yearly"} {
		if !validRecurrence(value) {
			t.Fatalf("expected %q to be valid", value)
		}
	}
	if validRecurrence("hourly") {
		t.Fatal("hourly recurrence should be rejected")
	}
}

func TestSchedulerURLStreamClientHasNoWholeRequestTimeout(t *testing.T) {
	if schedulerURLStreamClient.Timeout != 0 {
		t.Fatalf("scheduler URL streams must not have a whole-request timeout; got %s", schedulerURLStreamClient.Timeout)
	}
}
