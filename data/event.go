package data

import "time"

// Event is a single event from a row.
type Event struct {
	Date  time.Time
	Cells map[string]string
}

// Set is the set of events contained in a row.
type Set struct {
	Columns []string
	Events  map[string][]*Event
}
