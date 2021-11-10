package data

import "time"

// Event is a single event from a row.
type Event struct {
	Date  time.Time
	Cells map[string]string
}
