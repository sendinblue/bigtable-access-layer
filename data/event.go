/*
Package data provides the structs that embed the event data.
there are two of them:
 - data.Set which includes a set of data.Event and the mapped column names
 - data.Event which includes all data related to a single event: its date, its row-key and all its properties in the Cells map.
 */
package data

import "time"

// Event is a single event from a row.
type Event struct {
	RowKey string
	Date   time.Time
	Cells  map[string]string
}

// Set is the set of events contained in a row.
type Set struct {
	Columns []string
	Events  map[string][]*Event
}
