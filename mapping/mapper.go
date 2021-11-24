package mapping

import (
	"strings"

	"cloud.google.com/go/bigtable"
	"github.com/DTSL/go-bigtable-access-layer/data"
)

// Mapper is in charge of translating data from Big Table into a human-readable format.
type Mapper struct {
	// mapping coming from the JSON file
	*Mapping
	// those functions are in charge of seeking data
	seekers []func(m *Mapping, column string, value string) (bool, string, string)
}

func NewMapper(mapping *Mapping, extraSeekers ...func(m *Mapping, column string, value string) (bool, string, string)) *Mapper {
	rev := newReverseSeeker()
	seekers := []func(m *Mapping, column string, value string) (bool, string, string){
		seekRaw,
		seekMapped,
		rev.seekFromCache,
		rev.seekFromMapping,
	}
	seekers = append(seekers, extraSeekers...)
	return &Mapper{
		seekers: seekers,
		Mapping: mapping,
	}
}

// GetMappedEvents translates a slice of bigtable.ReadItem into a slice of data.Event.
// It uses the Mapping to know which columns to seek and each event is identified by the timestamp
// of the bigtable.ReadItem. So assuming there's a slice of 20 bigtable.ReadItem with the same timestamp,
// then the returned slice will have 1 data.Event containing a slice of 20 Cells.
func (m *Mapper) GetMappedEvents(items []bigtable.ReadItem) ([]string, []*data.Event) {
	cols := make(map[string]bool)
	rows := make(map[string]map[bigtable.Timestamp]map[string]string)
	for _, item := range items {
		col, val := m.Seek(removePrefix(item.Column), string(item.Value))
		cols[col] = true
		if _, ok := rows[item.Row]; !ok {
			rows[item.Row] = make(map[bigtable.Timestamp]map[string]string)
		}
		if _, ok := rows[item.Row][item.Timestamp]; !ok {
			rows[item.Row][item.Timestamp] = make(map[string]string)
		}
		rows[item.Row][item.Timestamp][col] = val
	}
	return processColumns(cols), processRows(rows)
}

func processColumns(cols map[string]bool) []string {
	columns := make([]string, 0, len(cols))
	for c := range cols {
		columns = append(columns, c)
	}
	return columns
}

func processRows(r map[string]map[bigtable.Timestamp]map[string]string) []*data.Event {
	events := make([]*data.Event, 0)
	for key, row := range r {
		for ts, cells := range row {
			event := &data.Event{
				Date:   ts.Time(),
				Cells:  cells,
				RowKey: key,
			}
			events = append(events, event)
		}
	}
	return events
}

func removePrefix(col string) string {
	s := strings.Split(col, ":")
	if len(s) > 1 {
		return s[1]
	}
	return col
}

// Seek uses all `seekers` to find the appropriate mapping method and return the mapped column + value.
func (m *Mapper) Seek(column string, value string) (string, string) {
	for _, seek := range m.seekers {
		if ok, col, val := seek(m.Mapping, column, value); ok {
			return col, val
		}
	}
	return column, value
}

// seekRaw is a default seeker that simply translates the column's name if a match exists.
func seekRaw(m *Mapping, column string, value string) (bool, string, string) {
	r, ok := m.Raws[column]
	if ok {
		return true, r, value
	}
	return false, "", ""
}

// seekMapped checks if a map exists for the given column's short name and returns the full column from the mapping + the mapped value.
// example with this mapping:
// "mapped": {
//    "oi": {
//      "name": "is_opted_in",
//      "values": {
//        "0": "false",
//        "1": "true"
//      }
//    }
//  },
// this call would return "is_opted_in" and "true": seekMapped(m, "oi", "1")
func seekMapped(m *Mapping, column string, value string) (bool, string, string) {
	ma, ok := m.Mapped[column]
	if ok {
		v, ok := ma.Values[value]
		if ok {
			return true, ma.Name, v
		}
		return true, ma.Name, value
	}
	return false, "", ""
}

// cacheEntry is a cache entry for the reverse Seeker.
type cacheEntry struct {
	col   string
	value string
}

// reverseSeeker is to use for data stored in a "reversed way" meaning that the column qualifier is the actual data and is a kind of enum value
type reverseSeeker struct {
	cache map[string]*cacheEntry
}

func newReverseSeeker() *reverseSeeker {
	return &reverseSeeker{
		cache: make(map[string]*cacheEntry),
	}
}

// seekFromCache returns the column and value from the cache if it exists.
func (c *reverseSeeker) seekFromCache(_ *Mapping, column string, _ string) (bool, string, string) {
	if entry, ok := c.cache[column]; ok {
		return true, entry.col, entry.value
	}
	return false, "", ""
}

// seekFromMapping returns the column and value from the mapping.
func (c *reverseSeeker) seekFromMapping(m *Mapping, column string, _ string) (bool, string, string) {
	for _, ma := range m.Reversed {
		for short, val := range ma.Values {
			if column == short {
				c.cache[column] = &cacheEntry{col: ma.Name, value: val}
				return true, ma.Name, val
			}
		}
	}
	return false, "", ""
}
