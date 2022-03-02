package mapping

import (
	"strings"

	"cloud.google.com/go/bigtable"
	"github.com/sendinblue/bigtable-access-layer/data"
)

// Mapper is in charge of translating data from Big Table into a human-readable format.
type Mapper struct {
	// mapping coming from the JSON file
	*Mapping
	// those functions are in charge of seeking data
	rules *rules
}

type rules struct {
	toBigTable, toEvent []func(m *Mapping, column string, value string) (bool, string, string)
}

func NewMapper(mapping *Mapping) *Mapper {
	rev := newReverseSeeker()
	toEvent := []func(m *Mapping, column string, value string) (bool, string, string){
		seekFromShortColumn,
		seekFromMappedColumn,
		rev.seekFromCache,
		rev.seekFromMapping,
	}
	toBigTable := []func(m *Mapping, column string, value string) (bool, string, string){
		turnToShortColumn,
		turnToMappedColumnValue,
		turnToReversedColumnValue,
	}
	return &Mapper{
		rules: &rules{
			toBigTable: toBigTable,
			toEvent:    toEvent,
		},
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
		col, val := getMappedData(m.Mapping, m.rules.toEvent, removePrefix(item.Column), string(item.Value))
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

func (m *Mapper) GetMutations(eventSet *data.Set) map[string]*bigtable.Mutation {
	mutations := make(map[string]*bigtable.Mutation)
	for family, events := range eventSet.Events {
		for _, event := range events {
			if _, ok := mutations[event.RowKey]; !ok {
				mutations[event.RowKey] = bigtable.NewMutation()
			}
			for name, value := range event.Cells {
				btName, btValue := getMappedData(m.Mapping, m.rules.toBigTable, name, value)
				mutations[event.RowKey].Set(family, btName, bigtable.Time(event.Date), []byte(btValue))
			}
		}
	}
	return mutations
}

// getMappedData uses all `rules` to find the appropriate mapping method and return the mapped column + value.
func getMappedData(mapping *Mapping, rules []func(m *Mapping, column string, value string) (bool, string, string), column string, value string) (string, string) {
	for _, seek := range rules {
		if ok, col, val := seek(mapping, column, value); ok {
			return col, val
		}
	}
	return column, value
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
