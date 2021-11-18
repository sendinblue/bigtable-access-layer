package aggregation

import (
	"strconv"

	"github.com/DTSL/go-bigtable-access-layer/data"
)

// GetLatestBy groups lines by the given columns keeping the most recent one, thus without performing any aggregation.
func GetLatestBy(events []*data.Event, columns ...string) map[string]*data.Event {
	result := make(map[string]*data.Event)
	for _, event := range events {
		key := ""
		for _, column := range columns {
			if d, ok := event.Cells[column]; ok {
				key += d
			}
		}
		if _, ok := result[key]; !ok || event.Date.After(result[key].Date) {
			result[key] = event
		}
	}
	return result
}

// GroupBy groups lines by the given columns, performing the given aggregation function.
func GroupBy(events []*data.Event, agg func(line *data.Event, lines []*data.Event) *data.Event, columns ...string) map[string]*data.Event {
	result := make(map[string]*data.Event)
	group := make(map[string][]*data.Event)
	for _, event := range events {
		key := ""
		for _, column := range columns {
			if d, ok := event.Cells[column]; ok {
				key += d
			}
		}
		if _, ok := group[key]; !ok {
			group[key] = make([]*data.Event, 0)
		}
		result[key] = agg(event, group[key])
		group[key] = append(group[key], event)
	}
	return result
}

// aggregate is the core struct that contains the properties of the aggregation.
type aggregate struct {
	column     string
	projection string
}

// Count returns the number of lines in the given group.
type Count struct {
	projection string
}

func NewCount(column string) *Count {
	return &Count{
		projection: column,
	}
}

func (c *Count) Compute(e *data.Event, events []*data.Event) *data.Event {
	e.Cells[c.projection] = strconv.Itoa(len(events) + 1)
	return e
}

// Max returns the maximum value of the given column in the given group.
type Max struct {
	aggregate
}

func NewMax(column string, projection string) *Max {
	return &Max{
		aggregate: aggregate{
			column:     column,
			projection: projection,
		},
	}
}

func (m *Max) Compute(e *data.Event, events []*data.Event) *data.Event {
	e.Cells[m.projection] = selectOne(e, events, m.column, func(c, s float64) bool {
        return c > s
    })
	return e
}

// Min returns the minimum value of the given column in the given group.
type Min struct {
	aggregate
}

func NewMin(column string, projection string) *Min {
	return &Min{
		aggregate: aggregate{
			column:     column,
			projection: projection,
		},
	}
}

func (m *Min) Compute(e *data.Event, events []*data.Event) *data.Event {
	e.Cells[m.projection] = selectOne(e, events, m.column, func(c, s float64) bool {
		return s == 0 || c < s
	})
	return e
}

func selectOne(e *data.Event, events []*data.Event, col string, f func(c, s float64) bool) string {
	var selected float64
	events = append(events, e)
	for _, line := range events {
		if d, ok := line.Cells[col]; ok {
			if v, err := strconv.ParseFloat(d, 64); err == nil {
				if f(v, selected) {
					selected = v
				}
			}
		}
	}
	return strconv.FormatFloat(selected, 'f', -1, 64)
}

// Average returns the average value of the given column in the given group.
type Average struct {
	aggregate
}

func NewAverage(column string, projection string) *Average {
	return &Average{
		aggregate: aggregate{
			column:     column,
			projection: projection,
		},
	}
}

func (m *Average) Compute(e *data.Event, events []*data.Event) *data.Event {
	total := sum(m.column, e, events)
	e.Cells[m.projection] = strconv.FormatFloat(total/float64(len(events)+1), 'f', -1, 64)
	return e
}

// Sum returns the sum of the given column in the given group.
type Sum struct {
	aggregate
}

func NewSum(column string, projection string) *Sum {
	return &Sum{
		aggregate: aggregate{
			column:     column,
			projection: projection,
		},
	}
}

func (m *Sum) Compute(e *data.Event, events []*data.Event) *data.Event {
	total := sum(m.column, e, events)
	e.Cells[m.projection] = strconv.FormatFloat(total, 'f', -1, 64)
	return e
}

func sum(column string, e *data.Event, events []*data.Event) float64 {
	total := 0.0
	events = append(events, e)
	for _, line := range events {
		if d, ok := line.Cells[column]; ok {
			if v, err := strconv.ParseFloat(d, 64); err == nil {
				total += v
			}
		}
	}
	return total
}

// Set is a set of aggregations. It is designed to apply several aggregations to the same line.
type Set struct {
	aggs []func(e *data.Event, events []*data.Event) *data.Event
}

func NewAggregationSet() *Set {
	return &Set{}
}

func (s *Set) Add(agg func(e *data.Event, events []*data.Event) *data.Event) {
	s.aggs = append(s.aggs, agg)
}

func (s *Set) Compute(e *data.Event, events []*data.Event) *data.Event {
	for _, agg := range s.aggs {
		e = agg(e, events)
	}
	return e
}
