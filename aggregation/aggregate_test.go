package aggregation

import (
	"testing"
	"time"

	"github.com/DTSL/go-bigtable-access-layer/data"
)

func TestGroupByOne(t *testing.T) {
	events := []*data.Event{
		{
			Date: time.Now().AddDate(0, 0, -4),
			Cells: map[string]string{
				"id":     "12",
				"status": "added",
			},
		},
		{
			Date: time.Now().AddDate(0, 0, -1),
			Cells: map[string]string{
				"id":     "12",
				"status": "confirmed",
			},
		},
		{
			Date: time.Now().AddDate(0, 0, -4),
			Cells: map[string]string{
				"id":     "15",
				"status": "added",
			},
		},
		{
			Date: time.Now().AddDate(0, 0, -8),
			Cells: map[string]string{
				"id":     "12",
				"status": "added",
			},
		},
		{
			Date: time.Now().AddDate(0, 0, -1),
			Cells: map[string]string{
				"id":     "15",
				"status": "canceled",
			},
		},
		{
			Date: time.Now().AddDate(0, 0, -10),
			Cells: map[string]string{
				"id":     "16",
				"status": "added",
			},
		},
	}

	result := GroupBy(events, "id")
	expected := map[string]*data.Event{
		"12": events[1],
		"15": events[4],
		"16": events[5],
	}
	comp(t, result, expected)
}

func TestGroupBy(t *testing.T) {
	lines := []*data.Event{
		{
			Date: time.Now().AddDate(-25, 0, 0),
			Cells: map[string]string{
				"country":    "france",
				"city":       "paris",
				"population": "1200000",
			},
		},
		{
			Date: time.Now().AddDate(-1, 0, 0),
			Cells: map[string]string{
				"country":    "france",
				"city":       "paris",
				"population": "1500000",
			},
		},
		{
			Date: time.Now().AddDate(-25, 0, 0),
			Cells: map[string]string{
				"country":    "texas",
				"city":       "paris",
				"population": "400000",
			},
		},
		{
			Date: time.Now().AddDate(-12, 0, 0),
			Cells: map[string]string{
				"country":    "texas",
				"city":       "paris",
				"population": "450000",
			},
		},
		{
			Date: time.Now().AddDate(-2, 0, 0),
			Cells: map[string]string{
				"country":    "texas",
				"city":       "paris",
				"population": "500000",
			},
		},
		{
			Date: time.Now().AddDate(-25, 0, 0),
			Cells: map[string]string{
				"country":    "france",
				"city":       "marseille",
				"population": "900000",
			},
		},
		{
			Date: time.Now().AddDate(0, 0, -1),
			Cells: map[string]string{
				"country":    "france",
				"city":       "marseille",
				"population": "1200000",
			},
		},
		{
			Date: time.Now().AddDate(0, 0, -10),
			Cells: map[string]string{
				"country":    "germany",
				"city":       "berlin",
				"population": "1800000",
			},
		},
	}

	result := GroupBy(lines, "country", "city")
	expected := map[string]*data.Event{
		"francemarseille": lines[6],
		"franceparis":     lines[1],
		"germanyberlin":   lines[7],
		"texasparis":      lines[4],
	}
	comp(t, result, expected)
}

func TestGroupByCount(t *testing.T) {
	lines := []*data.Event{
		{
			Date: time.Now().AddDate(-25, 0, 0),
			Cells: map[string]string{
				"country":    "france",
				"city":       "paris",
				"population": "1200000",
			},
		},
		{
			Date: time.Now().AddDate(-1, 0, 0),
			Cells: map[string]string{
				"country":    "france",
				"city":       "paris",
				"population": "1500000",
			},
		},
		{
			Date: time.Now().AddDate(-25, 0, 0),
			Cells: map[string]string{
				"country":    "texas",
				"city":       "paris",
				"population": "400000",
			},
		},
		{
			Date: time.Now().AddDate(-12, 0, 0),
			Cells: map[string]string{
				"country":    "texas",
				"city":       "paris",
				"population": "450000",
			},
		},
		{
			Date: time.Now().AddDate(-2, 0, 0),
			Cells: map[string]string{
				"country":    "texas",
				"city":       "paris",
				"population": "500000",
			},
		},
		{
			Date: time.Now().AddDate(-25, 0, 0),
			Cells: map[string]string{
				"country":    "france",
				"city":       "marseille",
				"population": "900000",
			},
		},
		{
			Date: time.Now().AddDate(0, 0, -1),
			Cells: map[string]string{
				"country":    "france",
				"city":       "marseille",
				"population": "1200000",
			},
		},
		{
			Date: time.Now().AddDate(0, 0, -10),
			Cells: map[string]string{
				"country":    "germany",
				"city":       "berlin",
				"population": "1800000",
			},
		},
	}

	cnt := NewCount("count")
	result := GroupByAggregate(lines, cnt.Compute, "country")
	lines[4].Cells["count"] = "3"
	lines[6].Cells["count"] = "4"
	lines[7].Cells["count"] = "1"

	expected := map[string]*data.Event{
		"france":  lines[6],
		"germany": lines[7],
		"texas":   lines[4],
	}
	comp(t, result, expected)
}

func TestAverage_Compute(t *testing.T) {
	lines := []*data.Event{
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "12",
				"amount": "115",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "12",
				"amount": "120",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "15",
				"amount": "200",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "12",
				"amount": "110",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "15",
				"amount": "100",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "16",
				"amount": "900",
			},
		},
	}

	avg := NewAverage("amount", "average")
	result := GroupByAggregate(lines, avg.Compute, "id")
	expected := map[string]*data.Event{
		"12": {
			Date: time.Now(),
			Cells: map[string]string{
				"id":      "12",
				"amount":  "110",
				"average": "115",
			},
		},
		"15": {
			Date: time.Now(),
			Cells: map[string]string{
				"id":      "15",
				"amount":  "100",
				"average": "150",
			},
		},
		"16": {
			Date: time.Now(),
			Cells: map[string]string{
				"id":      "16",
				"amount":  "900",
				"average": "900",
			},
		},
	}
	comp(t,  result, expected)
}

func TestSum_Compute(t *testing.T) {
	lines := []*data.Event{
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "12",
				"amount": "115",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "12",
				"amount": "120",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "15",
				"amount": "200",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "12",
				"amount": "110",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "15",
				"amount": "100",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "16",
				"amount": "900",
			},
		},
	}

	sum := NewSum("amount", "sum")
	result := GroupByAggregate(lines, sum.Compute, "id")
	expected := map[string]*data.Event{
		"12": {
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "12",
				"amount": "110",
				"sum":    "345",
			},
		},
		"15": {
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "15",
				"amount": "100",
				"sum":    "300",
			},
		},
		"16": {
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "16",
				"amount": "900",
				"sum":    "900",
			},
		},
	}
	comp(t, result, expected)
}

func TestMax_Compute(t *testing.T) {
	lines := []*data.Event{
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "12",
				"amount": "115",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "12",
				"amount": "120",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "15",
				"amount": "200",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "12",
				"amount": "110",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "15",
				"amount": "100",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "16",
				"amount": "900",
			},
		},
	}

	max := NewMax("amount", "max")
	result := GroupByAggregate(lines, max.Compute, "id")
	expected := map[string]*data.Event{
		"12": {
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "12",
				"amount": "110",
				"max":    "120",
			},
		},
		"15": {
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "15",
				"amount": "100",
				"max":    "200",
			},
		},
		"16": {
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "16",
				"amount": "900",
				"max":    "900",
			},
		},
	}
	comp(t, result, expected)
}

func TestMin_Compute(t *testing.T) {
	lines := []*data.Event{
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "12",
				"amount": "115",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "12",
				"amount": "120",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "15",
				"amount": "200",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "12",
				"amount": "110",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "15",
				"amount": "100",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "16",
				"amount": "900",
			},
		},
	}

	min := NewMin("amount", "min")
	result := GroupByAggregate(lines, min.Compute, "id")
	expected := map[string]*data.Event{
		"12": {
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "12",
				"amount": "110",
				"min":    "110",
			},
		},
		"15": {
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "15",
				"amount": "100",
				"min":    "100",
			},
		},
		"16": {
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "16",
				"amount": "900",
				"min":    "900",
			},
		},
	}
	comp(t, result, expected)
}

func TestAggregationSet_Compute(t *testing.T) {
	lines := []*data.Event{
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "12",
				"amount": "115",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "12",
				"amount": "120",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "15",
				"amount": "200",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "12",
				"amount": "110",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "15",
				"amount": "100",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "16",
				"amount": "900",
			},
		},
	}
	avg := NewAverage("amount", "average")
	sum := NewSum("amount", "sum")
	cnt := NewCount("count")

	set := NewAggregationSet()
	set.Add(avg.Compute)
	set.Add(sum.Compute)
	set.Add(cnt.Compute)

	result := GroupByAggregate(lines, set.Compute, "id")
	expected := map[string]*data.Event{
		"12": {
			Date: time.Now(),
			Cells: map[string]string{
				"id":      "12",
				"amount":  "110",
				"sum":     "345",
				"count":   "3",
				"average": "115",
			},
		},
		"15": {
			Date: time.Now(),
			Cells: map[string]string{
				"id":      "15",
				"amount":  "100",
				"sum":     "300",
				"count":   "2",
				"average": "150",
			},
		},
		"16": {
			Date: time.Now(),
			Cells: map[string]string{
				"id":      "16",
				"amount":  "900",
				"sum":     "900",
				"count":   "1",
				"average": "900",
			},
		},
	}
	comp(t, result, expected)
}

func comp(t *testing.T, result, expected map[string]*data.Event) {
	if len(result) != len(expected) {
		t.Fatalf("wrong number of events, wanted: %v, got :%v \n", len(expected), len(result))
	}
	for name, events := range result {
		if _, ok := expected[name]; ! ok {
			t.Fatalf("unexpected group %s \n", name)
		}
		for cname, cvalue := range events.Cells {
			if v, ok := expected[name].Cells[cname]; ! ok {
				t.Fatalf("unexpected column")
			} else {
				if cvalue != v {
					t.Fatalf("value mismatch, wanted: %v, got %v\n", v, cvalue)
				}
			}
		}
	}
}
