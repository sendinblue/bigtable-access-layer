package repository

import (
	"context"
	"embed"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/DTSL/go-bigtable-access-layer/mapping"
)

var t1 = bigtable.Time(time.Date(2020, time.January, 1, 0, 1, 0, 0, time.UTC))
var t2 = bigtable.Time(time.Date(2020, time.January, 1, 0, 2, 0, 0, time.UTC))
var t3 = bigtable.Time(time.Date(2020, time.January, 1, 0, 3, 0, 0, time.UTC))

func TestRepository_Read(t *testing.T) {

	ctx := context.Background()
	adapter := &adapter{
		ReadRow: mockReadRow,
	}
	repository := &Repository{
		adapter: adapter,
		mapper:  getMockMapper(t),
	}
	eventSet, err := repository.Read(ctx, "contact-3")
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}
	if len(eventSet.Events) != 1 {
		t.Fatalf("expected 1 event family, got %d", len(eventSet.Events))
	}
	if v, ok := eventSet.Events["front"]; !ok {
		t.Fatalf("expected front family, got %v", v)
	} else {
		if len(v) != 3 {
			t.Fatalf("expected 3 events, got %d", len(v))
		}
		if v[0].RowKey != "contact-3" {
            t.Fatalf("expected contact-3, got %s", v[0].RowKey)
        }
		if v[0].Cells["url"] != "http://someexample.url/query/string/1" {
			t.Fatalf("expected http://someexample.url/query/string/1, got %s", v[0].Cells["url"])
		}
		if v[0].Cells["device_type"] != "Smartphone" {
			t.Fatalf("expected Smartphone, got %s", v[0].Cells["device_type"])
		}
		if v[0].Cells["event_type"] != "page_view" {
			t.Fatalf("page_view, got %s", v[0].Cells["event_type"])
		}
		if v[0].Date.Unix() != t1.Time().Unix() {
			t.Fatalf("expected %v, got %v", t1.Time().Unix(), v[0].Date.Unix())
		}
	}
}

func TestRepository_Search(t *testing.T) {
	ctx := context.Background()
	adapter := &adapter{
		ReadRow: mockReadRow,
		ReadRows: mockReadRows,
	}
	repository := &Repository{
		adapter: adapter,
		mapper:  getMockMapper(t),
	}
	filter := bigtable.ColumnFilter("d")
	eventSet, err := repository.Search(ctx, filter)
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}
	if len(eventSet.Events) != 1 {
		t.Fatalf("expected 1 event family, got %d", len(eventSet.Events))
	}
	if v, ok := eventSet.Events["front"]; !ok {
		t.Fatalf("expected front family, got %v", v)
	} else {
		if len(v) != 3 {
			t.Fatalf("expected 3 events, got %d", len(v))
		}
		if v[0].Cells["url"] != "http://someexample.url/query/string/1" {
			t.Fatalf("expected http://someexample.url/query/string/1, got %s", v[0].Cells["url"])
		}
		if v[0].Cells["device_type"] != "Smartphone" {
			t.Fatalf("expected Smartphone, got %s", v[0].Cells["device_type"])
		}
		if v[0].Cells["event_type"] != "page_view" {
			t.Fatalf("page_view, got %s", v[0].Cells["event_type"])
		}
		if v[0].Date.Unix() != t1.Time().Unix() {
			t.Fatalf("expected %v, got %v", t1.Time().Unix(), v[0].Date.Unix())
		}
	}

}
//go:embed testdata/mapping.json
var fs embed.FS

func getMockMapper(t *testing.T) *mapping.Mapper {
	c, err := fs.ReadFile("testdata/mapping.json")
	if err != nil {
		t.Fatalf("failed to read mapping.json: %v", err)
	}
	jsonMapping, err := mapping.LoadMapping(c)
	if err != nil {
		t.Fatalf("failed to load mapping: %v", err)
	}
	return mapping.NewMapper(jsonMapping)
}

func mockReadRows(_ context.Context, _ bigtable.RowSet, f func(bigtable.Row) bool, _ ...bigtable.ReadOption) (err error) {
	for _, row := range getRows() {
		f(row)
	}
	return nil
}

func getRows() []bigtable.Row {
	return []bigtable.Row{
		{
			"front": []bigtable.ReadItem{
				{
					Row:       "contact-3",
					Column:    "d",
					Value:     []byte("1"),
					Timestamp: t1,
				},
				{
					Row:       "contact-3",
					Column:    "d",
					Value:     []byte("1"),
					Timestamp: t2,
				},
				{
					Row:       "contact-3",
					Column:    "d",
					Value:     []byte("1"),
					Timestamp: t3,
				},
			},
		},
	}
}

func mockReadRow(_ context.Context, row string, _ ...bigtable.ReadOption) (bigtable.Row, error) {
	output := bigtable.Row{
		"front": []bigtable.ReadItem{
			{
				Row:       row,
				Column:    "u",
				Timestamp: t1,
				Value:     []byte("http://someexample.url/query/string/1"),
			},
			{
				Row:       row,
				Column:    "d",
				Timestamp: t1,
				Value:     []byte("1"),
			},
			{
				Row:       row,
				Column:    "1",
				Timestamp: t1,
				Value:     []byte("1"),
			},
			{
				Row:       row,
				Column:    "u",
				Timestamp: t2,
				Value:     []byte("http://someexample.url/query/string/1"),
			},
			{
				Row:       row,
				Column:    "d",
				Timestamp: t2,
				Value:     []byte("1"),
			},
			{
				Row:       row,
				Column:    "1",
				Timestamp: t2,
				Value:     []byte("2"),
			},
			{
				Row:       row,
				Column:    "u",
				Timestamp: t3,
				Value:     []byte("http://someexample.url/query/string/1"),
			},
			{
				Row:       row,
				Column:    "d",
				Timestamp: t3,
				Value:     []byte("1"),
			},
			{
				Row:       row,
				Column:    "1",
				Timestamp: t3,
				Value:     []byte("3"),
			},
		},
	}
	return output, nil
}
