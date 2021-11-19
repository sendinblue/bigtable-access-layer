package repository

import (
	"context"
	"embed"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/DTSL/go-bigtable-access-layer/mapping"
)

//go:embed testdata/mapping.json
var fs embed.FS

func TestRepository_Read(t *testing.T) {
	ctx := context.Background()
	c, err := fs.ReadFile("testdata/mapping.json")
	if err != nil {
		t.Fatalf("failed to read mapping.json: %v", err)
	}
	jsonMapping, err := mapping.LoadMapping(c)
	if err != nil {
		t.Fatalf("failed to load mapping: %v", err)
	}
	adapter := &adapter{
		ReadRow: mockReadRow,
	}
	repository := &Repository{
		adapter: adapter,
		mapper:  mapping.NewMapper(jsonMapping),
	}
	eventSet, err := repository.Read(ctx, "contact-3")
	if err != nil {
        t.Fatalf("failed to read: %v", err)
    }
	if len(eventSet.Events) != 1 {
		t.Fatalf("expected 1 event family, got %d", len(eventSet.Events))
	}
	t1 := time.Now().Add(-time.Duration(3) * time.Minute)
	if v, ok := eventSet.Events["front"]; ! ok {
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
		if v[0].Date.Unix() != t1.Unix() {
			t.Fatalf("expected %v, got %v", t1, v[0].Date)
		}
	}
}

func mockReadRow(_ context.Context, row string, _ ...bigtable.ReadOption) (bigtable.Row, error) {
	t1 := bigtable.Time(time.Now().Add(-time.Duration(3) * time.Minute))
	t2 := bigtable.Time(time.Now().Add(-time.Duration(2) * time.Minute))
	t3 := bigtable.Time(time.Now().Add(-time.Duration(1) * time.Minute))
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
