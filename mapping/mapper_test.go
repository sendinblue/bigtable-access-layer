package mapping

import (
	"fmt"
	"log"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
)

func TestSeekRaw(t *testing.T) {
	str := `{"raws": {"ui": "user_id"}}`
	mapping, err := LoadMapping([]byte(str))
	if err != nil {
		log.Println(err)
		t.Fatal("should not raise an error")
	}
	ok, col, val := seekRaw(mapping, "ui", "123")
	if !ok {
		t.Fatal("should have found the column")
	}
	if col != "user_id" {
		t.Fatal("column must be user_id")
	}
	if val != "123" {
		t.Fatal("value must be 123")
	}
	ok, col, val = seekRaw(mapping, "unk", "123")
	if ok {
		t.Fatal("should NOT have found the column")
	}
	if col != "" {
		t.Fatal(fmt.Sprintf("column must be empty, got %s \n", col))
	}
	if val != "" {
		t.Fatal(fmt.Sprintf("value must be empty, got %s \n", val))
	}
}

func TestSeekMapped(t *testing.T) {
	str := `{"mapped": {
    "oi": {
      "name": "is_opted_in",
      "values": {
        "0": "false",
        "1": "true"
      }
    }
  }}`
	mapping, err := LoadMapping([]byte(str))
	if err != nil {
		log.Println(err)
		t.Fatal("should not raise an error")
	}
	ok, col, val := seekMapped(mapping, "oi", "1")
	if !ok {
		t.Fatal("should have found the column")
	}
	if col != "is_opted_in" {
		t.Fatal("column must be user_id")
	}
	if val != "true" {
		t.Fatal("value must be true")
	}
	ok, col, val = seekRaw(mapping, "unk", "123")
	if ok {
		t.Fatal("should NOT have found the column")
	}
	if col != "" {
		t.Fatal(fmt.Sprintf("column must be empty, got %s \n", col))
	}
	if val != "" {
		t.Fatal(fmt.Sprintf("value must be empty, got %s \n", val))
	}
}

func TestReversedSeeker(t *testing.T) {
	str := `{"reversed": [{"name": "order_status","values": {"1": "pending_payment","2": "failed"}}]}`
	mapping, err := LoadMapping([]byte(str))
	if err != nil {
		log.Println(err)
		t.Fatal("should not raise an error")
	}
	seeker := newReverseSeeker()
	ok, c1, v1 := seeker.seekFromCache(mapping, "1", "")
	if ok {
		t.Fatal("`ok` should be false")
	}
	if c1 != "" {
		t.Fatal("c1` should be empty")
	}
	if v1 != "" {
		t.Fatal("v1` should be empty")
	}
	_, c2, v2 := seeker.seekFromMapping(mapping, "1", "")
	if c2 != "order_status" {
		t.Fatal("c2` should be 'order_status'")
	}
	if v2 != "pending_payment" {
		t.Fatal("v2` should be 'pending_payment'")
	}
	ok, c1, v1 = seeker.seekFromCache(mapping, "1", "")
	if !ok {
		t.Fatal("`ok` should be `true`")
	}
	if c1 != "order_status" {
		t.Fatal("c1` should be 'order_status'")
	}
	if v1 != "pending_payment" {
		t.Fatal("v1` should be 'pending_payment'")
	}
}

func TestMapper(t *testing.T) {
	mapping, err := LoadMappingFromFile("./testdata/mapping.json")
	if err != nil {
		log.Println(err)
		t.Fatal("should not raise an error")
	}
	mapper := NewMapper(mapping)
	compare(t, mapper, "ui", "1233", "user_id", "1233")
	compare(t, mapper, "oi", "1", "is_opted_in", "true")
	compare(t, mapper, "3", "1", "order_status", "processing")
}

func compare(t *testing.T, m *Mapper, col string, val string, wantedCol string, wantedVal string) {
	fCol, fVal := m.Seek(col, val)
	if fCol != wantedCol {
		t.Fatal(fmt.Sprintf("wrong column: wanted %s, got %s", wantedCol, fCol))
	}
	if fVal != wantedVal {
		t.Fatal(fmt.Sprintf("wrong value: wanted %s, got %s", wantedVal, fVal))
	}
}


func TestMapper_GetMappedEvents(t *testing.T) {
	t1 := time.Now().AddDate(0, 0, -3)
	t2 := time.Now().AddDate(0, 0, -1)
	bigtableItems := []bigtable.ReadItem{
		{
            Row:       "1",
            Column:    "ui",
            Timestamp: bigtable.Time(t1),
            Value:     []byte("42"),
        },
        {
            Row:       "1",
            Column:    "oi",
            Timestamp: bigtable.Time(t1),
            Value:     []byte("1"),
        },
        {
            Row:       "1",
            Column:    "1",
            Timestamp: bigtable.Time(t1),
            Value:     []byte("1"),
        },
		{
			Row:       "1",
			Column:    "ui",
			Timestamp: bigtable.Time(t2),
			Value:     []byte("42"),
		},
		{
			Row:       "1",
			Column:    "oi",
			Timestamp: bigtable.Time(t2),
			Value:     []byte("1"),
		},
		{
			Row:       "1",
			Column:    "4",
			Timestamp: bigtable.Time(t2),
			Value:     []byte("1"),
		},
	}

	mapping, err := LoadMappingFromFile("./testdata/mapping.json")
	if err != nil {
		log.Println(err)
		t.Fatal("should not raise an error")
	}
	mapper := NewMapper(mapping)
	cols, events := mapper.GetMappedEvents(bigtableItems, false)
	if len(cols) != 4 {
		t.Fatal("should have 3 columns")
	}
	if len(events) != 2 {
		t.Fatal("should have 2 events")
	}
	for _, event := range events {
		if v, ok := event.Cells["user_id"]; !ok && v != "42" {
			t.Fatal("user_id must be 42")
		}
		if v, ok := event.Cells["is_opted_in"]; !ok && v != "true" {
			t.Fatal("is_opted_in must be true")
		}
	}
	if v, ok := events[0].Cells["order_status"]; !ok && v != "pending_payment" {
		t.Fatal("order_status must be 'pending_payment' for 1st event")
	}
	if v, ok := events[0].Cells["order_status"]; !ok && v != "completed" {
		t.Fatal("order_status must be 'completed' for 2nd event")
	}
}
