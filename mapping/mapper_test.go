package mapping

import (
	"fmt"
	"log"
	"testing"
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
