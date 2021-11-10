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
