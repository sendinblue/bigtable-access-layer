package mapping

import (
	"log"
	"testing"
)

func TestLoadMapping(t *testing.T) {
	str := "some malformed JSON"
	_, err := LoadMapping([]byte(str))
	if err == nil {
		t.Fatal("no error")
	}
	str = `{"raws": {"cai": "campaign_id"}}`
	mapping, err := LoadMapping([]byte(str))
	if err != nil {
		log.Println(err)
		t.Fatal("should not raise an error")
	}
	if len(mapping.Raws) != 1 {
		t.Fatal("wrong number of raws")
	}
}

func TestLoadMappingFromFile(t *testing.T) {
	mapping, err := LoadMappingFromFile("./testdata/mapping.json")
	if err != nil {
		log.Println(err)
		t.Fatal("should not raise an error")
	}
	if len(mapping.Raws) != 1 {
        t.Fatal("wrong number of raws")
    }
}
