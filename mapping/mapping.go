/*
Package mapping provides the API to convert data coming from Big Table into a data.Set.
*/
/*
The mapping system is based on a set of rules described inside a JSON mapping file, here's an example:
	{
	  "raws": {
		"ui": "user_id"
	  },
	  "mapped": {
		"oi": {
		  "name": "is_opted_in",
		  "values": {
			"0": "false",
			"1": "true"
		  }
		}
	  },
	  "reversed": [
		{
		  "name": "order_status",
		  "values": {
			"1": "pending_payment",
			"2": "failed",
			"3": "processing",
			"4": "completed",
			"5": "on_hold",
			"6": "canceled",
			"7": "refunded"
		  }
		}
	  ]
	}
The mapping has 3 sections:
  - raws: only the column qualifier is translated from its short version to a meaningful name.
  - mapped: the column qualifier is translated from its short version to a meaningful name, and the value is translated from its short-value to the full value.
  - reversed: the column qualifier contains the real data which is mapped to a value as described in the "values" property and the column name is taken from the "name" attribute.

Considering the mapping above, let's say we have a row with the following data coming from Big Table:
    {
      "ui": "12345",
      "oi": "1",
      "3": "1"
    }
The mapping system will map it to a new data.Event containing the following data:
    {
      "user_id": "12345",
      "is_opted_in": "true",
      "order_status": "processing"
    }
*/
package mapping

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

// Mapping describes the mapping between data stored in Big Table and its human-readable representation.
type Mapping struct {
	// columns that are taken as there are in Big Table
	Raws map[string]string `json:"raws"`
	// columns that needs to be mapped to a string value
	Mapped map[string]Map `json:"mapped"`
	// columns featuring a reversed mapping, meaning that the column qualifier is the data
	Reversed []Map `json:"reversed"`
}

// Map is used to map a column to a set of string values.
type Map struct {
	Name   string            `json:"name"`
	Values map[string]string `json:"values"`
}

// LoadMapping loads a mapping from a slice of bytes.
// You can use this function if you prefer to open the mapping file yourself.
func LoadMapping(c []byte) (*Mapping, error) {
	m := &Mapping{}
	err := json.Unmarshal(c, &m)
	if err != nil {
		return nil, err
	}
	return m, nil
}

// LoadMappingIO loads a mapping from a IO reader.
func LoadMappingIO(reader io.ReadCloser) (*Mapping, error) {
	m := &Mapping{}
	decoder := json.NewDecoder(reader)
	err := decoder.Decode(&m)
	if err != nil {
		return nil, errors.Wrap(err, "decode mapping")
	}
	return m, nil
}

// LoadMappingFromFile loads a mapping from a file.
func LoadMappingFromFile(path string) (*Mapping, error) {
	c, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	return LoadMapping(c)
}
