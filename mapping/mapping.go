package mapping

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// Mapping is used to turn data extracted from Big Table into a human-readable output.
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

// LoadMappingFromFile loads a mapping from a file.
func LoadMappingFromFile(path string) (*Mapping, error) {
	c, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	return LoadMapping(c)
}
