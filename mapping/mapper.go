package mapping

// Mapper is in charge of translating data from Big Table into a human-readable format.
type Mapper struct {
	// mapping coming from the JSON file
	*Mapping
	// those functions are in charge of seeking data
	seekers []func(m *Mapping, column string, value string) (bool, string, string)
}

// Seek uses all `seekers` to find the appropriate mapping method and return the mapped column + value.
func (m *Mapper) Seek(column string, value string) (string, string) {
	for _, seek := range m.seekers {
		if ok, col, val := seek(m.Mapping, column, value); ok {
			return col, val
		}
	}
	return column, value
}

// seekRaw is a default seeker that simply translates the column's name if a match exists.
func seekRaw(m *Mapping, column string, value string) (bool, string, string) {
	r, ok := m.Raws[column]
	if ok {
		return true, r, value
	}
	return false, "", ""
}

// seekMapped checks if a map exists for the given column's short name and returns the full column from the mapping + the mapped value.
// example with this mapping:
// "mapped": {
//    "oi": {
//      "name": "is_opted_in",
//      "values": {
//        "0": "false",
//        "1": "true"
//      }
//    }
//  },
// this call would return "is_opted_in" and "true": seekMapped(m, "oi", "1")
func seekMapped(m *Mapping, column string, value string) (bool, string, string) {
	ma, ok := m.Mapped[column]
	if ok {
		v, ok := ma.Values[value]
		if ok {
			return true, ma.Name, v
		}
		return true, ma.Name, value
	}
	return false, "", ""
}
