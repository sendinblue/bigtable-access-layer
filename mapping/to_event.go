package mapping

import "sync"

// turnToShortColumn is a default seeker that simply translates the column's name if a match exists.
func seekFromShortColumn(m *Mapping, column string, value string) (bool, string, string) {
	r, ok := m.Raws[column]
	if ok {
		return true, r, value
	}
	return false, "", ""
}

// turnToMappedColumn checks if a map exists for the given column's short name and returns the full column from the mapping + the mapped value.
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
// this call would return "is_opted_in" and "true": turnToMappedColumn(m, "oi", "1")
func seekFromMappedColumn(m *Mapping, column string, value string) (bool, string, string) {
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

// cacheEntry is a cache entry for the reverse Seeker.
type cacheEntry struct {
	col   string
	value string
}

// reverseTransformer is to use for data stored in a "reversed way" meaning that the column qualifier is the actual data and is a kind of enum value
type reverseSeeker struct {
	cache sync.Map
}

func newReverseSeeker() *reverseSeeker {
	return &reverseSeeker{}
}

// seekFromCache returns the column and value from the cache if it exists.
func (c *reverseSeeker) seekFromCache(_ *Mapping, column string, _ string) (bool, string, string) {
	if entry, ok := c.cache.Load(column); ok {
		return true, entry.(cacheEntry).col, entry.(cacheEntry).value
	}
	return false, "", ""
}

// seekFromMapping returns the column and value from the mapping.
func (c *reverseSeeker) seekFromMapping(m *Mapping, column string, _ string) (bool, string, string) {
	for _, ma := range m.Reversed {
		for short, val := range ma.Values {
			if column == short {
				c.cache.Store(column, cacheEntry{col: ma.Name, value: val})
				return true, ma.Name, val
			}
		}
	}
	return false, "", ""
}
