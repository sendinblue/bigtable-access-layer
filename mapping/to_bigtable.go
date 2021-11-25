package mapping

func turnToShortColumn(m *Mapping, column string, value string) (bool, string, string) {
	for short, full := range m.Raws {
		if full == column {
			return true, short, value
		}
	}
	return false, "", ""
}

func turnToMappedColumnValue(m *Mapping, column string, value string) (bool, string, string) {
	for short, rule := range m.Mapped {
		if rule.Name == column {
			for shortValue, mappedValue := range rule.Values {
				if mappedValue == value {
					return true, short, shortValue
				}
			}
		}
	}
	return false, "", ""
}

func turnToReversedColumnValue(m *Mapping, column string, value string) (bool, string, string) {
	for _, reversed := range m.Reversed {
		if reversed.Name == column {
			for short, val := range reversed.Values {
				if val == value {
					return true, short, "1"
				}
			}
		}
	}
	return false, "", ""
}
