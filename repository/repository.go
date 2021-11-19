package repository

import (
	"context"

	"cloud.google.com/go/bigtable"
	"github.com/DTSL/go-bigtable-access-layer/data"
	"github.com/DTSL/go-bigtable-access-layer/mapping"
)

type Repository struct {
	adapter *adapter
	mapper  *mapping.Mapper
}

func NewRepository(table *bigtable.Table, mapper *mapping.Mapper) *Repository {
	return &Repository{
		adapter: &adapter{
			ReadRow: table.ReadRow,
		},
		mapper:  mapper,
	}
}

// Read a row from the repository and map it to a data.Set
func (r *Repository) Read(ctx context.Context, key string) (*data.Set, error) {
	row, err := r.adapter.ReadRow(ctx, key)
	if err != nil {
		return nil, err
	}
	set := &data.Set{
		Events:  make(map[string][]*data.Event),
		Columns: make([]string, 0),
	}
	for family, readItem := range row {
		cols, events := r.mapper.GetMappedEvents(readItem, false)
		set.Events[family] = events
		set.Columns = merge(set.Columns, cols)
	}
	return set, nil
}

// adapter acts as a proxy between the repository and the actual data source.
// It always to easily mock the data source in tests.
type adapter struct {
	ReadRow func(ctx context.Context, row string, opts ...bigtable.ReadOption) (bigtable.Row, error)
}

// merge returns a new slice with the contents of both slices.
func merge(a, b []string) []string {
	m := make(map[string]bool)
	for _, x := range a {
		m[x] = true
	}
	for _, x := range b {
		m[x] = true
	}
	var res []string
	for x := range m {
		res = append(res, x)
	}
	return res
}
