// Package repository provides the Repository struct that is used to read data coming from Big Table  when a mapping is defined.
package repository

import (
	"context"

	"cloud.google.com/go/bigtable"
	"github.com/sendinblue/bigtable-access-layer/data"
	"github.com/sendinblue/bigtable-access-layer/mapping"
)

const defaultMaxRows = 100

type Repository struct {
	adapter Adapter
	mapper  *mapping.Mapper
	maxRows int
}

// NewRepository creates a new Repository for the given table.
func NewRepository(table *bigtable.Table, mapper *mapping.Mapper, opts ...Option) *Repository {
	adapter := &bigTableAdapter{
		table: table,
	}
	repo := &Repository{
		adapter: adapter,
		mapper:  mapper,
		maxRows: defaultMaxRows,
	}
	for _, opt := range opts {
		opt.apply(repo)
	}
	return repo
}

type Option interface {
	apply(r *Repository)
}

type MaxRowsOption struct {
	maxRows int
}

func (o MaxRowsOption) apply(r *Repository) {
	r.maxRows = o.maxRows
}

/*
Read a row from the repository and map it to a data.Set

This method takes a row key as an argument, uses its internal adapter to read the row from Big Table,
parses all cells contained in the row to turn it into a map of data.Event and finally returns the data.Set that contains all the events.
*/
func (r *Repository) Read(ctx context.Context, key string) (*data.Set, error) {
	return r.read(ctx, key)
}

/*
ReadFamily reads a row from the repository keeping only the desired column family and map it to a data.Set

This method takes a row key and the column family as an argument, uses its internal adapter to read the row from Big Table,
parses all cells contained in the row to turn it into a map of data.Event and finally returns the data.Set that contains all the events.

Be careful, this method will perform an exact match on the column family name.
*/
func (r *Repository) ReadFamily(ctx context.Context, key string, family string) (*data.Set, error) {
	familyFilter := bigtable.RowFilter(bigtable.FamilyFilter(family))
	return r.read(ctx, key, familyFilter)
}

// ReadLast reads a row from the repository while returning only the latest cell values after
// mapping it to a data.Set. This method takes a row key as an argument, uses its internal adapter
// to read the row from Big Table, parses only the latest cells contained in the row to turn it into
// a map of data.Event and finally returns the data.Set that contains all the events.
func (r *Repository) ReadLast(ctx context.Context, key string) (*data.Set, error) {
	return r.read(ctx, key, bigtable.RowFilter(bigtable.LatestNFilter(1)))
}

// ReadRow reads a row from the repository while returning the cell values after
// mapping it to a data.Set. This method takes a row key as an argument, uses its internal adapter
// to read the row from Big Table, parses only the cells contained in the row to turn it into
// a map of data.Event and finally returns the data.Set that contains all the events.
func (r *Repository) ReadRow(ctx context.Context, key string, opts ...bigtable.ReadOption) (*data.Set, error) {
	row, err := r.adapter.ReadRow(ctx, key, opts...)
	if err != nil {
		return nil, err
	}
	return buildEventSet([]bigtable.Row{row}, r.mapper), nil
}

func (r *Repository) read(ctx context.Context, key string, opts ...bigtable.ReadOption) (*data.Set, error) {
	row, err := r.adapter.ReadRow(ctx, key, opts...)
	if err != nil {
		return nil, err
	}
	return buildEventSet([]bigtable.Row{row}, r.mapper), nil
}

func buildEventSet(rows []bigtable.Row, mapper *mapping.Mapper) *data.Set {
	set := &data.Set{
		Events:  make(map[string][]*data.Event),
		Columns: make([]string, 0),
	}
	for _, row := range rows {
		for family, readItem := range row {
			cols, events := mapper.GetMappedEvents(readItem)
			set.Events[family] = append(set.Events[family], events...)
			set.Columns = merge(set.Columns, cols)
		}
	}
	return set
}

// Search for rows in the repository that match the given filter and return the according data.Set
func (r *Repository) Search(ctx context.Context, rowSet bigtable.RowSet, filter bigtable.Filter) (*data.Set, error) {
	rows, err := r.search(ctx, rowSet, filter)
	if err != nil {
		return nil, err
	}
	resultMap := mapResult(rows, r.maxRows)
	result := make([]bigtable.Row, 0, len(resultMap))
	for key, row := range resultMap {
		fullRow, err := r.adapter.ReadRow(ctx, key)
		if err != nil {
			return nil, err
		}
		result = append(result, filterReadItems(fullRow, row))
	}
	return buildEventSet(result, r.mapper), nil
}

func (r *Repository) Write(ctx context.Context, eventSet *data.Set) ([]error, error) {
	allMutations := r.mapper.GetMutations(eventSet)
	rowKeys := make([]string, 0, len(allMutations))
	mutations := make([]*bigtable.Mutation, 0, len(allMutations))
	for key := range allMutations {
		rowKeys = append(rowKeys, key)
		mutations = append(mutations, allMutations[key])
	}
	return r.adapter.ApplyBulk(ctx, rowKeys, mutations)
}

func (r *Repository) search(ctx context.Context, rowSet bigtable.RowSet, filter bigtable.Filter) ([]bigtable.Row, error) {
	var rows []bigtable.Row
	err := r.adapter.ReadRows(ctx, rowSet, func(row bigtable.Row) bool {
		rows = append(rows, row)
		return true
	}, bigtable.RowFilter(filter))
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func mapResult(rows []bigtable.Row, limit int) map[string][]bigtable.Timestamp {
	resultMap := make(map[string][]bigtable.Timestamp)
	for i, row := range rows {
		for _, items := range row {
			resultMap[row.Key()] = make([]bigtable.Timestamp, 0)
			for _, item := range items {
				resultMap[row.Key()] = append(resultMap[row.Key()], item.Timestamp)
			}
		}
		if i > limit {
			return resultMap
		}
	}
	return resultMap
}

func filterReadItems(row bigtable.Row, ts []bigtable.Timestamp) map[string][]bigtable.ReadItem {
	result := make(map[string][]bigtable.ReadItem)
	timestamps := make(map[bigtable.Timestamp]bool, len(ts))
	for _, t := range ts {
		timestamps[t] = true
	}
	for fam, items := range row {
		if result[fam] == nil {
			result[fam] = make([]bigtable.ReadItem, 0)
		}
		for _, item := range items {
			if _, ok := timestamps[item.Timestamp]; ok {
				result[fam] = append(result[fam], item)
			}
		}
	}
	return result
}

// Adapter acts as a proxy between the repository and the actual data source.
// It allows to easily mock the data source in tests.
type Adapter interface {
	ReadRow(ctx context.Context, row string, opts ...bigtable.ReadOption) (bigtable.Row, error)
	ReadRows(ctx context.Context, arg bigtable.RowSet, f func(bigtable.Row) bool, opts ...bigtable.ReadOption) (err error)
	ApplyBulk(ctx context.Context, rowKeys []string, muts []*bigtable.Mutation, opts ...bigtable.ApplyOption) (errs []error, err error)
}

type bigTableAdapter struct {
	table *bigtable.Table
}

func (a *bigTableAdapter) ReadRow(ctx context.Context, row string, opts ...bigtable.ReadOption) (bigtable.Row, error) {
	return a.table.ReadRow(ctx, row, opts...)
}

func (a *bigTableAdapter) ReadRows(ctx context.Context, arg bigtable.RowSet, f func(bigtable.Row) bool, opts ...bigtable.ReadOption) (err error) {
	return a.table.ReadRows(ctx, arg, f, opts...)
}

func (a *bigTableAdapter) ApplyBulk(ctx context.Context, rowKeys []string, muts []*bigtable.Mutation, opts ...bigtable.ApplyOption) (errs []error, err error) {
	return a.table.ApplyBulk(ctx, rowKeys, muts, opts...)
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
