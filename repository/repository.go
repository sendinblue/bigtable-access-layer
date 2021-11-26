// Package repository provides the Repository struct that is used to read data coming from Big Table  when a mapping is defined.
package repository

import (
	"context"

	"cloud.google.com/go/bigtable"
	"github.com/DTSL/go-bigtable-access-layer/data"
	"github.com/DTSL/go-bigtable-access-layer/mapping"
)

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
		maxRows: 100,
	}
	for _, opt := range opts {
		opt.apply(repo)
	}
	return repo
}

type Option interface {
	apply(r *Repository)
}

/*
Read a row from the repository and map it to a data.Set

This method takes a row key as an argument, uses its internal adapter to read the row from Big Table,
parses all cells contained in the row to turn it into a map of data.Event and finally returns the data.Set that contains all the events.
*/
func (r *Repository) Read(ctx context.Context, key string) (*data.Set, error) {
	row, err := r.adapter.ReadRow(ctx, key)
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
			set.Events[family] = events
			set.Columns = merge(set.Columns, cols)
		}
	}
	return set
}

// Search for rows in the repository that match the given filter and return the according data.Set
func (r *Repository) Search(ctx context.Context, filter bigtable.Filter) (*data.Set, error) {
	rows, err := r.search(ctx, filter)
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

func (r *Repository) search(ctx context.Context, filter bigtable.Filter) ([]bigtable.Row, error) {
	var rows []bigtable.Row
	err := r.adapter.ReadRows(ctx, bigtable.RowRange{}, func(row bigtable.Row) bool {
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
