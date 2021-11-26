package repository

import (
	"context"
	"fmt"
	"io"
	"time"

	"cloud.google.com/go/bigtable"
)

type DebugAdapter struct {
	writer  io.Writer
	adapter Adapter
}

type DebugAdapterOption struct {
	writer io.Writer
}

func NewDebugAdapterOption(w io.Writer) *DebugAdapterOption {
	return &DebugAdapterOption{
		writer: w,
	}
}

func (opt *DebugAdapterOption) apply(repo *Repository) {
	a := DebugAdapter{
		writer:  opt.writer,
		adapter: repo.adapter,
	}
	repo.adapter = a
}

func (a DebugAdapter) ReadRow(ctx context.Context, row string, opts ...bigtable.ReadOption) (bigtable.Row, error) {
	_, _ = fmt.Fprintf(a.writer, "%s: ReadRow(%s, %+v)\n", time.Now().UTC().String(), row, opts)
	start := time.Now()
	btRow, err := a.adapter.ReadRow(ctx, row, opts...)
	end := time.Now()
	_, _ = fmt.Fprintf(a.writer, "%s: ReadRow(%s): %d row in %s, error is %v\n", time.Now().UTC().String(), row, len(btRow), end.Sub(start), err)
	return btRow, err
}

func (a DebugAdapter) ReadRows(ctx context.Context, arg bigtable.RowSet, f func(bigtable.Row) bool, opts ...bigtable.ReadOption) (err error) {
	_, _ = fmt.Fprintf(a.writer, "%s: ReadRows(%+v)\n", time.Now().UTC().String(), opts)
	start := time.Now()
	err = a.adapter.ReadRows(ctx, arg, f, opts...)
	end := time.Now()
	_, _ = fmt.Fprintf(a.writer, "%s: ReadRows(): %s, error is %v\n", time.Now().UTC().String(), end.Sub(start), err)
	return err
}

func (a DebugAdapter) ApplyBulk(ctx context.Context, rowKeys []string, muts []*bigtable.Mutation, opts ...bigtable.ApplyOption) (errs []error, err error) {
	_, _ = fmt.Fprintf(a.writer, "%s: ApplyBulk(%+v) with %v mutations\n", time.Now().UTC().String(), rowKeys, len(muts))
	start := time.Now()
	errs, err = a.adapter.ApplyBulk(ctx, rowKeys, muts, opts...)
	end := time.Now()
	_, _ = fmt.Fprintf(a.writer, "%s: ApplyBulk(): %s, errored items: %v ,error is %v\n", time.Now().UTC().String(), end.Sub(start), len(errs), err)
	return errs, err
}
