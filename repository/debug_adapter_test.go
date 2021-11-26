package repository

import (
	"context"
	"log"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/DTSL/go-bigtable-access-layer/data"
	"github.com/DTSL/go-bigtable-access-layer/mapping"
)

func TestDebugAdapter(t *testing.T) {
	ctx := context.Background()
	client := getBigTableClient(ctx)
	c, err := fs.ReadFile("testdata/mapping.json")
	if err != nil {
		log.Fatalln(err)
	}
	jsonMapping, err := mapping.LoadMapping(c)
	if err != nil {
		log.Fatalln(err)
	}
	mapper := mapping.NewMapper(jsonMapping)
	tbl := client.Open(table)

	dbgWriter := debugWriter{}
	dbgAdapterOpt := NewDebugAdapterOption(&dbgWriter)
	repo := NewRepository(tbl, mapper, dbgAdapterOpt)

	_, err = repo.Read(ctx, "contact-3")
	if err != nil {
		log.Fatalln(err)
	}
	if len(dbgWriter.lines) != 2 {
		t.Errorf("expected 2 lines, got %d", len(dbgWriter.lines))
	}
	_, err = repo.search(ctx, bigtable.LatestNFilter(1))
	if err != nil {
		log.Fatalln(err)
	}
	if len(dbgWriter.lines) != 4 {
		t.Errorf("expected 4 line, got %d", len(dbgWriter.lines))
	}
	eventSet := data.Set{Events: map[string][]*data.Event{
		"front": {
			{
				RowKey: "event-1",
				Date:   time.Now(),
				Cells: map[string]string{
					"device_type": "Smartphone",
				},
			},
		},
	}}
	errs, err := repo.Write(ctx, &eventSet)
	log.Println(errs)
	if err != nil {
		log.Fatalln(err)
	}
	if len(dbgWriter.lines) != 6 {
		t.Errorf("expected 6 line, got %d", len(dbgWriter.lines))
	}
}

type debugWriter struct {
	lines [][]byte
}

func (w *debugWriter) Write(p []byte) (n int, err error) {
	w.lines = append(w.lines, p)
	return len(p), nil
}
