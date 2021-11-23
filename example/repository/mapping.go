package main

import (
	"context"
	"embed"
	"fmt"
	"io"
	"log"
	"os"

	"cloud.google.com/go/bigtable"
	"github.com/DTSL/go-bigtable-access-layer/mapping"
	"github.com/DTSL/go-bigtable-access-layer/repository"
)

const (
	projectID  = "example-project"
	instanceID = "example-instance"
	tableID    = "ecommerce_events"
)

func main() {
	ctx := context.Background()
	client, err := bigtable.NewClient(ctx, projectID, instanceID)
	if err != nil {
		log.Fatalf("impossible to connect to big table: %v\n", err)
	}
	defer func() {
		_ = client.Close()
	}()
	err = read(ctx, client, os.Stdout, tableID)
	if err != nil {
		log.Fatalf("impossible to read the events table: %v\n", err)
	}
}

//go:embed mapping.json
var fs embed.FS

func read(ctx context.Context, client *bigtable.Client, out io.Writer, table string) error {
	c, err := fs.ReadFile("mapping.json")
	if err != nil {
		return err
	}
	jsonMapping, err := mapping.LoadMapping(c)
	if err != nil {
		return err
	}
	mapper := mapping.NewMapper(jsonMapping)
	repo := repository.NewRepository(client.Open(table), mapper)
	eventSet, err := repo.Read(ctx, "contact-3")
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "columns: %+v\n", eventSet.Columns)
	for fam, events := range eventSet.Events {
		fmt.Fprintf(out, "family: %s\n", fam)
		for _, event := range events {
			fmt.Fprintf(out, "  row key: %s\n", event.RowKey)
			fmt.Fprintf(out, "  event date: %s\n", event.Date)
            fmt.Fprintf(out, "  event cells: %+v\n", event.Cells)
        }
	}
	return nil
}
