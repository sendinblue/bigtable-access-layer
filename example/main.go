package main

import (
	"context"
	"embed"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/DTSL/go-bigtable-access-layer/aggregation"
	"github.com/DTSL/go-bigtable-access-layer/mapping"
)

const (
	projectID  = "example-project"
	instanceID = "example-instance"
	tableID    = "ecommerce_events"

	urlPattern = "https://www.example.com/products/%d"
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
	err = fillTable(ctx, client, tableID)
	if err != nil {
		log.Fatalf("impossible to fill the events table: %v\n", err)
	}
	err = aggregate(ctx, client, os.Stdout, tableID)
	if err != nil {
		log.Fatalf("impossible to aggregate the events table: %v\n", err)
	}
}

//go:embed mapping.json
var fs embed.FS

func aggregate(ctx context.Context, client *bigtable.Client, out io.Writer, table string) error {
	tbl := client.Open(table)
	row, err := tbl.ReadRow(ctx, "contact-3")
	if err != nil {
		return err
	}
	c, err := fs.ReadFile("mapping.json")
	if err != nil {
		return err
	}
	jsonMapping, err := mapping.LoadMapping(c)
	if err != nil {
		return err
	}
	mapper := mapping.NewMapper(jsonMapping)
	for fam, items := range row {
		_,_ = fmt.Fprintf(out, "family: %s\n", fam)
		columns, events := mapper.GetMappedEvents(items, false)
		_,_ = fmt.Fprintf(out, "mapped columns: %+v\n", columns)
		cnt := aggregation.NewCount("count")
		grpDeviceEvent := aggregation.GroupByAggregate(events, cnt.Compute, "device_type", "event_type")
		for _, result := range grpDeviceEvent {
            _,_ = fmt.Fprintf(out, "device: %v, event: %v , count: %v\n", result.Cells["device_type"], result.Cells["event_type"], result.Cells["count"])
        }
		grpEvent := aggregation.GroupByAggregate(events, cnt.Compute, "event_type")
		for _, result := range grpEvent {
			_,_ = fmt.Fprintf(out, "event: %v , count: %v\n", result.Cells["event_type"], result.Cells["count"])
		}
	}
	return nil
}

func fillTable(ctx context.Context, client *bigtable.Client, table string) error {
    tbl := client.Open(table)
	numContacts := 10
	for i := 0; i < numContacts; i++ {
		row := fmt.Sprintf("contact-%d", i+1)
		mutations := generateMutations(100)
		for _, m := range mutations {
            if err := tbl.Apply(ctx, row, m); err != nil {
                return err
            }
        }
	}

    mut := bigtable.NewMutation()
    mut.Set("front", "col", bigtable.Now(), []byte("value 3"))
    err := tbl.Apply(ctx, "row", mut)
    if err != nil {
        return err
    }
    return nil
}

func generateMutations(numEvents int) []*bigtable.Mutation {
	var data []*bigtable.Mutation
    for i := 0; i < numEvents; i++ {
		mod := i % 11
		mut := bigtable.NewMutation()
		t := bigtable.Time(time.Now().Add(-time.Duration(i) * time.Minute))
		mut.Set("front", "u", t, []byte(fmt.Sprintf(urlPattern, mod)))
		switch mod {
		case 1, 2:
			mut.Set("front", "2", t, []byte("1"))
		case 3:
            mut.Set("front", "3", t, []byte("1"))
		default:
			mut.Set("front", "1", t, []byte("1"))
		}
		mut.Set("front", "d", t, []byte(fmt.Sprintf("%d", 1 + (i % 2))))
		data = append(data, mut)
    }
    return data
}
