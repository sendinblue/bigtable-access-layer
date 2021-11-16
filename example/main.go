package main

import (
	"context"
	"embed"
	"encoding/json"
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
	err = printTwoEvents(ctx, client, os.Stdout, tableID)
	if err != nil {
		log.Fatalf("impossible to print the events table: %v\n", err)
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
	jsonOutput := make(map[string]interface{})
	for _, items := range row {
		_, events := mapper.GetMappedEvents(items, false)
		cnt := aggregation.NewCount("count")
		grpDeviceEvent := aggregation.GroupByAggregate(events, cnt.Compute, "device_type", "event_type")
		for name, result := range grpDeviceEvent {
			jsonOutput[name] = result.Cells["count"]
		}
		grpEvent := aggregation.GroupByAggregate(events, cnt.Compute, "event_type")
		for name, result := range grpEvent {
			jsonOutput[name] = result.Cells["count"]
		}
	}
	j, err := json.Marshal(jsonOutput)
	if err != nil {
		_,_ = fmt.Fprintf(out, "error while formatting JSON: %v. Delivering the raw content instead:\n %+v \n", err, jsonOutput)
	}
	_,_ = fmt.Fprintf(out, "%s\n", j)
	return nil
}

func printTwoEvents(ctx context.Context, client *bigtable.Client, out io.Writer, table string) error {
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
	jsonOutput := make(map[string]interface{})
	for fam, items := range row {
		jsonOutput["family"] = fam
		columns, events := mapper.GetMappedEvents(items, false)
		jsonOutput["columns"] = columns
		events = events[:2]
		jsonOutput["events"] = events
	}
	j, err := json.Marshal(jsonOutput)
	if err != nil {
        _,_ = fmt.Fprintf(out, "error while formatting JSON: %v. Delivering the raw content instead:\n %+v \n", err, jsonOutput)
    }
	_,_ = fmt.Fprintf(out, "%s\n", j)
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
	return nil
}

func generateMutations(numEvents int) []*bigtable.Mutation {
	var data []*bigtable.Mutation
	for i := 0; i < numEvents; i++ {
		mod := i % 20
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
		mut.Set("front", "d", t, []byte(fmt.Sprintf("%d", 1+(i%2))))
		data = append(data, mut)
	}
	return data
}
