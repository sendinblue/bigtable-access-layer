# go-bigtable-access-layer

[![Maintainability](https://api.codeclimate.com/v1/badges/e06e7c0df20da7a298fc/maintainability)](https://codeclimate.com/repos/619e604df3947401b701334c/maintainability)
[![Test Coverage](https://api.codeclimate.com/v1/badges/e06e7c0df20da7a298fc/test_coverage)](https://codeclimate.com/repos/619e604df3947401b701334c/test_coverage)


`go-bigtable-access-layer` is a library designed to ease reading data from Big Table. it features:

- cells grouping to build events using the timestamp
- schema mapping to transform the data into a human-readable format
- aggregation functions like `count()`, `min()`, `max()`, `sum()`

## Use case

This library fits fine when you want to store time series data in Big Table like:

- user-generated events we can capture on a website ('page view', 'click', 'purchase', etc.)
- series of measures from weather sensors ('temperature', 'humidity', 'wind speed', etc.)

In those use-cases, each row will be a logical set of events, with its row key built in a way it can be easily identified and will contain a manageable number of events. For instance, a row key could include the region of the weather station, the year and the week number separated with `#` to look like `europe-west1#2021#week1`. Each event is a set of cells sharing the same timestamp, so when the access-layer turns a row into a set of events, it groups cells by timestamp to end with one event / timestamp. Here's an example from Google's documentation: https://cloud.google.com/bigtable/docs/schema-design-time-series#time-buckets

## Mapping system

Big Table treats column qualifiers as data not metadata, meaning that each character in a column qualifier counts. So the longer a column qualifier is, the more it will use space. As a consequence, Google recommends using the column qualifier as data or if it's not possible, to use short but meaningful column names. It will save space and reduce amount of transferred data.

The mapping system is here to turn short column names into human-readable equivalent. It can also be used when the column qualifier contains data, granted it is an "enum" as defined in the mapping.

here's an example of a mapping:

```json
{
  "raws": {
    "ui": "user_id"
  },
  "mapped": {
    "oi": {
      "name": "is_opted_in",
      "values": {
        "0": "false",
        "1": "true"
      }
    }
  },
  "reversed": [
    {
      "name": "order_status",
      "values": {
        "1": "pending_payment",
        "2": "failed",
        "3": "processing",
        "4": "completed",
        "5": "on_hold",
        "6": "canceled",
        "7": "refunded"
      }
    }
  ]
}
```

Here, each column is an example of a mapping method supported by the library:

- `raws` contains columns for which the short qualifier will be replaced by the long version. Here, "ui" will be replaced by "user_id".
- `mapped` contains columns for which the short qualifier will be replaced by the long version (`name` property) and the value will be replaced by the mapped value. Here, "oi" will be replaced by "is_opted_in" and the value will be replaced by "true" or "false".
- `reversed` contains columns for which the short qualifier will be used as the value and the `name` property will be used for the column qualifier. Here, a column named "1" will result to `order_status=pending_payment`.

### Usage

In the example below, the `main` function contains only the "basic stuff" like creating the context and the client, and then calling the `fetchRow` function. It's where we read a row and then use the mapping to turn it into a set of events.

```go
package main

import (
	"context"
	"embed"
	"fmt"
	"io"
	"log"
	"os"

	"cloud.google.com/go/bigtable"
	"github.com/DTSL/go-bigtable-access-layer/aggregation"
	"github.com/DTSL/go-bigtable-access-layer/mapping"
)

const (
	projectID  = "platform-staging-200307"
	instanceID = "sib-stg-bigtable-events-01"
	tableID    = "sib_identified_events"
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
	err = fetchRow(ctx, client, os.Stdout, getRowKey())
	if err != nil {
		log.Fatalf("impossible to fetch a single row: %v\n", err)
	}
}

//go:embed events.json
var fs embed.FS

// here is the real stuff: we fetch a single row from Big Table and ues the mapping system to transform it into a human-readable format
func fetchRow(ctx context.Context, client *bigtable.Client, out io.Writer, id string) error {
	table := client.Open(tableID)
	row, err := table.ReadRow(ctx, id)
	if err != nil {
		return err
	}
	c, err := fs.ReadFile("events.json")
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
		for _, event := range events {
			_,_ = fmt.Fprintf(out, "event: %+v\n", event)
		}
	}
	return nil
}
```

Please see the code in the ['example folder](.example) for a complete example that you can run on your local machine using the emulator provided with Google Cloud SDK.
