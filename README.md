# go-bigtable-access-layer

[![Maintainability](https://api.codeclimate.com/v1/badges/e06e7c0df20da7a298fc/maintainability)](https://codeclimate.com/repos/619e604df3947401b701334c/maintainability)
[![Test Coverage](https://api.codeclimate.com/v1/badges/e06e7c0df20da7a298fc/test_coverage)](https://codeclimate.com/repos/619e604df3947401b701334c/test_coverage)


`go-bigtable-access-layer` is a library designed to ease reading data from Big Table. it features:

- cells grouping to build events using the timestamp
- schema mapping to transform the data into a human-readable format
- aggregation functions like `count()`, `min()`, `max()`, `sum()`

## Quick presentation of Big Table

The description above is a summary of Big Table's design and is meant to help you understand it quickly. Please refer to the official documentation provided by Google for further reference https://cloud.google.com/bigtable/docs/

Big Table is a No-SQL database, it stores **rows** in **tables**, with sets of **columns** inside **column families**.

A table **must** have at least one column family and can have multiple ones. Each column family has its own expiration policy.

A row is a set of cells each cell being made of a timestamp, a column qualifier and a value. The column qualifier is the combination of the column family and the column name, separated by a `:`. For instance, the column named "temperature" in the column family "weather" will have the column qualifier "weather:temperature".

Each row has a unique row key, which is a string made of whatever needed to identify the row.

## Motivations and purpose of the present library

Big Table is really efficient when used to store time series data. The trade-off is that it treats all column qualifiers as data. This means that the longer a column name is, the more it will take space on storage and the more it will have a hit on performances. It will also have an impact on the financial aspect: a higher space usage comes with a higher cost, so there are actually two main reasons to optimize the storage of data in Big Table:

- performance
- costs

That's why `bigtable-access-layer` offers a mapping system, it will allow you to reduce space usage without losing the functional meaning of the data. The library also provides functions to aggregate data like it's possible to do in the SQL world with functions like `count()`, `min()`, `max()`, `sum()` and the `GROUP BY` clause.

## Use case

This library fits fine when you want to store time series data in Big Table like:

- user-generated events we can capture on a website ('page view', 'click', 'purchase', etc.)
- series of measures from weather sensors ('temperature', 'humidity', 'wind speed', etc.)

In those use-cases, each row will be a logical set of events, with its row key built in a way it can be easily identified and will contain a manageable number of events. For instance, a row key could include the region of the weather station, the year and the week number separated with `#` to look like `europe-west1#2021#week1`. Each event is a set of cells sharing the same timestamp, so when the access-layer turns a row into a set of events, it groups cells by timestamp to end with one event / timestamp. Here's an example from Google's documentation: https://cloud.google.com/bigtable/docs/schema-design-time-series#time-buckets

## Mapping system

Big Table treats column qualifiers as data, not metadata, meaning that each character in a column qualifier counts. So the longer a column qualifier is, the more it will use space. As a consequence, Google recommends using the column qualifier as data or if it's not possible, to use short but meaningful column names. It will save space and reduce amount of transferred data.

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

In the example below we read a row through the repository to get a set of events.

```go
repo := repository.NewRepository(client.Open(tableID), mapper)
dataSet, err := repo.Read(ctx, rowKey)
if err != nil {
    log.Fatalf("impossible to read the data: %v\n", err)
}
for _, familyEvents := range dataSet.Events {
    for _, event := range familyEvents {
        fmt.Fprintf(out, "%s -  %s: %+v\n", event.RowKey, event.Date, event.Cells)
    }
}
```

## Aggregation

It is possible to aggregate data from multiple events into a single one. It can be seen as the equivalent of a SQL `GROUP BY` clause like in the example below:

```SQL
SELECT event_type, device_type, COUNT(*) as `count`, SUM(amount) AS total FROM events GROUP BY event_type, device_type;
```

Using the library, we can do the same on client side:

```go
// we first fetch the row
repo := repository.NewRepository(cl.Open(tableID), mapper)
set, err := repo.Read(ctx, rowKey)
if err != nil {
    log.Fatalf("impossible to read the data: %v\n", err)
}
cnt := aggregation.NewCount("count")
total := aggregation.NewSum("amount", "total")
aggregationSet := aggregation.NewAggregationSet()
aggregationSet.Add(cnt.Compute)
aggregationSet.Add(total.Compute)
// like in the SQL world, we'll find fields from the last line browsed by the engine.
// So we get a merge of the aggregated columns (here "count" and "total") and the last line browsed.
groupByEventAndDevice := aggregation.GroupBy(set.Events[family], aggregationSet.Compute, "event_type", "device_type")
for name, event := range groupByEventAndDevice {
    fmt.Printf("group name: %s\n", name)
    fmt.Printf("latest: %+v\n", event)
}
```

## Documentation

### Godoc

To browse the documentation, you can use `godoc` in the root folder of the library.

```shell
go get -u golang.org/x/tools/godoc
cd $GOPATH/src/github.com/DTSL/go-bigtable-access-layer
godoc
```

Then head to http://localhost:6060/pkg/github.com/DTSL/go-bigtable-access-layer/

### Example code

Please see the code in the ['example folder](.example) for a complete example that you can run on your local machine using the emulator provided with Google Cloud SDK.
