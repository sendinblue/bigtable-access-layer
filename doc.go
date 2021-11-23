/*
Copyright 2021 Sendinblue S.A.S
 */

/*
Package go_bigtable_access_layer is a library designed to ease reading data from Big Table. it features:

 - Cells grouping to build events using the timestamp
 - Schema mapping to transform the data into a human-readable format
 - Aggregation functions like count(), min(), max(), sum().

This library fits fine when you want to store time series data in Big Table like:

 - user-generated events we can capture on a website ('page view', 'click', 'purchase', etc.)
 - series of measures from weather sensors ('temperature', 'humidity', 'wind speed', etc.)

In those use-cases, each row will be a logical set of events, with its row key built in a way it can be easily
identified and will contain a manageable number of events. For instance, a row key could include the region of the
weather station, the year and the week number separated with `#` to look like `europe-west1#2021#week1`.
Each event is a set of cells sharing the same timestamp, so when the access-layer turns a row into a set of events,
it groups cells by timestamp to end with one event / timestamp.
Here's an example from Google's documentation: https://cloud.google.com/bigtable/docs/schema-design-time-series#time-buckets


Mapping

Big Table treats column qualifiers as data not metadata, meaning that each character in a column qualifier counts. So the longer a column qualifier is, the more it will use space. As a consequence, Google recommends using the column qualifier as data or if it's not possible, to use short but meaningful column names. It will save space and reduce amount of transferred data.

The mapping system is here to turn short column names into human-readable equivalent. It can also be used when the column qualifier contains data, granted it is an "enum" as defined in the mapping.

here's an example of a mapping:

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

And now how to use it in the mapper:


	func read(ctx context.Context, client *bigtable.Client, out io.Writer, table string) error {
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
		// bigtable.Table.ReadRow returns a map[string][]bigtable.ReadItem where the string is the column family name.
		for family, items := range row {
			fmt.Fprintf(out, "processing family: %s\n", family)
			// here we use the mapper to turn the readItems into a set of data.Event.
			cols, events := mapper.GetMappedEvents(items)
			fmt.Fprintf(out, "Columns: %+v\n", cols)
			for _, event := range events {
				fmt.Fprintf(out, "Event: %+v\n", event)
			}
		}
		return nil
	}

Repository

The repository embeds the mapper to have easy access to mapped data. It also provides a search engine that performs all
the required logic to search filtered data and collect all properties for each event.

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

*/
package go_bigtable_access_layer // import "github.com/sendinblue/go-bigtable-access-layer"
