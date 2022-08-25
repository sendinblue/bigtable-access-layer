package repository

import (
	"context"
	"embed"
	"fmt"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"regexp"
	"strconv"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/bttest"
	"github.com/sendinblue/bigtable-access-layer/data"
	"github.com/sendinblue/bigtable-access-layer/mapping"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

const (
	projectID    = "project-id"
	instance     = "instance-id"
	table        = "ecommerce_events"
	columnFamily = "front"
)

func ExampleRepository_Read() {
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

	repo := NewRepository(tbl, mapper)
	eventSet, err := repo.Read(ctx, "contact-3")
	if err != nil {
		log.Fatalln(err)
	}

	fmt.Println(eventSet.Events["front"][0].Cells["event_type"])

	// Output:
	// page_view
}

func ExampleRepository_Write() {
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

	repo := NewRepository(tbl, mapper)
	eventSet := &data.Set{Events: map[string][]*data.Event{
		"front": {
			{
				RowKey: "contact-101",
				Date:   time.Date(2018, time.January, 1, 0, 0, 0, 0, time.UTC),
				Cells: map[string]string{
					"event_type":  "page_view",
					"device_type": "Smartphone",
					"url":         "https://example.org/some/product",
				},
			},
			{
				RowKey: "contact-101",
				Date:   time.Date(2018, time.January, 1, 0, 1, 0, 0, time.UTC),
				Cells: map[string]string{
					"event_type":  "add_to_cart",
					"device_type": "Smartphone",
					"url":         "https://example.org/some/product",
				},
			},
			{
				RowKey: "contact-101",
				Date:   time.Date(2018, time.January, 1, 0, 2, 0, 0, time.UTC),
				Cells: map[string]string{
					"event_type":  "purchase",
					"device_type": "Smartphone",
					"url":         "https://example.org/some/product",
				},
			},
			{
				RowKey: "contact-102",
				Date:   time.Date(2018, time.January, 1, 0, 2, 0, 0, time.UTC),
				Cells: map[string]string{
					"event_type":  "add_to_cart",
					"device_type": "Computer",
					"url":         "https://example.org/some/product",
				},
			},
		},
	}}

	errs, err := repo.Write(ctx, eventSet)
	if err != nil {
		log.Fatalln(err)
	}
	if len(errs) > 0 {
		log.Fatalln(errs)
	}

	row, err := tbl.ReadRow(ctx, "contact-101")
	if err != nil {
		log.Fatalln(err)
	}

	fmt.Println("cells-for_contact-101")
	for _, family := range row {
		for _, cell := range family {
			fmt.Println(cell.Column, string(cell.Value))
		}
	}

	fmt.Println("cells-for_contact-102")
	row, err = tbl.ReadRow(ctx, "contact-102")
	if err != nil {
		log.Fatalln(err)
	}
	for _, family := range row {
		for _, cell := range family {
			fmt.Println(cell.Column, string(cell.Value))
		}
	}

	readSet, err := repo.Read(ctx, "contact-102")
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("mapped-event-for_contact-102")
	for _, event := range readSet.Events["front"] {
		fmt.Println(event.Date.UTC())
		fmt.Println(event.RowKey)
		fmt.Println(event.Cells["event_type"])
		fmt.Println(event.Cells["device_type"])
	}
	// Output:
	// cells-for_contact-101
	// front:d 1
	// front:d 1
	// front:d 1
	// front:e 13
	// front:e 12
	// front:e 11
	// front:u https://example.org/some/product
	// front:u https://example.org/some/product
	// front:u https://example.org/some/product
	// cells-for_contact-102
	// front:d 2
	// front:e 12
	// front:u https://example.org/some/product
	// mapped-event-for_contact-102
	// 2018-01-01 00:02:00 +0000 UTC
	// contact-102
	// add_to_cart
	// Computer

}

func ExampleRepository_Search() {
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

	repo := NewRepository(tbl, mapper)
	eventSet := &data.Set{Events: map[string][]*data.Event{
		"front": {
			{
				RowKey: "contactx-101",
				Date:   time.Date(2018, time.January, 1, 0, 0, 0, 0, time.UTC),
				Cells: map[string]string{
					"event_type":  "page_view",
					"device_type": "Smartphone",
					"url":         "https://example.org/some/product",
				},
			},
			{
				RowKey: "contactx-101",
				Date:   time.Date(2018, time.January, 1, 0, 1, 0, 0, time.UTC),
				Cells: map[string]string{
					"event_type":  "add_to_cart",
					"device_type": "Smartphone",
					"url":         "https://example.org/some/product",
				},
			},
			{
				RowKey: "contactx-101",
				Date:   time.Date(2018, time.January, 1, 0, 2, 0, 0, time.UTC),
				Cells: map[string]string{
					"event_type":  "purchase",
					"device_type": "Smartphone",
					"url":         "https://example.org/some/product",
				},
			},
			{
				RowKey: "contactx-102",
				Date:   time.Date(2018, time.January, 1, 0, 2, 0, 0, time.UTC),
				Cells: map[string]string{
					"event_type":  "add_to_cart",
					"device_type": "Computer",
					"url":         "https://example.org/some/product",
				},
			},
			{
				RowKey: "contacty-102",
				Date:   time.Date(2018, time.January, 1, 0, 2, 0, 0, time.UTC),
				Cells: map[string]string{
					"event_type":  "add_to_cart",
					"device_type": "Computer",
					"url":         "https://example.org/some/product",
				},
			},
		},
	}}
	errs, err := repo.Write(ctx, eventSet)
	if err != nil {
		log.Fatalln(err)
	}
	if len(errs) > 0 {
		log.Fatalln(errs)
	}
	readSet, err := repo.Search(ctx, bigtable.PrefixRange("contactx"), bigtable.CellsPerRowLimitFilter(1))
	if err != nil {
		log.Fatalln(err)
	}
	events := sortByContactID(readSet.Events["front"])
	for _, event := range events {
		fmt.Println(event.Date.UTC())
		fmt.Println(event.RowKey)
		fmt.Println(event.Cells["event_type"])
		fmt.Println(event.Cells["device_type"])
	}

	// Output:
	// 2018-01-01 00:02:00 +0000 UTC
	// contactx-101
	// purchase
	// Smartphone
	// 2018-01-01 00:02:00 +0000 UTC
	// contactx-102
	// add_to_cart
	// Computer
}

func sortByContactID(events []*data.Event) []*data.Event {
	for i := 0; i < len(events); i++ {
		iID := extractID(events[i].RowKey)
		for j := 0; j < len(events); j++ {
			jID := extractID(events[j].RowKey)
			if iID < jID {
				perm := events[j]
				events[j] = events[i]
				events[i] = perm
			}
		}
	}
	return events
}

func TestExtractID(t *testing.T) {
	l := "contact-113"
	i := extractID(l)
	if i != 113 {
		t.Fatalf("unexpected value: %+v", i)
	}
}

func extractID(literalID string) int {
	re := regexp.MustCompile("[0-9]+")
	m := re.FindAllString(literalID, 1)
	out := 0
	for _, f := range m {
		i, err := strconv.Atoi(f)
		if err != nil {
			log.Fatalln(err)
			return out
		}
		out = i
	}
	return out
}

func ExampleRepository_ReadLast() {
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

	repo := NewRepository(tbl, mapper)
	eventSet := &data.Set{Events: map[string][]*data.Event{
		"front": {
			{
				RowKey: "contactz-102",
				Date:   time.Date(2018, time.January, 1, 0, 2, 0, 0, time.UTC),
				Cells: map[string]string{
					"event_type":  "add_to_cart",
					"device_type": "Computer",
					"url":         "https://example.org/some/product",
				},
			},
		},
	}}

	// insert
	errs, err := repo.Write(ctx, eventSet)
	if err != nil {
		log.Fatalln(err)
	}
	if len(errs) > 0 {
		log.Fatalln(errs)
	}

	// update
	eventSet = &data.Set{Events: map[string][]*data.Event{
		"front": {
			{
				RowKey: "contactz-102",
				Date:   time.Now(),
				Cells: map[string]string{
					"event_type":  "purchase",
					"device_type": "Smartphone",
					"url":         "https://example.org/some/cart",
				},
			},
		},
	}}
	errs, err = repo.Write(ctx, eventSet)
	if err != nil {
		log.Fatalln(err)
	}
	if len(errs) > 0 {
		log.Fatalln(errs)
	}

	readSet, err := repo.ReadLast(ctx, "contactz-102")
	if err != nil {
		log.Fatalln(err)
	}
	for _, event := range readSet.Events["front"] {
		fmt.Println(event.Cells["event_type"])
		fmt.Println(event.Cells["device_type"])
	}

	// Output:
	// purchase
	// Smartphone
}

func ExampleRepository_ReadFamily() {
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

	repo := NewRepository(tbl, mapper)
	eventSet := &data.Set{Events: map[string][]*data.Event{
		"front": {
			{
				RowKey: "contactz-102",
				Date:   time.Date(2018, time.January, 1, 0, 2, 0, 0, time.UTC),
				Cells: map[string]string{
					"event_type":  "add_to_cart",
					"device_type": "Computer",
					"url":         "https://example.org/some/product",
				},
			},
		},
		"blog": {
			{
				RowKey: "contactz-102",
				Date:   time.Date(2018, time.January, 1, 0, 2, 0, 0, time.UTC),
				Cells: map[string]string{
					"event_type":  "page_view",
					"device_type": "Computer",
					"url":         "https://example.org/blog/article/1",
				},
			},
		},
	}}

	// insert
	errs, err := repo.Write(ctx, eventSet)
	if err != nil {
		log.Fatalln(err)
	}
	if len(errs) > 0 {
		log.Fatalln(errs)
	}

	readSet, err := repo.ReadFamily(ctx, "contactz-102", "blog")
	if err != nil {
		log.Fatalln(err)
	}
	for _, event := range readSet.Events["blog"] {
		fmt.Println(event.Cells["event_type"])
		fmt.Println(event.Cells["device_type"])
	}
	// Output:
	// page_view
	// Computer
}

var t1 = bigtable.Time(time.Date(2020, time.January, 1, 0, 1, 0, 0, time.UTC))
var t2 = bigtable.Time(time.Date(2020, time.January, 1, 0, 2, 0, 0, time.UTC))
var t3 = bigtable.Time(time.Date(2020, time.January, 1, 0, 3, 0, 0, time.UTC))

func TestRepository_Read(t *testing.T) {

	ctx := context.Background()
	repository := &Repository{
		adapter: mockAdapter{},
		mapper:  getMockMapper(t),
	}
	eventSet, err := repository.Read(ctx, "contact-3")
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}
	if len(eventSet.Events) != 1 {
		t.Fatalf("expected 1 event family, got %d", len(eventSet.Events))
	}
	if v, ok := eventSet.Events["front"]; !ok {
		t.Fatalf("expected front family, got %v", v)
	} else {
		if len(v) != 3 {
			t.Fatalf("expected 3 events, got %d", len(v))
		}

		if v[0].RowKey != "contact-3" {
			t.Fatalf("expected contact-3, got %s", v[0].RowKey)
		}
		if v[0].Cells["url"] != "http://someexample.url/query/string/1" {
			t.Fatalf("expected http://someexample.url/query/string/1, got %s", v[0].Cells["url"])
		}
		if v[0].Cells["device_type"] != "Smartphone" {
			t.Fatalf("expected Smartphone, got %s", v[0].Cells["device_type"])
		}
		// here we're testing each event_type depending on the timestamp.
		// It's because Go doesn't guarantee the order of the map iteration
		for _, event := range v {
			if event.Date.Unix() == t1.Time().Unix() {
				if event.Cells["event_type"] != "page_view" {
					t.Fatalf("expected page_view, got %s", event.Cells["event_type"])
				}
			}
			if event.Date.Unix() == t2.Time().Unix() {
				if event.Cells["event_type"] != "add_to_cart" {
					t.Fatalf("expected add_to_cart, got %s", event.Cells["event_type"])
				}
			}
			if event.Date.Unix() == t3.Time().Unix() {
				if event.Cells["event_type"] != "purchase" {
					t.Fatalf("expected purchase, got %s", event.Cells["event_type"])
				}
			}
		}
	}
}

func TestRepository_Search(t *testing.T) {
	ctx := context.Background()
	repository := &Repository{
		adapter: mockAdapter{},
		mapper:  getMockMapper(t),
	}
	filter := bigtable.ColumnFilter("d")
	eventSet, err := repository.Search(ctx, bigtable.RowRange{}, filter)
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}
	if len(eventSet.Events) != 1 {
		t.Fatalf("expected 1 event family, got %d", len(eventSet.Events))
	}
	if v, ok := eventSet.Events["front"]; !ok {
		t.Fatalf("expected front family, got %v", v)
	} else {
		if len(v) != 3 {
			t.Fatalf("expected 3 events, got %d", len(v))
		}
		if v[0].Cells["url"] != "http://someexample.url/query/string/1" {
			t.Fatalf("expected http://someexample.url/query/string/1, got %s", v[0].Cells["url"])
		}
		if v[0].Cells["device_type"] != "Smartphone" {
			t.Fatalf("expected Smartphone, got %s", v[0].Cells["device_type"])
		}
		for _, event := range v {
			if event.Date.Unix() == t1.Time().Unix() {
				if event.Cells["event_type"] != "page_view" {
					t.Fatalf("expected page_view, got %s", event.Cells["event_type"])
				}
			}
			if event.Date.Unix() == t2.Time().Unix() {
				if event.Cells["event_type"] != "add_to_cart" {
					t.Fatalf("expected add_to_cart, got %s", event.Cells["event_type"])
				}
			}
			if event.Date.Unix() == t3.Time().Unix() {
				if event.Cells["event_type"] != "purchase" {
					t.Fatalf("expected purchase, got %s", event.Cells["event_type"])
				}
			}
		}
	}

}

func TestRepository_ReadLast(t *testing.T) {
	ctx := context.Background()
	repository := &Repository{
		adapter: mockAdapter{},
		mapper:  getMockMapper(t),
	}
	eventSet, err := repository.ReadLast(ctx, "contact-3")
	if err != nil {
		t.Fatal(err)
	}
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}
	if len(eventSet.Events) != 1 {
		t.Fatalf("expected 1 event family, got %d", len(eventSet.Events))
	}
	if v, ok := eventSet.Events["front"]; !ok {
		t.Fatalf("expected front family, got %v", v)
	} else {
		if len(v) != 3 {
			t.Fatalf("expected 3 events, got %d", len(v))
		}

		if v[0].RowKey != "contact-3" {
			t.Fatalf("expected contact-3, got %s", v[0].RowKey)
		}
		if v[0].Cells["url"] != "http://someexample.url/query/string/1" {
			t.Fatalf("expected http://someexample.url/query/string/1, got %s", v[0].Cells["url"])
		}
		if v[0].Cells["device_type"] != "Smartphone" {
			t.Fatalf("expected Smartphone, got %s", v[0].Cells["device_type"])
		}
		// here we're testing each event_type depending on the timestamp.
		// It's because Go doesn't guarantee the order of the map iteration
		for _, event := range v {
			if event.Date.Unix() == t1.Time().Unix() {
				if event.Cells["event_type"] != "page_view" {
					t.Fatalf("expected page_view, got %s", event.Cells["event_type"])
				}
			}
			if event.Date.Unix() == t2.Time().Unix() {
				if event.Cells["event_type"] != "add_to_cart" {
					t.Fatalf("expected add_to_cart, got %s", event.Cells["event_type"])
				}
			}
			if event.Date.Unix() == t3.Time().Unix() {
				if event.Cells["event_type"] != "purchase" {
					t.Fatalf("expected purchase, got %s", event.Cells["event_type"])
				}
			}
		}
	}
}

func TestRepository_ReadFamily(t *testing.T) {
	ctx := context.Background()
	repository := &Repository{
		adapter: mockAdapter{},
		mapper:  getMockMapper(t),
	}
	eventSet, err := repository.ReadFamily(ctx, "contact-3", "front")
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}
	if len(eventSet.Events) != 1 {
		t.Fatalf("expected 1 event family, got %d", len(eventSet.Events))
	}
	if v, ok := eventSet.Events["front"]; !ok {
		t.Fatalf("expected front family, got %v", v)
	} else {
		if len(v) != 3 {
			t.Fatalf("expected 3 events, got %d", len(v))
		}

		if v[0].RowKey != "contact-3" {
			t.Fatalf("expected contact-3, got %s", v[0].RowKey)
		}
		if v[0].Cells["url"] != "http://someexample.url/query/string/1" {
			t.Fatalf("expected http://someexample.url/query/string/1, got %s", v[0].Cells["url"])
		}
		if v[0].Cells["device_type"] != "Smartphone" {
			t.Fatalf("expected Smartphone, got %s", v[0].Cells["device_type"])
		}
		// here we're testing each event_type depending on the timestamp.
		// It's because Go doesn't guarantee the order of the map iteration
		for _, event := range v {
			if event.Date.Unix() == t1.Time().Unix() {
				if event.Cells["event_type"] != "page_view" {
					t.Fatalf("expected page_view, got %s", event.Cells["event_type"])
				}
			}
			if event.Date.Unix() == t2.Time().Unix() {
				if event.Cells["event_type"] != "add_to_cart" {
					t.Fatalf("expected add_to_cart, got %s", event.Cells["event_type"])
				}
			}
			if event.Date.Unix() == t3.Time().Unix() {
				if event.Cells["event_type"] != "purchase" {
					t.Fatalf("expected purchase, got %s", event.Cells["event_type"])
				}
			}
		}
	}
}

//go:embed testdata/mapping.json
var fs embed.FS

func getMockMapper(t *testing.T) *mapping.Mapper {
	c, err := fs.ReadFile("testdata/mapping.json")
	if err != nil {
		t.Fatalf("failed to read mapping.json: %v", err)
	}
	jsonMapping, err := mapping.LoadMapping(c)
	if err != nil {
		t.Fatalf("failed to load mapping: %v", err)
	}
	return mapping.NewMapper(jsonMapping)
}

type mockAdapter struct{}

func (a mockAdapter) ReadRows(_ context.Context, _ bigtable.RowSet, f func(bigtable.Row) bool, _ ...bigtable.ReadOption) (err error) {
	for _, row := range getRows() {
		f(row)
	}
	return nil
}

func getRows() []bigtable.Row {
	return []bigtable.Row{
		{
			"front": []bigtable.ReadItem{
				{
					Row:       "contact-3",
					Column:    "d",
					Value:     []byte("1"),
					Timestamp: t1,
				},
				{
					Row:       "contact-3",
					Column:    "d",
					Value:     []byte("1"),
					Timestamp: t2,
				},
				{
					Row:       "contact-3",
					Column:    "d",
					Value:     []byte("1"),
					Timestamp: t3,
				},
			},
		},
	}
}

func (a mockAdapter) ReadRow(_ context.Context, row string, _ ...bigtable.ReadOption) (bigtable.Row, error) {
	output := bigtable.Row{
		"front": []bigtable.ReadItem{
			{
				Row:       row,
				Column:    "u",
				Timestamp: t1,
				Value:     []byte("http://someexample.url/query/string/1"),
			},
			{
				Row:       row,
				Column:    "d",
				Timestamp: t1,
				Value:     []byte("1"),
			},
			{
				Row:       row,
				Column:    "e",
				Timestamp: t1,
				Value:     []byte("11"),
			},
			{
				Row:       row,
				Column:    "u",
				Timestamp: t2,
				Value:     []byte("http://someexample.url/query/string/1"),
			},
			{
				Row:       row,
				Column:    "d",
				Timestamp: t2,
				Value:     []byte("1"),
			},
			{
				Row:       row,
				Column:    "e",
				Timestamp: t2,
				Value:     []byte("12"),
			},
			{
				Row:       row,
				Column:    "u",
				Timestamp: t3,
				Value:     []byte("http://someexample.url/query/string/1"),
			},
			{
				Row:       row,
				Column:    "d",
				Timestamp: t3,
				Value:     []byte("1"),
			},
			{
				Row:       row,
				Column:    "e",
				Timestamp: t3,
				Value:     []byte("13"),
			},
		},
	}
	return output, nil
}

func (a mockAdapter) ApplyBulk(_ context.Context, _ []string, _ []*bigtable.Mutation, _ ...bigtable.ApplyOption) (errs []error, err error) {
	return nil, nil
}

func getBigTableClient(ctx context.Context) *bigtable.Client {
	srv, err := bttest.NewServer("localhost:0")
	if err != nil {
		log.Fatalln(err)
	}
	conn, err := grpc.Dial(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalln(err)
	}
	adminClient, err := bigtable.NewAdminClient(ctx, projectID, instance, option.WithGRPCConn(conn))
	if err != nil {
		log.Fatalln(err)
	}
	if err = adminClient.CreateTable(ctx, table); err != nil {
		log.Fatalln(err)
	}
	if err = adminClient.CreateColumnFamily(ctx, table, columnFamily); err != nil {
		log.Fatalln(err)
	}

	if err = adminClient.CreateColumnFamily(ctx, table, "blog"); err != nil {
		log.Fatalln(err)
	}

	client, err := bigtable.NewClient(ctx, projectID, instance, option.WithGRPCConn(conn))
	if err != nil {
		log.Fatalln(err)
	}
	err = fillTable(ctx, client, table)
	if err != nil {
		log.Fatalln(err)
	}
	return client
}

func fillTable(ctx context.Context, client *bigtable.Client, t string) error {
	tbl := client.Open(t)
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
		mut.Set("front", "u", t, []byte(fmt.Sprintf("https://www.example.com/products/%d", mod)))
		switch mod {
		case 1, 2:
			mut.Set("front", "e", t, []byte("12"))
		case 3:
			mut.Set("front", "e", t, []byte("13"))
		default:
			mut.Set("front", "e", t, []byte("11"))
		}
		mut.Set("front", "d", t, []byte(fmt.Sprintf("%d", 1+(i%2))))
		data = append(data, mut)
	}
	return data
}
