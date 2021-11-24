package aggregation

import (
	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/bttest"
	"context"
	"fmt"
	"github.com/DTSL/go-bigtable-access-layer/mapping"
	"github.com/DTSL/go-bigtable-access-layer/repository"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"log"
	"testing"
	"time"

	"github.com/DTSL/go-bigtable-access-layer/data"
)

const (
	projectID = "project-id"
	instance     = "instance-id"
	table        = "ecommerce_events"
	columnFamily = "front"
)

var jMapping = `
{
  "raws": {
    "u": "url",
    "a": "amount"
  },
  "mapped": {
    "d": {
      "name": "device_type",
      "values": {
        "1": "Smartphone",
        "2": "Computer"
      }
    }
  },
  "reversed": [
    {
      "name": "event_type",
      "values": {
        "1": "page_view",
        "2": "add_to_cart",
        "3": "purchase"
      }
    }
  ]
}
`

func ExampleGetLatestBy() {
	ctx := context.Background()
	client := getBigTableClient(ctx)
	jsonMapping, err := mapping.LoadMapping([]byte(jMapping))
	if err != nil {
		log.Fatalln(err)
	}
	mapper := mapping.NewMapper(jsonMapping)
	tbl := client.Open(table)

	repo := repository.NewRepository(tbl, mapper)
	eventSet, err := repo.Read(ctx, "contact-3")
	if err != nil {
		log.Fatalln(err)
	}
	latest := GetLatestBy(eventSet.Events["front"], "device_type")

	// we prefer to access each event individually as it's impossible to predict the order in which the events will be returned
	fmt.Println(latest["Computer"].Date)
	fmt.Println(latest["Computer"].Cells["device_type"])
	fmt.Println(latest["Smartphone"].Date)
	fmt.Println(latest["Smartphone"].Cells["device_type"])

	// Output:
	// 1970-01-01 02:39:00 +0100 CET
	// Computer
	// 1970-01-01 02:38:00 +0100 CET
	// Smartphone
}

func ExampleGroupBy() {
	ctx := context.Background()
	client := getBigTableClient(ctx)
	jsonMapping, err := mapping.LoadMapping([]byte(jMapping))
	if err != nil {
		log.Fatalln(err)
	}
	mapper := mapping.NewMapper(jsonMapping)
	tbl := client.Open(table)

	repo := repository.NewRepository(tbl, mapper)
	eventSet, err := repo.Read(ctx, "contact-3")
	if err != nil {
		log.Fatalln(err)
	}
	cnt := NewCount("count")
	total := NewSum("amount", "total_amount")

	set := NewAggregationSet()
	set.Add(cnt.Compute)
	set.Add(total.Compute)
	grouped := GroupBy(eventSet.Events["front"], set.Compute, "device_type", "event_type")

	fmt.Println(grouped["Computerpurchase"].Cells["device_type"])
	fmt.Println(grouped["Computerpurchase"].Cells["event_type"])
	fmt.Println(grouped["Computerpurchase"].Cells["count"])
	fmt.Println(grouped["Computerpurchase"].Cells["total_amount"])

	// Output:
	// Computer
	// purchase
	// 5
	// 150
}

func TestGroupByOne(t *testing.T) {
	events := []*data.Event{
		{
			Date: time.Now().AddDate(0, 0, -4),
			Cells: map[string]string{
				"id":     "12",
				"status": "added",
			},
		},
		{
			Date: time.Now().AddDate(0, 0, -1),
			Cells: map[string]string{
				"id":     "12",
				"status": "confirmed",
			},
		},
		{
			Date: time.Now().AddDate(0, 0, -4),
			Cells: map[string]string{
				"id":     "15",
				"status": "added",
			},
		},
		{
			Date: time.Now().AddDate(0, 0, -8),
			Cells: map[string]string{
				"id":     "12",
				"status": "added",
			},
		},
		{
			Date: time.Now().AddDate(0, 0, -1),
			Cells: map[string]string{
				"id":     "15",
				"status": "canceled",
			},
		},
		{
			Date: time.Now().AddDate(0, 0, -10),
			Cells: map[string]string{
				"id":     "16",
				"status": "added",
			},
		},
	}

	result := GetLatestBy(events, "id")
	expected := map[string]*data.Event{
		"12": events[1],
		"15": events[4],
		"16": events[5],
	}
	comp(t, result, expected)
}

func TestGroupBy(t *testing.T) {
	lines := []*data.Event{
		{
			Date: time.Now().AddDate(-25, 0, 0),
			Cells: map[string]string{
				"country":    "france",
				"city":       "paris",
				"population": "1200000",
			},
		},
		{
			Date: time.Now().AddDate(-1, 0, 0),
			Cells: map[string]string{
				"country":    "france",
				"city":       "paris",
				"population": "1500000",
			},
		},
		{
			Date: time.Now().AddDate(-25, 0, 0),
			Cells: map[string]string{
				"country":    "texas",
				"city":       "paris",
				"population": "400000",
			},
		},
		{
			Date: time.Now().AddDate(-12, 0, 0),
			Cells: map[string]string{
				"country":    "texas",
				"city":       "paris",
				"population": "450000",
			},
		},
		{
			Date: time.Now().AddDate(-2, 0, 0),
			Cells: map[string]string{
				"country":    "texas",
				"city":       "paris",
				"population": "500000",
			},
		},
		{
			Date: time.Now().AddDate(-25, 0, 0),
			Cells: map[string]string{
				"country":    "france",
				"city":       "marseille",
				"population": "900000",
			},
		},
		{
			Date: time.Now().AddDate(0, 0, -1),
			Cells: map[string]string{
				"country":    "france",
				"city":       "marseille",
				"population": "1200000",
			},
		},
		{
			Date: time.Now().AddDate(0, 0, -10),
			Cells: map[string]string{
				"country":    "germany",
				"city":       "berlin",
				"population": "1800000",
			},
		},
	}

	result := GetLatestBy(lines, "country", "city")
	expected := map[string]*data.Event{
		"francemarseille": lines[6],
		"franceparis":     lines[1],
		"germanyberlin":   lines[7],
		"texasparis":      lines[4],
	}
	comp(t, result, expected)
}

func TestGroupByCount(t *testing.T) {
	lines := []*data.Event{
		{
			Date: time.Now().AddDate(-25, 0, 0),
			Cells: map[string]string{
				"country":    "france",
				"city":       "paris",
				"population": "1200000",
			},
		},
		{
			Date: time.Now().AddDate(-1, 0, 0),
			Cells: map[string]string{
				"country":    "france",
				"city":       "paris",
				"population": "1500000",
			},
		},
		{
			Date: time.Now().AddDate(-25, 0, 0),
			Cells: map[string]string{
				"country":    "texas",
				"city":       "paris",
				"population": "400000",
			},
		},
		{
			Date: time.Now().AddDate(-12, 0, 0),
			Cells: map[string]string{
				"country":    "texas",
				"city":       "paris",
				"population": "450000",
			},
		},
		{
			Date: time.Now().AddDate(-2, 0, 0),
			Cells: map[string]string{
				"country":    "texas",
				"city":       "paris",
				"population": "500000",
			},
		},
		{
			Date: time.Now().AddDate(-25, 0, 0),
			Cells: map[string]string{
				"country":    "france",
				"city":       "marseille",
				"population": "900000",
			},
		},
		{
			Date: time.Now().AddDate(0, 0, -1),
			Cells: map[string]string{
				"country":    "france",
				"city":       "marseille",
				"population": "1200000",
			},
		},
		{
			Date: time.Now().AddDate(0, 0, -10),
			Cells: map[string]string{
				"country":    "germany",
				"city":       "berlin",
				"population": "1800000",
			},
		},
	}

	cnt := NewCount("count")
	result := GroupBy(lines, cnt.Compute, "country")
	lines[4].Cells["count"] = "3"
	lines[6].Cells["count"] = "4"
	lines[7].Cells["count"] = "1"

	expected := map[string]*data.Event{
		"france":  lines[6],
		"germany": lines[7],
		"texas":   lines[4],
	}
	comp(t, result, expected)
}

func TestAverage_Compute(t *testing.T) {
	lines := []*data.Event{
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "12",
				"amount": "115",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "12",
				"amount": "120",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "15",
				"amount": "200",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "12",
				"amount": "110",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "15",
				"amount": "100",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "16",
				"amount": "900",
			},
		},
	}

	avg := NewAverage("amount", "average")
	result := GroupBy(lines, avg.Compute, "id")
	expected := map[string]*data.Event{
		"12": {
			Date: time.Now(),
			Cells: map[string]string{
				"id":      "12",
				"amount":  "110",
				"average": "115",
			},
		},
		"15": {
			Date: time.Now(),
			Cells: map[string]string{
				"id":      "15",
				"amount":  "100",
				"average": "150",
			},
		},
		"16": {
			Date: time.Now(),
			Cells: map[string]string{
				"id":      "16",
				"amount":  "900",
				"average": "900",
			},
		},
	}
	comp(t,  result, expected)
}

func TestSum_Compute(t *testing.T) {
	lines := []*data.Event{
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "12",
				"amount": "115",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "12",
				"amount": "120",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "15",
				"amount": "200",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "12",
				"amount": "110",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "15",
				"amount": "100",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "16",
				"amount": "900",
			},
		},
	}

	sum := NewSum("amount", "sum")
	result := GroupBy(lines, sum.Compute, "id")
	expected := map[string]*data.Event{
		"12": {
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "12",
				"amount": "110",
				"sum":    "345",
			},
		},
		"15": {
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "15",
				"amount": "100",
				"sum":    "300",
			},
		},
		"16": {
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "16",
				"amount": "900",
				"sum":    "900",
			},
		},
	}
	comp(t, result, expected)
}

func TestMax_Compute(t *testing.T) {
	lines := []*data.Event{
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "12",
				"amount": "115",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "12",
				"amount": "120",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "15",
				"amount": "200",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "12",
				"amount": "110",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "15",
				"amount": "100",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "16",
				"amount": "900",
			},
		},
	}

	max := NewMax("amount", "max")
	result := GroupBy(lines, max.Compute, "id")
	expected := map[string]*data.Event{
		"12": {
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "12",
				"amount": "110",
				"max":    "120",
			},
		},
		"15": {
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "15",
				"amount": "100",
				"max":    "200",
			},
		},
		"16": {
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "16",
				"amount": "900",
				"max":    "900",
			},
		},
	}
	comp(t, result, expected)
}

func TestMin_Compute(t *testing.T) {
	lines := []*data.Event{
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "12",
				"amount": "115",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "12",
				"amount": "120",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "15",
				"amount": "200",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "12",
				"amount": "110",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "15",
				"amount": "100",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "16",
				"amount": "900",
			},
		},
	}

	min := NewMin("amount", "min")
	result := GroupBy(lines, min.Compute, "id")
	expected := map[string]*data.Event{
		"12": {
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "12",
				"amount": "110",
				"min":    "110",
			},
		},
		"15": {
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "15",
				"amount": "100",
				"min":    "100",
			},
		},
		"16": {
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "16",
				"amount": "900",
				"min":    "900",
			},
		},
	}
	comp(t, result, expected)
}

func TestAggregationSet_Compute(t *testing.T) {
	lines := []*data.Event{
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "12",
				"amount": "115",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "12",
				"amount": "120",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "15",
				"amount": "200",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "12",
				"amount": "110",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "15",
				"amount": "100",
			},
		},
		{
			Date: time.Now(),
			Cells: map[string]string{
				"id":     "16",
				"amount": "900",
			},
		},
	}
	avg := NewAverage("amount", "average")
	sum := NewSum("amount", "sum")
	cnt := NewCount("count")

	set := NewAggregationSet()
	set.Add(avg.Compute)
	set.Add(sum.Compute)
	set.Add(cnt.Compute)

	result := GroupBy(lines, set.Compute, "id")
	expected := map[string]*data.Event{
		"12": {
			Date: time.Now(),
			Cells: map[string]string{
				"id":      "12",
				"amount":  "110",
				"sum":     "345",
				"count":   "3",
				"average": "115",
			},
		},
		"15": {
			Date: time.Now(),
			Cells: map[string]string{
				"id":      "15",
				"amount":  "100",
				"sum":     "300",
				"count":   "2",
				"average": "150",
			},
		},
		"16": {
			Date: time.Now(),
			Cells: map[string]string{
				"id":      "16",
				"amount":  "900",
				"sum":     "900",
				"count":   "1",
				"average": "900",
			},
		},
	}
	comp(t, result, expected)
}

func comp(t *testing.T, result, expected map[string]*data.Event) {
	if len(result) != len(expected) {
		t.Fatalf("wrong number of events, wanted: %v, got :%v \n", len(expected), len(result))
	}
	for name, events := range result {
		if _, ok := expected[name]; ! ok {
			t.Fatalf("unexpected group %s \n", name)
		}
		for cname, cvalue := range events.Cells {
			if v, ok := expected[name].Cells[cname]; ! ok {
				t.Fatalf("unexpected column")
			} else {
				if cvalue != v {
					t.Fatalf("value mismatch, wanted: %v, got %v\n", v, cvalue)
				}
			}
		}
	}
}

func getBigTableClient(ctx context.Context) *bigtable.Client {
	srv, err := bttest.NewServer("localhost:0")
	if err != nil {
		log.Fatalln(err)
	}
	conn, err := grpc.Dial(srv.Addr, grpc.WithInsecure())
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
	var mutations []*bigtable.Mutation
	for i := 0; i < numEvents; i++ {
		mod := i % 20
		mut := bigtable.NewMutation()
		t := bigtable.Time(time.UnixMilli(0).Add(time.Duration(i) * time.Minute))
		mut.Set("front", "u", t, []byte(fmt.Sprintf("https://www.example.com/products/%d", mod)))
		switch mod {
		case 1, 2:
			mut.Set("front", "2", t, []byte("1"))
		case 3:
			mut.Set("front", "3", t, []byte("1"))
		default:
			mut.Set("front", "1", t, []byte("1"))
		}
		mut.Set("front", "a", t, []byte(fmt.Sprintf("%d", mod * 10)))
		mut.Set("front", "d", t, []byte(fmt.Sprintf("%d", 1+(i%2))))
		mutations = append(mutations, mut)
	}
	return mutations
}
