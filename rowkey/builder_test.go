package rowkey

import (
	"fmt"
	"testing"
)

func ExampleNewBuilder() {
	sepOpt := NewSeparatorOption(":")
	processOpt := NewProcessOption(func(s string) string {
		return s
	})
	b := NewBuilder(sepOpt, processOpt)
	key := b.ToRowKey("1234", "john")
	fmt.Println(key)

	// Output:
	// 1234:john
}

func ExampleReverse() {
	processOpt := NewProcessOption(Reverse)
	// we explicitly tell the builder to use the `Reverse` function to process each part of the key
	b := NewBuilder(processOpt)
	key := b.ToRowKey("1234", "john")
	fmt.Println(key)

	// Output:
	// 4321#nhoj
}

func ExampleReverseIfInteger() {
	processOpt := NewProcessOption(ReverseIfInteger)
	// we explicitly tell the builder to use the `ReverseIfInteger` function to process each part of the key
	b := NewBuilder(processOpt)
	key := b.ToRowKey("1234", "john")
	fmt.Println(key)

	// Output:
	// 4321#john
}

func TestDetectIsInteger(t *testing.T) {
	tests := []struct {
        input  string
        output bool
    }{
		{"1", true},
		{"-1", true},
		{"0", true},
		{"-0", true},
		{"hello", false},
		{"john5", false},
	}
	for _, test := range tests {
        if detectIsInteger(test.input) != test.output {
            t.Errorf("detectIsInteger(%s) = %v, want %v", test.input, detectIsInteger(test.input), test.output)
        }
    }
}

func TestBuilder_ToRowKey(t *testing.T) {
	b := NewBuilder()
	key := b.ToRowKey("hello", "world")
	if key != "hello#world" {
        t.Errorf("ToRowKey(\"hello\", \"world\") = %s, want \"hello#world\"", key)
    }

	key = b.ToRowKey("123", "456")
	if key != "321#654" {
        t.Errorf("ToRowKey(\"123\", \"456\") = %s, want \"321#654\"", key)
    }

	key = b.ToRowKey("12345")
	if key != "54321" {
        t.Errorf("ToRowKey(\"12345\") = %s, want \"54321\"", key)
    }

	key = b.ToRowKey("12345", "john.doe@example.org")
	if key != "54321#john.doe@example.org" {
		t.Errorf("ToRowKey(\"12345\", \"john.doe@example.org\") = %s, want \"54321#john.doe@example.org\"", key)
	}
}
