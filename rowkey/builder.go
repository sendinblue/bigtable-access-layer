package rowkey

import (
	"strconv"
	"strings"
)

const DefaultSeparator = "#"

type Builder struct {
	separator string
	process   func(string) string
}

func NewBuilder(opts ... BuilderOption) *Builder {
    b := &Builder{
		separator: DefaultSeparator,
		process: ReverseIfInteger,
    }
    for _, opt := range opts {
        opt.apply(b)
    }
    return b
}

func (b *Builder) ToRowKey(parts ...string) string {
	for i, part := range parts {
        parts[i] = b.process(part)
    }
	return strings.Join(parts, b.separator)
}

func ReverseIfInteger(s string) string {
    if detectIsInteger(s) {
        return Reverse(s)
    }
    return s
}

func detectIsInteger(part string) bool {
    _, err := strconv.Atoi(part)
    return err == nil
}

func Reverse(s string) string {
	r := []rune(s)
	for i, j := 0, len(r)-1; i < len(r)/2; i, j = i+1, j-1 {
		r[i], r[j] = r[j], r[i]
	}
	return string(r)
}

//region options

// BuilderOption is a function that can be used to configure a Builder.
type BuilderOption interface {
	apply(*Builder)
}

type SeparatorOption struct {
	separator string
}

func NewSeparatorOption(separator string) SeparatorOption {
	return SeparatorOption{separator}
}

func (o SeparatorOption) apply(builder *Builder) {
	builder.separator = o.separator
}

type ProcessOption struct {
    process func(string) string
}

func NewProcessOption(process func(string) string) ProcessOption {
    return ProcessOption{process}
}

func (o ProcessOption) apply(builder *Builder) {
    builder.process = o.process
}
//endregion
