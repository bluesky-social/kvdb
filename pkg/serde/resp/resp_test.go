package resp

import (
	"bufio"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseCommand(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    *Command
		wantErr bool
	}{
		{
			name:  "simple string command",
			input: "*2\r\n+GET\r\n+key\r\n",
			want: &Command{
				Name: "GET",
				Args: []Value{{Type: TypeSimpleString, Value: "key"}},
			},
		},
		{
			name:  "bulk string command",
			input: "*2\r\n$3\r\nSET\r\n$5\r\nmykey\r\n",
			want: &Command{
				Name: "SET",
				Args: []Value{{Type: TypeBulkString, Value: "mykey"}},
			},
		},
		{
			name:  "mixed args command",
			input: "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n:123\r\n",
			want: &Command{
				Name: "SET",
				Args: []Value{
					{Type: TypeBulkString, Value: "key"},
					{Type: TypeInteger, Value: int64(123)},
				},
			},
		},
		{
			name:  "lowercase command name",
			input: "*1\r\n+ping\r\n",
			want: &Command{
				Name: "PING",
				Args: []Value{},
			},
		},
		{
			name:    "not an array",
			input:   "+PING\r\n",
			wantErr: true,
		},
		{
			name:    "empty array",
			input:   "*0\r\n",
			wantErr: true,
		},
		{
			name:    "null array",
			input:   "*-1\r\n",
			wantErr: true,
		},
		{
			name:    "invalid command name type",
			input:   "*1\r\n:123\r\n",
			wantErr: true,
		},
		{
			name:    "invalid RESP3",
			input:   "invalid\r\n",
			wantErr: true,
		},
		{
			name:    "no newline",
			input:   "invalid",
			wantErr: true,
		},
		{
			name:    "empty command name (fuzz regression)",
			input:   "*1\r\n+\r\n",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(strings.NewReader(tt.input))
			got, err := ParseCommand(reader)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestParseRESP3Value(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    *Value
		wantErr bool
	}{
		{
			name:  "simple string",
			input: "+OK\r\n",
			want:  &Value{Type: TypeSimpleString, Value: "OK"},
		},
		{
			name:  "simple error",
			input: "-ERR unknown command\r\n",
			want:  &Value{Type: TypeSimpleError, Value: "ERR unknown command"},
		},
		{
			name:  "integer",
			input: ":1000\r\n",
			want:  &Value{Type: TypeInteger, Value: int64(1000)},
		},
		{
			name:  "bulk string",
			input: "$6\r\nfoobar\r\n",
			want:  &Value{Type: TypeBulkString, Value: "foobar"},
		},
		{
			name:  "null",
			input: "_\r\n",
			want:  &Value{Type: TypeNull, Value: nil},
		},
		{
			name:  "boolean true",
			input: "#t\r\n",
			want:  &Value{Type: TypeBoolean, Value: true},
		},
		{
			name:  "double",
			input: ",1.23\r\n",
			want:  &Value{Type: TypeDouble, Value: 1.23},
		},
		{
			name:  "big number",
			input: "(3492890328409238509324850943850943825024385\r\n",
			want:  &Value{Type: TypeBigNumber, Value: "3492890328409238509324850943850943825024385"},
		},
		{
			name:    "unknown type",
			input:   "?unknown\r\n",
			wantErr: true,
		},
		{
			name:    "empty input",
			input:   "",
			wantErr: true,
		},
		{
			name:    "extremely large array should fail gracefully and not crash (fuzz regression)",
			input:   "*99999999999\r\n",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(strings.NewReader(tt.input))
			got, err := ParseRESP3Value(reader)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestReadLine(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{
			name:  "simple line with CRLF",
			input: "hello\r\n",
			want:  "hello",
		},
		{
			name:  "simple line with LF only",
			input: "hello\n",
			want:  "hello\n",
		},
		{
			name:  "empty line with CRLF",
			input: "\r\n",
			want:  "",
		},
		{
			name:  "empty line with LF only",
			input: "\n",
			want:  "\n",
		},
		{
			name:  "line with spaces",
			input: "hello world\r\n",
			want:  "hello world",
		},
		{
			name:    "no newline",
			input:   "hello",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(strings.NewReader(tt.input))
			got, err := readLine(reader)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestParseSimpleString(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    *Value
		wantErr bool
	}{
		{
			name:  "basic string",
			input: "OK\r\n",
			want:  &Value{Type: TypeSimpleString, Value: "OK"},
		},
		{
			name:  "empty string",
			input: "\r\n",
			want:  &Value{Type: TypeSimpleString, Value: ""},
		},
		{
			name:  "string with spaces",
			input: "hello world\r\n",
			want:  &Value{Type: TypeSimpleString, Value: "hello world"},
		},
		{
			name:    "no newline",
			input:   "OK",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(strings.NewReader(tt.input))
			got, err := parseSimpleString(reader)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestParseSimpleError(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    *Value
		wantErr bool
	}{
		{
			name:  "basic error",
			input: "ERR unknown command\r\n",
			want:  &Value{Type: TypeSimpleError, Value: "ERR unknown command"},
		},
		{
			name:  "empty error",
			input: "\r\n",
			want:  &Value{Type: TypeSimpleError, Value: ""},
		},
		{
			name:    "no newline",
			input:   "ERR test",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(strings.NewReader(tt.input))
			got, err := parseSimpleError(reader)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestParseInteger(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    *Value
		wantErr bool
	}{
		{
			name:  "positive integer",
			input: "1000\r\n",
			want:  &Value{Type: TypeInteger, Value: int64(1000)},
		},
		{
			name:  "negative integer",
			input: "-1000\r\n",
			want:  &Value{Type: TypeInteger, Value: int64(-1000)},
		},
		{
			name:  "zero",
			input: "0\r\n",
			want:  &Value{Type: TypeInteger, Value: int64(0)},
		},
		{
			name:  "max int64",
			input: "9223372036854775807\r\n",
			want:  &Value{Type: TypeInteger, Value: int64(9223372036854775807)},
		},
		{
			name:  "min int64",
			input: "-9223372036854775808\r\n",
			want:  &Value{Type: TypeInteger, Value: int64(-9223372036854775808)},
		},
		{
			name:    "invalid integer",
			input:   "abc\r\n",
			wantErr: true,
		},
		{
			name:    "float as integer",
			input:   "123.45\r\n",
			wantErr: true,
		},
		{
			name:    "no newline",
			input:   "123",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(strings.NewReader(tt.input))
			got, err := parseInteger(reader)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestParseBulkString(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    *Value
		wantErr bool
	}{
		{
			name:  "basic bulk string",
			input: "6\r\nfoobar\r\n",
			want:  &Value{Type: TypeBulkString, Value: "foobar"},
		},
		{
			name:  "empty bulk string",
			input: "0\r\n\r\n",
			want:  &Value{Type: TypeBulkString, Value: ""},
		},
		{
			name:  "null bulk string",
			input: "-1\r\n",
			want:  &Value{Type: TypeBulkString, Value: nil},
		},
		{
			name:  "bulk string with special chars",
			input: "12\r\nhello\r\nworld\r\n",
			want:  &Value{Type: TypeBulkString, Value: "hello\r\nworld"},
		},
		{
			name:  "bulk string with unicode",
			input: "8\r\n🚀🌟\r\n",
			want:  &Value{Type: TypeBulkString, Value: "🚀🌟"},
		},
		{
			name:    "invalid length",
			input:   "abc\r\ntest\r\n",
			wantErr: true,
		},
		{
			name:    "negative length other than -1",
			input:   "-5\r\n",
			wantErr: true,
		},
		{
			name:    "insufficient data",
			input:   "10\r\nshort\r\n",
			wantErr: true,
		},
		{
			name:    "no length line",
			input:   "foobar\r\n",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(strings.NewReader(tt.input))
			got, err := parseBulkString(reader)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestParseArray(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    *Value
		wantErr bool
	}{
		{
			name:  "empty array",
			input: "0\r\n",
			want:  &Value{Type: TypeArray, Value: []Value{}},
		},
		{
			name:  "null array",
			input: "-1\r\n",
			want:  &Value{Type: TypeArray, Value: nil},
		},
		{
			name:  "array with integers",
			input: "3\r\n:1\r\n:2\r\n:3\r\n",
			want: &Value{Type: TypeArray, Value: []Value{
				{Type: TypeInteger, Value: int64(1)},
				{Type: TypeInteger, Value: int64(2)},
				{Type: TypeInteger, Value: int64(3)},
			}},
		},
		{
			name:  "mixed array",
			input: "3\r\n+hello\r\n:123\r\n$5\r\nworld\r\n",
			want: &Value{Type: TypeArray, Value: []Value{
				{Type: TypeSimpleString, Value: "hello"},
				{Type: TypeInteger, Value: int64(123)},
				{Type: TypeBulkString, Value: "world"},
			}},
		},
		{
			name:  "nested array",
			input: "2\r\n*2\r\n:1\r\n:2\r\n+hello\r\n",
			want: &Value{Type: TypeArray, Value: []Value{
				{Type: TypeArray, Value: []Value{
					{Type: TypeInteger, Value: int64(1)},
					{Type: TypeInteger, Value: int64(2)},
				}},
				{Type: TypeSimpleString, Value: "hello"},
			}},
		},
		{
			name:    "invalid length",
			input:   "abc\r\n",
			wantErr: true,
		},
		{
			name:    "negative length other than -1",
			input:   "-5\r\n",
			wantErr: true,
		},
		{
			name:    "insufficient elements",
			input:   "2\r\n:1\r\n",
			wantErr: true,
		},
		{
			name:    "invalid element",
			input:   "1\r\n?invalid\r\n",
			wantErr: true,
		},
		{
			name:    "no newlin",
			input:   "1\r\n:1",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(strings.NewReader(tt.input))
			got, err := parseArray(reader)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestParseNull(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    *Value
		wantErr bool
	}{
		{
			name:  "basic null",
			input: "\r\n",
			want:  &Value{Type: TypeNull, Value: nil},
		},
		{
			name:  "null with content before newline",
			input: "ignored content\r\n",
			want:  &Value{Type: TypeNull, Value: nil},
		},
		{
			name:    "no newline",
			input:   "test",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(strings.NewReader(tt.input))
			got, err := parseNull(reader)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestParseBoolean(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    *Value
		wantErr bool
	}{
		{
			name:  "true",
			input: "t\r\n",
			want:  &Value{Type: TypeBoolean, Value: true},
		},
		{
			name:  "false",
			input: "f\r\n",
			want:  &Value{Type: TypeBoolean, Value: false},
		},
		{
			name:    "invalid boolean",
			input:   "y\r\n",
			wantErr: true,
		},
		{
			name:    "true spelled out",
			input:   "true\r\n",
			wantErr: true,
		},
		{
			name:    "false spelled out",
			input:   "false\r\n",
			wantErr: true,
		},
		{
			name:    "no newline",
			input:   "t",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(strings.NewReader(tt.input))
			got, err := parseBoolean(reader)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestParseDouble(t *testing.T) {
	compareDoubleValues := func(a, b *Value) bool {
		if a == nil && b == nil {
			return true
		}
		if a == nil || b == nil {
			return false
		}
		if a.Type != b.Type {
			return false
		}

		// special handling for NaN values
		aVal, aOk := a.Value.(float64)
		bVal, bOk := b.Value.(float64)
		if aOk && bOk {
			if math.IsNaN(aVal) && math.IsNaN(bVal) {
				return true
			}
			return aVal == bVal
		}

		return false
	}

	tests := []struct {
		name    string
		input   string
		want    *Value
		wantErr bool
	}{
		{
			name:  "positive double",
			input: "1.23\r\n",
			want:  &Value{Type: TypeDouble, Value: 1.23},
		},
		{
			name:  "negative double",
			input: "-1.23\r\n",
			want:  &Value{Type: TypeDouble, Value: -1.23},
		},
		{
			name:  "zero",
			input: "0.0\r\n",
			want:  &Value{Type: TypeDouble, Value: 0.0},
		},
		{
			name:  "integer as double",
			input: "42\r\n",
			want:  &Value{Type: TypeDouble, Value: 42.0},
		},
		{
			name:  "positive infinity",
			input: "inf\r\n",
			want:  &Value{Type: TypeDouble, Value: math.Inf(1)},
		},
		{
			name:  "negative infinity",
			input: "-inf\r\n",
			want:  &Value{Type: TypeDouble, Value: math.Inf(-1)},
		},
		{
			name:  "NaN",
			input: "nan\r\n",
			want:  &Value{Type: TypeDouble, Value: math.NaN()},
		},
		{
			name:  "scientific notation",
			input: "1.23e-4\r\n",
			want:  &Value{Type: TypeDouble, Value: 1.23e-4},
		},
		{
			name:    "invalid double",
			input:   "abc\r\n",
			wantErr: true,
		},
		{
			name:    "no newline",
			input:   "1.23",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(strings.NewReader(tt.input))
			got, err := parseDouble(reader)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.True(t, compareDoubleValues(got, tt.want), "parseDouble() = %+v, want %+v", got, tt.want)
		})
	}
}

func TestParseBigNumber(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    *Value
		wantErr bool
	}{
		{
			name:  "large positive number",
			input: "3492890328409238509324850943850943825024385\r\n",
			want:  &Value{Type: TypeBigNumber, Value: "3492890328409238509324850943850943825024385"},
		},
		{
			name:  "large negative number",
			input: "-3492890328409238509324850943850943825024385\r\n",
			want:  &Value{Type: TypeBigNumber, Value: "-3492890328409238509324850943850943825024385"},
		},
		{
			name:  "regular integer as big number",
			input: "123\r\n",
			want:  &Value{Type: TypeBigNumber, Value: "123"},
		},
		{
			name:  "zero as big number",
			input: "0\r\n",
			want:  &Value{Type: TypeBigNumber, Value: "0"},
		},
		{
			name:    "no newline",
			input:   "123456789",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(strings.NewReader(tt.input))
			got, err := parseBigNumber(reader)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestParseBulkError(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    *Value
		wantErr bool
	}{
		{
			name:  "basic bulk error",
			input: "21\r\nSYNTAX invalid syntax\r\n",
			want:  &Value{Type: TypeBulkError, Value: "SYNTAX invalid syntax"},
		},
		{
			name:  "empty bulk error",
			input: "0\r\n\r\n",
			want:  &Value{Type: TypeBulkError, Value: ""},
		},
		{
			name:  "bulk error with special chars",
			input: "9\r\nERR\r\ntest\r\n",
			want:  &Value{Type: TypeBulkError, Value: "ERR\r\ntest"},
		},
		{
			name:    "invalid length",
			input:   "abc\r\nerror\r\n",
			wantErr: true,
		},
		{
			name:    "negative length",
			input:   "-1\r\n",
			wantErr: true,
		},
		{
			name:    "insufficient data",
			input:   "10\r\nshort\r\n",
			wantErr: true,
		},
		{
			name:    "no length line",
			input:   "error\r\n",
			wantErr: true,
		},
		{
			name:    "no newline",
			input:   "3\r\nERR",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(strings.NewReader(tt.input))
			got, err := parseBulkError(reader)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestParseVerbatimString(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    *Value
		wantErr bool
	}{
		{
			name:  "basic verbatim string",
			input: "15\r\ntxt:Some string\r\n",
			want:  &Value{Type: TypeVerbatimString, Value: "txt:Some string"},
		},
		{
			name:  "verbatim string with markdown",
			input: "11\r\nmkd:# hello\r\n",
			want:  &Value{Type: TypeVerbatimString, Value: "mkd:# hello"},
		},
		{
			name:  "minimum length verbatim string",
			input: "4\r\ntxt:\r\n",
			want:  &Value{Type: TypeVerbatimString, Value: "txt:"},
		},
		{
			name:    "too short length",
			input:   "3\r\ntxt\r\n",
			wantErr: true,
		},
		{
			name:    "invalid length",
			input:   "abc\r\ntxt:test\r\n",
			wantErr: true,
		},
		{
			name:    "insufficient data",
			input:   "10\r\nshort\r\n",
			wantErr: true,
		},
		{
			name:    "no length line",
			input:   "txt:test\r\n",
			wantErr: true,
		},
		{
			name:    "no newline",
			input:   "4\r\ntxt:",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(strings.NewReader(tt.input))
			got, err := parseVerbatimString(reader)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestParseMap(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    *Value
		wantErr bool
	}{
		{
			name:  "empty map",
			input: "0\r\n",
			want:  &Value{Type: TypeMap, Value: []Value{}},
		},
		{
			name:  "single key-value pair",
			input: "1\r\n+first\r\n:1\r\n",
			want: &Value{Type: TypeMap, Value: []Value{
				{Type: TypeSimpleString, Value: "first"},
				{Type: TypeInteger, Value: int64(1)},
			}},
		},
		{
			name:  "multiple key-value pairs",
			input: "2\r\n+first\r\n:1\r\n+second\r\n:2\r\n",
			want: &Value{Type: TypeMap, Value: []Value{
				{Type: TypeSimpleString, Value: "first"},
				{Type: TypeInteger, Value: int64(1)},
				{Type: TypeSimpleString, Value: "second"},
				{Type: TypeInteger, Value: int64(2)},
			}},
		},
		{
			name:    "invalid length",
			input:   "abc\r\n",
			wantErr: true,
		},
		{
			name:    "negative length",
			input:   "-1\r\n",
			wantErr: true,
		},
		{
			name:    "insufficient pairs",
			input:   "1\r\n+key\r\n",
			wantErr: true,
		},
		{
			name:    "invalid key",
			input:   "1\r\n?invalid\r\n:1\r\n",
			wantErr: true,
		},
		{
			name:    "no newline",
			input:   "1\r\n+first\r\n:1",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(strings.NewReader(tt.input))
			got, err := parseMap(reader)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestParseAttribute(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    *Value
		wantErr bool
	}{
		{
			name:  "empty attribute",
			input: "0\r\n",
			want:  &Value{Type: TypeAttribute, Value: []Value{}},
		},
		{
			name:  "single attribute pair",
			input: "1\r\n+key\r\n+value\r\n",
			want: &Value{Type: TypeAttribute, Value: []Value{
				{Type: TypeSimpleString, Value: "key"},
				{Type: TypeSimpleString, Value: "value"},
			}},
		},
		{
			name:  "multiple attribute pairs",
			input: "2\r\n+ttl\r\n:3600\r\n+encoding\r\n+utf8\r\n",
			want: &Value{Type: TypeAttribute, Value: []Value{
				{Type: TypeSimpleString, Value: "ttl"},
				{Type: TypeInteger, Value: int64(3600)},
				{Type: TypeSimpleString, Value: "encoding"},
				{Type: TypeSimpleString, Value: "utf8"},
			}},
		},
		{
			name:    "invalid length",
			input:   "abc\r\n",
			wantErr: true,
		},
		{
			name:    "negative length",
			input:   "-1\r\n",
			wantErr: true,
		},
		{
			name:    "insufficient pairs",
			input:   "1\r\n+key\r\n",
			wantErr: true,
		},
		{
			name:    "invalid attribute",
			input:   "1\r\n?invalid\r\n+value\r\n",
			wantErr: true,
		},
		{
			name:    "no newline",
			input:   "1\r\n+key\r\n+value",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(strings.NewReader(tt.input))
			got, err := parseAttribute(reader)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestParseSet(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    *Value
		wantErr bool
	}{
		{
			name:  "empty set",
			input: "0\r\n",
			want:  &Value{Type: TypeSet, Value: []Value{}},
		},
		{
			name:  "set with integers",
			input: "3\r\n:1\r\n:2\r\n:3\r\n",
			want: &Value{Type: TypeSet, Value: []Value{
				{Type: TypeInteger, Value: int64(1)},
				{Type: TypeInteger, Value: int64(2)},
				{Type: TypeInteger, Value: int64(3)},
			}},
		},
		{
			name:  "mixed set",
			input: "3\r\n+hello\r\n:123\r\n$5\r\nworld\r\n",
			want: &Value{Type: TypeSet, Value: []Value{
				{Type: TypeSimpleString, Value: "hello"},
				{Type: TypeInteger, Value: int64(123)},
				{Type: TypeBulkString, Value: "world"},
			}},
		},
		{
			name:    "invalid length",
			input:   "abc\r\n",
			wantErr: true,
		},
		{
			name:    "negative length",
			input:   "-1\r\n",
			wantErr: true,
		},
		{
			name:    "insufficient elements",
			input:   "2\r\n:1\r\n",
			wantErr: true,
		},
		{
			name:    "invalid element",
			input:   "1\r\n?invalid\r\n",
			wantErr: true,
		},
		{
			name:    "no newline",
			input:   "3\r\n:1\r\n:2\r\n:3",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(strings.NewReader(tt.input))
			got, err := parseSet(reader)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestParsePush(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    *Value
		wantErr bool
	}{
		{
			name:  "empty push",
			input: "0\r\n",
			want:  &Value{Type: TypePush, Value: []Value{}},
		},
		{
			name:  "push with message",
			input: "2\r\n+subscribe\r\n+mychannel\r\n",
			want: &Value{Type: TypePush, Value: []Value{
				{Type: TypeSimpleString, Value: "subscribe"},
				{Type: TypeSimpleString, Value: "mychannel"},
			}},
		},
		{
			name:  "push with mixed types",
			input: "3\r\n+message\r\n+channel\r\n$5\r\nhello\r\n",
			want: &Value{Type: TypePush, Value: []Value{
				{Type: TypeSimpleString, Value: "message"},
				{Type: TypeSimpleString, Value: "channel"},
				{Type: TypeBulkString, Value: "hello"},
			}},
		},
		{
			name:    "invalid length",
			input:   "abc\r\n",
			wantErr: true,
		},
		{
			name:    "negative length",
			input:   "-1\r\n",
			wantErr: true,
		},
		{
			name:    "insufficient elements",
			input:   "2\r\n+message\r\n",
			wantErr: true,
		},
		{
			name:    "invalid element",
			input:   "1\r\n?invalid\r\n",
			wantErr: true,
		},
		{
			name:    "no newline",
			input:   "1\r\n+hello",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(strings.NewReader(tt.input))
			got, err := parsePush(reader)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestStringLimits(t *testing.T) {
	t.Run("large array rejected", func(t *testing.T) {
		// array larger than MaxArrayLength
		input := "*1048577\r\n"
		reader := bufio.NewReader(strings.NewReader(input))

		_, err := ParseRESP3Value(reader)
		require.Error(t, err)
		require.Contains(t, err.Error(), "exceeds maximum")
	})

	t.Run("large bulk string rejected", func(t *testing.T) {
		// bulk string larger than MaxStringLength
		input := "$67108865\r\n"
		reader := bufio.NewReader(strings.NewReader(input))

		_, err := ParseRESP3Value(reader)
		require.Error(t, err)
		require.Contains(t, err.Error(), "exceeds maximum")
	})

	t.Run("large map rejected", func(t *testing.T) {
		// map larger than MaxArrayLength/2
		input := "%524289\r\n"
		reader := bufio.NewReader(strings.NewReader(input))

		_, err := ParseRESP3Value(reader)
		require.Error(t, err)
		require.Contains(t, err.Error(), "exceeds maximum")
	})

	t.Run("large set rejected", func(t *testing.T) {
		// set larger than MaxArrayLength
		input := "~1048577\r\n"
		reader := bufio.NewReader(strings.NewReader(input))

		_, err := ParseRESP3Value(reader)
		require.Error(t, err)
		require.Contains(t, err.Error(), "exceeds maximum")
	})

	t.Run("large bulk error rejected", func(t *testing.T) {
		// bulk error larger than MaxStringLength
		input := "!67108865\r\n"
		reader := bufio.NewReader(strings.NewReader(input))

		_, err := ParseRESP3Value(reader)
		require.Error(t, err)
		require.Contains(t, err.Error(), "exceeds maximum")
	})

	t.Run("large verbatim string rejected", func(t *testing.T) {
		// verbatim string larger than MaxStringLength
		input := "=67108865\r\n"
		reader := bufio.NewReader(strings.NewReader(input))

		_, err := ParseRESP3Value(reader)
		require.Error(t, err)
		require.Contains(t, err.Error(), "exceeds maximum")
	})

	t.Run("large attribute rejected", func(t *testing.T) {
		// attribute larger than MaxArrayLength/2
		input := "|524289\r\n"
		reader := bufio.NewReader(strings.NewReader(input))

		_, err := ParseRESP3Value(reader)
		require.Error(t, err)
		require.Contains(t, err.Error(), "exceeds maximum")
	})

	t.Run("large push rejected", func(t *testing.T) {
		// push larger than MaxArrayLength
		input := ">1048577\r\n"
		reader := bufio.NewReader(strings.NewReader(input))

		_, err := ParseRESP3Value(reader)
		require.Error(t, err)
		require.Contains(t, err.Error(), "exceeds maximum")
	})

	t.Run("reasonable sizes accepted", func(t *testing.T) {
		// Test that reasonable sizes work
		t.Run("normal array", func(t *testing.T) {
			input := "*3\r\n:1\r\n:2\r\n:3\r\n"
			reader := bufio.NewReader(strings.NewReader(input))

			val, err := ParseRESP3Value(reader)
			require.NoError(t, err)
			require.Equal(t, TypeArray, val.Type)
			elems := val.Value.([]Value) // nolint:errcheck
			require.Len(t, elems, 3)
		})

		t.Run("normal bulk string", func(t *testing.T) {
			input := "$5\r\nhello\r\n"
			reader := bufio.NewReader(strings.NewReader(input))

			val, err := ParseRESP3Value(reader)
			require.NoError(t, err)
			require.Equal(t, TypeBulkString, val.Type)
			require.Equal(t, "hello", val.Value)
		})
	})
}

func FuzzParseCommand(f *testing.F) {
	f.Add("*2\r\n+GET\r\n+key\r\n")                    // simple string command
	f.Add("*2\r\n$3\r\nSET\r\n$5\r\nvalue\r\n")        // bulk string command
	f.Add("*3\r\n$4\r\nHSET\r\n$3\r\nkey\r\n:123\r\n") // mixed types
	f.Add("*1\r\n+PING\r\n")                           // single command
	f.Add("*0\r\n")                                    // empty array (should fail)
	f.Add("*-1\r\n")                                   // null array (should fail)
	f.Add("+OK\r\n")                                   // not an array (should fail)
	f.Add("*1\r\n:123\r\n")                            // invalid command name type
	f.Add("invalid")                                   // completely invalid input
	f.Add("")                                          // empty input

	f.Fuzz(func(t *testing.T, input string) {
		reader := bufio.NewReader(strings.NewReader(input))
		cmd, err := ParseCommand(reader)
		if err != nil {
			require.NotEmpty(t, err.Error(), "error message should not be empty")
			return
		}

		require.NotNil(t, cmd, "command should not be nil when parsing succeeds")
		require.NotEmpty(t, cmd.Name, "command name should not be empty")
		require.Equal(t, strings.ToUpper(cmd.Name), cmd.Name, "command name should be uppercase")
		require.NotNil(t, cmd.Args, "command args should not be nil when parsing succeeds")
	})
}

func FuzzParseRESP3Value(f *testing.F) {
	f.Add("+OK\r\n")                             // simple string
	f.Add("-ERR message\r\n")                    // simple error
	f.Add(":123\r\n")                            // integer
	f.Add("$5\r\nhello\r\n")                     // bulk string
	f.Add("$-1\r\n")                             // null bulk string
	f.Add("*2\r\n+hello\r\n:123\r\n")            // array
	f.Add("*-1\r\n")                             // null array
	f.Add("_\r\n")                               // null
	f.Add("#t\r\n")                              // boolean true
	f.Add("#f\r\n")                              // boolean false
	f.Add(",1.23\r\n")                           // double
	f.Add(",inf\r\n")                            // infinity
	f.Add(",nan\r\n")                            // naN
	f.Add("(123456789012345678901234567890\r\n") // big number
	f.Add("!5\r\nerror\r\n")                     // bulk error
	f.Add("=15\r\ntxt:Some string\r\n")          // verbatim string
	f.Add("%1\r\n+key\r\n+value\r\n")            // map
	f.Add("|1\r\n+key\r\n+value\r\n")            // attribute
	f.Add("~2\r\n+a\r\n+b\r\n")                  // set
	f.Add(">2\r\n+message\r\n+data\r\n")         // push
	f.Add("?unknown\r\n")                        // unknown type
	f.Add("")                                    // empty
	f.Add("incomplete")                          // incomplete

	validTypes := []Type{
		TypeSimpleString, TypeSimpleError, TypeInteger, TypeBulkString,
		TypeArray, TypeNull, TypeBoolean, TypeDouble, TypeBigNumber,
		TypeBulkError, TypeVerbatimString, TypeMap, TypeAttribute,
		TypeSet, TypePush,
	}

	f.Fuzz(func(t *testing.T, input string) {
		reader := bufio.NewReader(strings.NewReader(input))
		value, err := ParseRESP3Value(reader)
		if err != nil {
			require.NotEmpty(t, err.Error(), "error message should not be empty")
			return
		}

		require.NotNil(t, value, "value should not be nil when parsing succeeds")
		require.True(t, slices.Contains(validTypes, value.Type), "parsed value should have a valid type, got: %v", value.Type)
	})
}

func FuzzReadLine(f *testing.F) {
	f.Add("hello\r\n")
	f.Add("hello\n")
	f.Add("\r\n")
	f.Add("\n")
	f.Add("hello world\r\n")
	f.Add("special chars: !@#$%^&*()\r\n")
	f.Add("unicode: 🚀🌟\r\n")
	f.Add("no newline")
	f.Add("")
	f.Add("line with\rcarriage return\r\n")

	f.Fuzz(func(t *testing.T, input string) {
		reader := bufio.NewReader(strings.NewReader(input))
		result, err := readLine(reader)
		if err != nil {
			require.NotEmpty(t, err.Error(), "error message should not be empty")
			return
		}

		require.NotNil(t, result, "result should not be nil when readLine succeeds")
		require.False(t, strings.HasSuffix(result, "\r\n"), "result should not end with \\r\\n")
	})
}
