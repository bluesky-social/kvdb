package resp

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
)

const (
	MaxArrayLength  = 1024 * 1024      // 1M elements max
	MaxStringLength = 64 * 1024 * 1024 // 64MB max
	MaxNestingDepth = 64               // Maximum recursion depth
)

type Command struct {
	Name string
	Args []Value
}

type Value struct {
	Type  Type
	Value any
}

type Type int16

const (
	TypeSimpleString   Type = 1 << iota // +
	TypeSimpleError                     // -
	TypeInteger                         // :
	TypeBulkString                      // $
	TypeArray                           // *
	TypeNull                            // _
	TypeBoolean                         // #
	TypeDouble                          // ,
	TypeBigNumber                       // (
	TypeBulkError                       // !
	TypeVerbatimString                  // =
	TypeMap                             // %
	TypeAttribute                       // |
	TypeSet                             // ~
	TypePush                            // >
)

func ParseCommand(reader *bufio.Reader) (*Command, error) {
	val, err := ParseRESP3Value(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to parse resp3 value: %w", err)
	}

	if val.Type != TypeArray {
		return nil, fmt.Errorf("redis commands must be arrays")
	}

	args, ok := val.Value.([]Value)
	if !ok || len(args) == 0 {
		return nil, fmt.Errorf("empty command")
	}

	switch args[0].Type {
	case TypeSimpleString, TypeBulkString: // ok
	default:
		return nil, fmt.Errorf("command name must be a simple or bulk string")
	}

	name, ok := args[0].Value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string command name, got %T", args[0].Value)
	}
	if name == "" {
		return nil, fmt.Errorf("empty command")
	}

	return &Command{
		Name: strings.ToUpper(name),
		Args: args[1:],
	}, nil
}

func ParseRESP3Value(reader *bufio.Reader) (*Value, error) {
	typ, err := reader.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("failed to read type byte: %w", err)
	}

	switch typ {
	case '+':
		return parseSimpleString(reader)
	case '-':
		return parseSimpleError(reader)
	case ':':
		return parseInteger(reader)
	case '$':
		return parseBulkString(reader)
	case '*':
		return parseArray(reader)
	case '_':
		return parseNull(reader)
	case '#':
		return parseBoolean(reader)
	case ',':
		return parseDouble(reader)
	case '(':
		return parseBigNumber(reader)
	case '!':
		return parseBulkError(reader)
	case '=':
		return parseVerbatimString(reader)
	case '%':
		return parseMap(reader)
	case '|':
		return parseAttribute(reader)
	case '~':
		return parseSet(reader)
	case '>':
		return parsePush(reader)
	default:
		return nil, fmt.Errorf("unknown resp3 type: %c", typ)
	}
}

// Reads a line and trims any trailing newline characters
func readLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	return strings.TrimSuffix(line, "\r\n"), nil
}

func parseSimpleString(reader *bufio.Reader) (*Value, error) {
	val, err := readLine(reader)
	if err != nil {
		return nil, err
	}

	return &Value{Type: TypeSimpleString, Value: val}, nil
}

func parseSimpleError(reader *bufio.Reader) (*Value, error) {
	val, err := readLine(reader)
	if err != nil {
		return nil, err
	}
	return &Value{Type: TypeSimpleError, Value: val}, nil
}

func parseInteger(reader *bufio.Reader) (*Value, error) {
	line, err := readLine(reader)
	if err != nil {
		return nil, err
	}

	value, err := strconv.ParseInt(line, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid integer: %s", line)
	}

	return &Value{Type: TypeInteger, Value: value}, nil
}

func parseBulkString(reader *bufio.Reader) (*Value, error) {
	line, err := readLine(reader)
	if err != nil {
		return nil, err
	}

	length, err := strconv.Atoi(line)
	if err != nil {
		return nil, fmt.Errorf("invalid bulk string length: %s", line)
	}

	if length == -1 {
		// null bulk string
		return &Value{Type: TypeBulkString, Value: nil}, nil
	}
	if length < 0 {
		return nil, fmt.Errorf("invalid bulk string length: %d", length)
	}
	if length > MaxStringLength {
		return nil, fmt.Errorf("bulk string length %d exceeds maximum %d", length, MaxStringLength)
	}

	data := make([]byte, length+2) // +2 for \r\n
	if _, err := io.ReadFull(reader, data); err != nil {
		return nil, err
	}

	return &Value{Type: TypeBulkString, Value: string(data[:length])}, nil
}

func parseArray(reader *bufio.Reader) (*Value, error) {
	line, err := readLine(reader)
	if err != nil {
		return nil, err
	}

	length, err := strconv.Atoi(line)
	if err != nil {
		return nil, fmt.Errorf("invalid array length: %s", line)
	}

	if length == -1 {
		// null array
		return &Value{Type: TypeArray, Value: nil}, nil
	}
	if length < 0 {
		return nil, fmt.Errorf("invalid array length: %d", length)
	}
	if length > MaxArrayLength {
		return nil, fmt.Errorf("array length %d exceeds maximum %d", length, MaxArrayLength)
	}

	elems := make([]Value, length)
	for ndx := range length {
		elem, err := ParseRESP3Value(reader)
		if err != nil {
			return nil, err
		}
		elems[ndx] = *elem
	}

	return &Value{Type: TypeArray, Value: elems}, nil
}

func parseNull(reader *bufio.Reader) (*Value, error) {
	// consume the \r\n but ignore content
	_, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	return &Value{Type: TypeNull, Value: nil}, nil
}

func parseBoolean(reader *bufio.Reader) (*Value, error) {
	line, err := readLine(reader)
	if err != nil {
		return nil, err
	}

	switch line {
	case "t":
		return &Value{Type: TypeBoolean, Value: true}, nil
	case "f":
		return &Value{Type: TypeBoolean, Value: false}, nil
	default:
		return nil, fmt.Errorf("invalid boolean value: %s", line)
	}
}

func parseDouble(reader *bufio.Reader) (*Value, error) {
	line, err := readLine(reader)
	if err != nil {
		return nil, err
	}

	// handle special values
	switch line {
	case "inf":
		return &Value{Type: TypeDouble, Value: math.Inf(1)}, nil
	case "-inf":
		return &Value{Type: TypeDouble, Value: math.Inf(-1)}, nil
	case "nan":
		return &Value{Type: TypeDouble, Value: math.NaN()}, nil
	}

	value, err := strconv.ParseFloat(line, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid double: %s", line)
	}

	return &Value{Type: TypeDouble, Value: value}, nil
}

func parseBigNumber(reader *bufio.Reader) (*Value, error) {
	line, err := readLine(reader)
	if err != nil {
		return nil, err
	}

	// store as string since Go's int64 might not be sufficient
	return &Value{Type: TypeBigNumber, Value: line}, nil
}

func parseBulkError(reader *bufio.Reader) (*Value, error) {
	line, err := readLine(reader)
	if err != nil {
		return nil, err
	}

	length, err := strconv.Atoi(line)
	if err != nil {
		return nil, fmt.Errorf("invalid bulk error length: %s", line)
	}
	if length < 0 {
		return nil, fmt.Errorf("invalid bulk error length: %d", length)
	}
	if length > MaxStringLength {
		return nil, fmt.Errorf("bulk error length %d exceeds maximum %d", length, MaxStringLength)
	}

	data := make([]byte, length+2) // +2 for \r\n
	if _, err := io.ReadFull(reader, data); err != nil {
		return nil, err
	}

	return &Value{Type: TypeBulkError, Value: string(data[:length])}, nil
}

func parseVerbatimString(reader *bufio.Reader) (*Value, error) {
	line, err := readLine(reader)
	if err != nil {
		return nil, err
	}

	length, err := strconv.Atoi(line)
	if err != nil {
		return nil, fmt.Errorf("invalid verbatim string length: %s", line)
	}
	if length < 4 {
		return nil, fmt.Errorf("verbatim string too short: %d", length)
	}
	if length > MaxStringLength {
		return nil, fmt.Errorf("verbatim string length %d exceeds maximum %d", length, MaxStringLength)
	}

	data := make([]byte, length+2) // +2 for \r\n
	if _, err := io.ReadFull(reader, data); err != nil {
		return nil, err
	}

	content := string(data[:length])
	return &Value{Type: TypeVerbatimString, Value: content}, nil
}

func parseMap(reader *bufio.Reader) (*Value, error) {
	line, err := readLine(reader)
	if err != nil {
		return nil, err
	}

	length, err := strconv.Atoi(line)
	if err != nil {
		return nil, fmt.Errorf("invalid map length: %s", line)
	}

	if length < 0 {
		return nil, fmt.Errorf("invalid map length: %d", length)
	}
	if length > MaxArrayLength/2 {
		return nil, fmt.Errorf("map length %d exceeds maximum %d", length, MaxArrayLength/2)
	}

	elems := make([]Value, length*2) // key-value pairs
	for i := 0; i < length*2; i++ {
		elem, err := ParseRESP3Value(reader)
		if err != nil {
			return nil, err
		}
		elems[i] = *elem
	}

	return &Value{Type: TypeMap, Value: elems}, nil
}

func parseAttribute(reader *bufio.Reader) (*Value, error) {
	line, err := readLine(reader)
	if err != nil {
		return nil, err
	}

	length, err := strconv.Atoi(line)
	if err != nil {
		return nil, fmt.Errorf("invalid attribute length: %s", line)
	}

	if length < 0 {
		return nil, fmt.Errorf("invalid attribute length: %d", length)
	}
	if length > MaxArrayLength/2 {
		return nil, fmt.Errorf("attribute length %d exceeds maximum %d", length, MaxArrayLength/2)
	}

	elems := make([]Value, length*2) // key-value pairs
	for i := 0; i < length*2; i++ {
		elem, err := ParseRESP3Value(reader)
		if err != nil {
			return nil, err
		}
		elems[i] = *elem
	}

	return &Value{Type: TypeAttribute, Value: elems}, nil
}

func parseSet(reader *bufio.Reader) (*Value, error) {
	line, err := readLine(reader)
	if err != nil {
		return nil, err
	}

	length, err := strconv.Atoi(line)
	if err != nil {
		return nil, fmt.Errorf("invalid set length: %s", line)
	}

	if length < 0 {
		return nil, fmt.Errorf("invalid set length: %d", length)
	}
	if length > MaxArrayLength {
		return nil, fmt.Errorf("set length %d exceeds maximum %d", length, MaxArrayLength)
	}

	elems := make([]Value, length)
	for ndx := range length {
		elem, err := ParseRESP3Value(reader)
		if err != nil {
			return nil, err
		}
		elems[ndx] = *elem
	}

	return &Value{Type: TypeSet, Value: elems}, nil
}

func parsePush(reader *bufio.Reader) (*Value, error) {
	line, err := readLine(reader)
	if err != nil {
		return nil, err
	}

	length, err := strconv.Atoi(line)
	if err != nil {
		return nil, fmt.Errorf("invalid push length: %s", line)
	}
	if length < 0 {
		return nil, fmt.Errorf("invalid push length: %d", length)
	}
	if length > MaxArrayLength {
		return nil, fmt.Errorf("push length %d exceeds maximum %d", length, MaxArrayLength)
	}

	elems := make([]Value, length)
	for ndx := range length {
		elem, err := ParseRESP3Value(reader)
		if err != nil {
			return nil, err
		}
		elems[ndx] = *elem
	}

	return &Value{Type: TypePush, Value: elems}, nil
}
