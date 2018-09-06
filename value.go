package athena

import (
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/service/athena"
)

const (
	// TimestampLayout is the Go time layout string for an Athena `timestamp`.
	TimestampLayout = "2006-01-02 15:04:05.999"
)

func convertRow(columns []*athena.ColumnInfo, in []*athena.Datum, ret []driver.Value) error {
	for i, val := range in {
		coerced, err := convertValue(*columns[i].Type, val.VarCharValue)
		if err != nil {
			return err
		}

		ret[i] = coerced
	}

	return nil
}

func convertValue(athenaType string, rawValue *string) (interface{}, error) {
	if rawValue == nil {
		return nil, nil
	}

	val := *rawValue
	switch athenaType {
	case "tinyint":
		return strconv.ParseInt(val, 10, 8)
	case "smallint":
		return strconv.ParseInt(val, 10, 16)
	case "integer":
		return strconv.ParseInt(val, 10, 32)
	case "bigint":
		return strconv.ParseInt(val, 10, 64)
	case "boolean":
		switch val {
		case "true":
			return true, nil
		case "false":
			return false, nil
		}
		return nil, fmt.Errorf("cannot parse '%s' as boolean", val)
	case "float":
		return strconv.ParseFloat(val, 32)
	case "double":
		return strconv.ParseFloat(val, 64)
	case "varchar", "string":
		return val, nil
	case "varbinary":
		arr := strings.Split(val, " ")
		dst := make([]byte, 1)
		ret := make([]byte, len(arr))
		for i, v := range arr {
			src := []byte(v)
			if len(src) != 2 {
				return nil, fmt.Errorf("unexpected byte length %d", len(src))
			}
			n, err := hex.Decode(dst, src)
			if err != nil {
				return nil, err
			}
			if n != 1 {
				return nil, fmt.Errorf("unexpected byte length %d", n)
			}
			ret[i] = dst[0]
		}
		return ret, nil
	case "timestamp":
		return time.Parse(TimestampLayout, val)
	default:
		return nil, fmt.Errorf("unknown type `%s` with value %s", athenaType, val)
	}
}
