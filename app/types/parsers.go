package types

import (
	"errors"
	"strconv"
	"strings"
)

// (millisecondsTime, sequenceNum)
func ParseStreamEntryKey(entryKey string, strict bool) (*int64, *int, error) {
	if entryKey == "*" {
		return nil, nil, nil
	}
	tokens := strings.Split(entryKey, "-")
	if len(tokens) != 2 && strict {
		return nil, nil, errors.New("expected entry key to be in format {milliseconds}-{sequenceNum}")
	}

	millisecondsTime, err := strconv.ParseInt(tokens[0], 10, 64)
	if err != nil {
		return nil, nil, errors.New("expected milliseconds to be valid int")
	}

	if millisecondsTime < 0 {
		return nil, nil, errors.New("expected milliseconds to be non-negative")
	}

	if len(tokens) != 2 {
		return &millisecondsTime, nil, nil
	}

	seqNumber, err := strconv.Atoi(tokens[1])
	if err != nil {
		if tokens[1] == "*" {
			return &millisecondsTime, nil, nil
		}
		return nil, nil, errors.New("expected sequenceNum to be valid int")
	}

	if seqNumber < 0 {
		return nil, nil, errors.New("expected sequenceNum to be non-negative")
	}

	return &millisecondsTime, &seqNumber, nil
}
