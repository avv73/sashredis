package marshal_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/codecrafters-io/redis-starter-go/app/marshal"
	"github.com/codecrafters-io/redis-starter-go/app/types"
)

func TestParseCommand(t *testing.T) {
	tests := []struct {
		name           string
		command        string
		expected       *types.Command
		expectedErrStr string
	}{
		{
			name:    "ping bstring",
			command: "*1\r\n$4\r\nPING\r\n",
			expected: &types.Command{
				Command: types.Ping,
				Args:    []*types.RedisData{},
			},
		},
		{
			name:    "ping sstring",
			command: "*1\r\n+PING\r\n",
			expected: &types.Command{
				Command: types.Ping,
				Args:    []*types.RedisData{},
			},
		},
	}

	for _, tt := range tests {
		commandAsBytes := []byte(tt.command)

		parser := marshal.NewParser()
		result, err := parser.ParseCommand(commandAsBytes)

		if tt.expectedErrStr != "" {
			assert.Nil(t, result)
			assert.ErrorContains(t, err, tt.expectedErrStr)
		} else {
			assert.Equal(t, tt.expected, result)
			assert.NoError(t, err)
		}

	}
}
