package replica

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/types"
	log "github.com/sirupsen/logrus"
)

type DataEncoder interface {
	Encode(input *types.RedisData) ([]byte, error)
}

type Client struct {
	address string
	conn    net.Conn
	dialer  net.Dialer
	encoder DataEncoder
}

// Add data encoder only if we really need it for some other commands and stuff.
func NewClient(encoder DataEncoder) *Client {
	return &Client{
		encoder: encoder,
	}
}

func (c *Client) Connect(ctx context.Context, host string, port int) error {
	c.address = fmt.Sprintf("%s:%d", host, port)

	var err error
	c.conn, err = c.dialer.DialContext(ctx, "tcp", c.address)
	if err != nil {
		return fmt.Errorf("failed to dial master: %w", err)
	}

	log.Info("dial successful to master")
	return nil
}

var pingCommand []byte = []byte("*1\r\n$4\r\nPING\r\n")
var pingResult []byte = []byte("+PONG\r\n")

func (c *Client) Ping(ctx context.Context) error {
	if c.conn == nil {
		return errors.New("no connection in progress; connect first")
	}

	err := c.writeToMaster(ctx, pingCommand)
	if err != nil {
		return fmt.Errorf("writing to master failed: %w", err)
	}

	return c.verifyMasterResponse(ctx, pingResult)
}

var replConfResult []byte = []byte("+OK\r\n")

func (c *Client) ReplConfListeningPort(ctx context.Context, port int) error {
	if c.conn == nil {
		return errors.New("no connection in progress; connect first")
	}

	configListeningPortCommand := types.ToCommandRedisData("REPLCONF", "listening-port", strconv.Itoa(port))
	commandBytes, err := c.encoder.Encode(configListeningPortCommand)
	if err != nil {
		return fmt.Errorf("internal encoding error: %w", err)
	}

	err = c.writeToMaster(ctx, commandBytes)
	if err != nil {
		return fmt.Errorf("writing to master failed: %w", err)
	}

	return c.verifyMasterResponse(ctx, replConfResult)
}

var replConfCapaCommand []byte = []byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")

func (c *Client) ReplConfCapa(ctx context.Context) error {
	if c.conn == nil {
		return errors.New("no connection in progress; connect first")
	}

	err := c.writeToMaster(ctx, replConfCapaCommand)
	if err != nil {
		return fmt.Errorf("writing to master failed: %w", err)
	}

	return c.verifyMasterResponse(ctx, replConfResult)
}

func (c *Client) Psync(ctx context.Context, replicationId string, offset string) error {
	if c.conn == nil {
		return errors.New("no connection in progress; connect first")
	}

	psyncCommand := types.ToCommandRedisData("PSYNC", replicationId, offset)
	commandBytes, err := c.encoder.Encode(psyncCommand)
	if err != nil {
		return fmt.Errorf("internal encoding error: %w", err)
	}

	err = c.writeToMaster(ctx, commandBytes)
	if err != nil {
		return fmt.Errorf("writing to master failed: %w", err)
	}

	expectedResponse := fmt.Appendf(make([]byte, 0), "+FULLRESYNC %s 0\r\n", replicationId)
	return c.verifyMasterResponse(ctx, expectedResponse)
}

func (c *Client) verifyMasterResponse(ctx context.Context, expectedResponse []byte) error {
	output := make([]byte, len(expectedResponse))
	err := c.readFromMaster(ctx, output)
	if err != nil {
		return fmt.Errorf("reading from master failed: %w", err)
	}

	if !bytes.Equal(output, expectedResponse) {
		return fmt.Errorf("expected a %s response from master, got: %s", string(expectedResponse), string(output))
	}

	return nil
}

func (c *Client) writeToMaster(ctx context.Context, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	stop := context.AfterFunc(ctx, func() {
		c.conn.SetWriteDeadline(time.Unix(0, 1))
	})
	defer stop()

	if dl, ok := ctx.Deadline(); ok {
		c.conn.SetWriteDeadline(dl)
	} else {
		c.conn.SetWriteDeadline(time.Time{})
	}

	_, err := c.conn.Write(data)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return err
}

func (c *Client) readFromMaster(ctx context.Context, output []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	stop := context.AfterFunc(ctx, func() {
		c.conn.SetReadDeadline(time.Unix(0, 1))
	})
	defer stop()

	if dl, ok := ctx.Deadline(); ok {
		c.conn.SetReadDeadline(dl)
	} else {
		c.conn.SetReadDeadline(time.Time{})
	}

	_, err := c.conn.Read(output) // TODO: need to know in advance the size of the output to initialize it (see HandleConnection in router/the way we're doing it for ping);
	// we might need something more elegant idk
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return err
}
