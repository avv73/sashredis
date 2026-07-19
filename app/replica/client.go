package replica

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	log "github.com/sirupsen/logrus"
)

type Client struct {
	address string
	conn    net.Conn
	dialer  net.Dialer
}

// Add data encoder only if we really need it for some other commands and stuff.
func NewClient() *Client {
	return &Client{}
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

	i, err := c.writeToMaster(ctx, pingCommand)
	if err != nil || i == 0 {
		return fmt.Errorf("writing to master failed: %w", err)
	}

	output := make([]byte, 7)
	i, err = c.readFromMaster(ctx, output)
	if err != nil {
		return fmt.Errorf("reading from master failed: %w", err)
	}

	if !bytes.Equal(output, pingResult) {
		return fmt.Errorf("expected a PONG response from master, got: %s", string(output))
	}

	return nil
}

func (c *Client) writeToMaster(ctx context.Context, data []byte) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
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

	n, err := c.conn.Write(data)
	if err != nil {
		if ctx.Err() != nil {
			return n, ctx.Err()
		}
	}
	return n, err
}

func (c *Client) readFromMaster(ctx context.Context, output []byte) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
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

	n, err := c.conn.Read(output) // TODO: need to know in advance the size of the output to initialize it (see HandleConnection in router/the way we're doing it for ping);
	// we might need something more elegant idk
	if err != nil {
		if ctx.Err() != nil {
			return n, ctx.Err()
		}
	}
	return n, err
}
