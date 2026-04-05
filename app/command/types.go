package command

type CommandName string

const (
	Ping CommandName = "*1\r\n"
)

type Command struct {
	Command CommandName
	Args    []string
}
