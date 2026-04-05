package command

type CommandName string

const (
	Ping CommandName = "PING"
)

type Command struct {
	Command CommandName
	Args    []string
}
