package marshal

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/codecrafters-io/redis-starter-go/app/types"
)

type Parser struct {
}

func NewParser() *Parser {
	return &Parser{}
}

func (p *Parser) ParseCommand(chunk []byte) (*types.Command, error) {
	array, _, err := p.parseArray(chunk, 0)
	if err != nil {
		return nil, fmt.Errorf("parse command: %w", err)
	}

	commandName := array[0]
	if commandName.Type != types.SString && commandName.Type != types.BString {
		return nil, fmt.Errorf("expected command name to be SString, but got: %d", commandName.Type)
	}

	var args []*types.RedisData
	if len(array) > 0 {
		args = array[1:]
	}

	return &types.Command{
		Command: types.CommandName(array[0].Data),
		Args:    args,
	}, nil
}

func (p *Parser) parseArray(chunk []byte, position int) ([]*types.RedisData, int, error) {
	position, err := p.validateSignatureByte(chunk, position, '*')
	if err != nil {
		return nil, 0, errors.New("expected array, but was not array")
	}

	lengthStr, position, err := p.readDataAsStringUntilTerminator(chunk, position)
	length, err := strconv.Atoi(lengthStr)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to parse length for array: %w", err)
	}

	result := make([]*types.RedisData, 0, length)
	for range length {
		data, newPosition, err := p.bestEffortParse(chunk, position)
		if err != nil {
			return nil, 0, fmt.Errorf("failed while parsing chunk: %w", err)
		}

		result = append(result, data)
		position = newPosition
	}

	return result, position, nil
}

func (p *Parser) bestEffortParse(chunk []byte, position int) (*types.RedisData, int, error) {
	firstByte := chunk[position]

	if firstByte == '+' {
		return p.parseSString(chunk, position)
	}
	if firstByte == ':' {
		return p.parseInt(chunk, position)
	}
	if firstByte == '$' {
		return p.parseBString(chunk, position)
	}
	if firstByte == '*' {
		return nil, 0, errors.New("WIP: support for nested arrays")
	}

	return nil, 0, fmt.Errorf("unimplemented parse function at position:%d, chunk: %s", position, string(chunk))
}

func (p *Parser) parseSString(chunk []byte, position int) (*types.RedisData, int, error) {
	position, err := p.validateSignatureByte(chunk, position, '+')
	if err != nil {
		return nil, 0, errors.New("expected sstring, but was not sstring")
	}

	data, position, err := p.readDataAsStringUntilTerminator(chunk, position)
	if err != nil {
		return nil, 0, fmt.Errorf("sstring parse: %w", err)
	}
	return &types.RedisData{
		Type: types.SString,
		Data: data,
	}, position, nil
}

func (p *Parser) parseBString(chunk []byte, position int) (*types.RedisData, int, error) {
	position, err := p.validateSignatureByte(chunk, position, '$')
	if err != nil {
		return nil, 0, errors.New("expected bstring, but was not bstring")
	}

	lengthStr, position, err := p.readDataAsStringUntilTerminator(chunk, position)
	if err != nil {
		return nil, 0, fmt.Errorf("bstring len parse: %w", err)
	}

	length, err := strconv.Atoi(lengthStr)
	if err != nil || length < 0 {
		return nil, 0, fmt.Errorf("invalid length for bstring: %s", lengthStr)
	}

	data, position, err := p.readDataAsString(chunk, position, length)
	if err != nil {
		return nil, 0, fmt.Errorf("bstring data parse: %w", err)
	}

	position, err = p.validateTerminator(chunk, position)
	if err != nil {
		return nil, 0, fmt.Errorf("bstring terminator: %w", err)
	}

	return &types.RedisData{
		Type: types.BString,
		Data: data,
	}, position, nil
}

func (p *Parser) parseInt(chunk []byte, position int) (*types.RedisData, int, error) {
	position, err := p.validateSignatureByte(chunk, position, ':')
	if err != nil {
		return nil, 0, errors.New("expected int, but was not int")
	}

	if position >= len(chunk) {
		return nil, 0, errors.New("out of bounds parsing int")
	}

	optionalSign := chunk[position]
	var addNegative bool
	switch optionalSign {
	case '+':
		position++
	case '-':
		addNegative = true
		position++
	default:
		if !unicode.IsDigit(rune(optionalSign)) {
			return nil, 0, fmt.Errorf("unexpected sign for integer: %b", optionalSign)
		}
	}

	data, position, err := p.readDataAsStringUntilTerminator(chunk, position)
	if err != nil {
		return nil, 0, fmt.Errorf("parse int: %w", err)
	}

	if addNegative {
		data = fmt.Sprintf("-%s", data)
	}

	_, err = strconv.Atoi(data)
	if err != nil {
		return nil, 0, fmt.Errorf("failed parsing integer: %w", err)
	}

	return &types.RedisData{
		Type: types.Integer,
		Data: data,
	}, position, nil
}

// === Lower level Parser utils function, could consider having them as ParserState struct or smth?

func (p *Parser) isTerminator(chunk []byte, position int) (bool, error) {
	isCR := chunk[position] == '\r'
	if !isCR {
		return false, nil
	}
	if position+1 >= len(chunk) {
		return false, errors.New("position overflow while checking for terminator")
	}
	return chunk[position+1] == '\n', nil
}

func (p *Parser) validateTerminator(chunk []byte, position int) (int, error) {
	isTerm, err := p.isTerminator(chunk, position)
	if err != nil {
		return 0, err
	}
	if !isTerm {
		return 0, errors.New("expected to have a terminator")
	}

	return position + 2, nil
}

func (p *Parser) readDataAsStringUntilTerminator(chunk []byte, position int) (string, int, error) {
	var sb strings.Builder
	var hasTerminated bool

	for ; position < len(chunk); position++ {
		if isTerm, err := p.isTerminator(chunk, position); !isTerm {
			if err != nil {
				return "", 0, fmt.Errorf("int parse: %w", err)
			}

			sb.WriteByte(chunk[position])
		} else {
			hasTerminated = true
			break
		}
	}

	if !hasTerminated {
		return "", 0, errors.New("parseInt: exhaused chunk, but did not meet terminator")
	}

	position += 2
	return sb.String(), position, nil
}

func (p *Parser) readDataAsString(chunk []byte, position int, readCount int) (string, int, error) {
	var sb strings.Builder

	finalPosition := position + readCount
	if finalPosition >= len(chunk) {
		return "", 0, fmt.Errorf("read string with count %d is out of bounds for chunk len %d", readCount, len(chunk))
	}
	for ; position < finalPosition; position++ {
		sb.WriteByte(chunk[position])
	}
	return sb.String(), position, nil
}

func (p *Parser) validateSignatureByte(chunk []byte, position int, signByte byte) (int, error) {
	firstByte := chunk[position]
	if firstByte != signByte {
		return 0, errors.New("attempted to parse int, but is not int")
	}

	position++
	return position, nil
}
