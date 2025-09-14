package parser

import (
	"bufio"
	"errors"
	"io"
	"strings"
)

type Command struct {
	Name  string
	Key   string
	Value string
}

type Parser struct{}

func New() *Parser {
	return &Parser{}
}

var (
	ErrEmpty      = errors.New("empty command")
	ErrBadFormat  = errors.New("bad command format")
	ErrBadCommand = errors.New("unknown command")
)

func (p *Parser) ParseLine(line string) (*Command, error) {
	line = strings.TrimSpace(line)
	if line == "" {
		return nil, ErrEmpty
	}

	upper := strings.ToUpper(line)
	if strings.HasPrefix(upper, "SET ") {
		parts := strings.SplitN(line, " ", 3)
		if len(parts) != 3 {
			return nil, ErrBadFormat
		}
		return &Command{Name: "SET", Key: parts[1], Value: parts[2]}, nil
	}

	if strings.HasPrefix(upper, "GET ") {
		parts := strings.SplitN(line, " ", 2)
		if len(parts) != 2 {
			return nil, ErrBadFormat
		}
		return &Command{Name: "GET", Key: parts[1]}, nil
	}

	if strings.HasPrefix(upper, "DELETE ") {
		parts := strings.SplitN(line, " ", 2)
		if len(parts) != 2 {
			return nil, ErrBadFormat
		}
		return &Command{Name: "DELETE", Key: parts[1]}, nil
	}

	if strings.HasPrefix(upper, "CREATE ") {
		parts := strings.SplitN(line, " ", 2)
		if len(parts) != 2 {
			return nil, ErrBadFormat
		}

		return &Command{Name: "CREATE", Key: parts[1]}, nil
	}

	if strings.HasPrefix(upper, "CONNECT ") {
		parts := strings.SplitN(line, " ", 2)
		if len(parts) != 2 {
			return nil, ErrBadFormat
		}

		return &Command{Name: "CONNECT", Key: parts[1]}, nil
	}

	if upper == "EXIT" {
		return &Command{Name: "EXIT"}, nil
	}

	return nil, ErrBadCommand
}

func (p *Parser) ReadCommand(r *bufio.Reader) (*Command, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		if errors.Is(err, io.EOF) && line == "" {
			return nil, err
		}

		// Still try to parse the line if it is not empty
		if line == "" {
			return nil, ErrEmpty
		}
	}
	return p.ParseLine(line)
}

func OK() string            { return "OK\n" }
func NotFound() string      { return "NOT FOUND\n" }
func Value(v string) string { return v + "\n" }
func Error(message string) string {
	return "ERROR: " + message + "\n"
}
func Success(message string) string {
	return "SUCCESS: " + message + "\n"
}
