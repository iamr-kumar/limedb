package server

import (
	"bufio"
	"context"
	"errors"
	"log"
	"net"
	"sync"
	"time"

	"github.com/ritik/limedb/internal/parser"
)

/*
Config holds the server configuration parameters
*/
type Config struct {
	// address to bind the server to
	Address string
	// timeout for a GET operation
	ReadTimeout time.Duration
	// timeout for a SET operation
	WriteTimeout time.Duration
	// maximum number of concurrent connections
	MaxConnections int
}

/*
Server represents the LimeDB server
*/
type Server struct {
	// server configuration
	config *Config
	// network listener
	listener net.Listener
	// parser for commands
	parser *parser.Parser
	// mutex to handle concurrent access
	mutex sync.Mutex
	// active connections
	connections map[net.Conn]struct{}
	// channel to signal server shutdown
	shutdownCh chan struct{}
}

/*
Creates a new Server instance with the provided configuration
*/
func NewServer(config *Config) *Server {
	return &Server{
		config:      config,
		connections: make(map[net.Conn]struct{}),
		shutdownCh:  make(chan struct{}),
	}
}

/*
Starts the LimeDB server and begins listening for incoming connections
*/
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.config.Address)
	if err != nil {
		return err
	}

	s.listener = ln
	log.Printf("LimeDB server started on %s", s.config.Address)

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			// Check for shutdown signal. When server is shutting down
			// this channel will be closed
			case <-s.shutdownCh:
				return nil
			// Handle other errors
			default:
				if _, ok := err.(net.Error); ok {
					log.Printf("Temporary error accepting aconnection")
					time.Sleep(100 * time.Millisecond)
					continue
				}
				return err
			}
		}

		if !s.trackConnection(conn) {
			conn.Close()
			continue
		}

		go s.handleConnection(conn)
	}
}

/*
Tacks a new connection and checks against MaxConnections limit
*/
func (s *Server) trackConnection(conn net.Conn) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Check if adding this connection would exceed MaxConnections
	if s.config.MaxConnections > 0 && len(s.connections) >= s.config.MaxConnections {
		log.Printf("Too many connections. Rejecting new connection from %s", conn.RemoteAddr())
		return false
	}

	// Add the connection to the active connections map
	s.connections[conn] = struct{}{}
	return true
}

/*
Untracks a connection when it is closed
*/
func (s *Server) untrackConnection(conn net.Conn) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	delete(s.connections, conn)
}

func (s *Server) handleConnection(conn net.Conn) {
	const clientBanner = `
██╗     ██╗███╗   ███╗███████╗██████╗ ██████╗ 
██║     ██║████╗ ████║██╔════╝██╔══██╗██╔══██╗
██║     ██║██╔████╔██║█████╗  ██║  ██║██████╔╝
██║     ██║██║╚██╔╝██║██╔══╝  ██║  ██║██╔══██╗
███████╗██║██║ ╚═╝ ██║███████╗██████╔╝██████╔╝
╚══════╝╚═╝╚═╝     ╚═╝╚══════╝╚═════╝ ╚═════╝

Commands: SET key value | GET key 
`
	_, _ = conn.Write([]byte(clientBanner))

	// Ensure connection is untracked and closed when done
	defer func() {
		s.untrackConnection(conn)
		conn.Close()
	}()

	reader := bufio.NewReader(conn)

	for {
		// Setting read timeout if configured
		// This helps to avoid hanging connections
		// If no request or command is received within the timeout period
		// the underlying connection will be closed
		if s.config.ReadTimeout > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(s.config.ReadTimeout))
		}

		// Read command from the connection
		command, err := s.parser.ReadCommand(reader)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, net.ErrClosed) {
				log.Printf("Connection closed: %v", conn.RemoteAddr())
				return
			}

			return
		}

		s.handleCommand(conn, command)
	}

}

func (s *Server) handleCommand(conn net.Conn, command *parser.Command) {
	switch command.Name {
	case "SET":
		// TODO: Handle SET command
		s.writeStringResponse(conn, "OK\n")
	case "GET":
		// TODO: Handle GET command
		s.writeStringResponse(conn, "VALUE\n")
	case "EXIT":
		s.writeStringResponse(conn, parser.OK())
		log.Printf("Connection closed by client: %v", conn.RemoteAddr())
		s.CloseConnection(conn)
	}
}

/*
Write string response to the connection
*/
func (s *Server) writeStringResponse(conn net.Conn, response string) {
	// Setting write timeout if configured
	// This helps to avoid hanging connections
	// If the response cannot be written within the timeout period
	// the underlying connection will be closed
	if s.config.WriteTimeout > 0 {
		_ = conn.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout))
	}
	_, _ = conn.Write([]byte(response))
}

/*
Closes a specific connection
*/
func (s *Server) CloseConnection(conn net.Conn) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, exists := s.connections[conn]; exists {
		delete(s.connections, conn)
		_ = conn.Close()
		log.Printf("Closed connection: %v", conn.RemoteAddr())
	}
}

/*
Stops the LimeDB server and closes all active connections
*/
func (s *Server) Stop() error {
	// Close the listener to stop accepting new connections
	close(s.shutdownCh)
	if s.listener != nil {
		s.listener.Close()
	}
	// Close all active connections
	s.mutex.Lock()
	defer s.mutex.Unlock()
	for conn := range s.connections {
		_ = conn.Close()
	}
	s.connections = make(map[net.Conn]struct{})
	log.Println("LimeDB server stopped")
	return nil
}
