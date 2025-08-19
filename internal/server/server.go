package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/ritik/limedb/internal/database"
	"github.com/ritik/limedb/internal/parser"
	"github.com/ritik/limedb/internal/store"
)

type Config struct {
	Address        string
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
	MaxConnections int
}

// Server represents the LimeDB server configuration
type Server struct {
	config      *Config
	listener    net.Listener
	store       *store.SharedStore
	registry    *database.Registry
	parser      *parser.Parser
	mutex       sync.Mutex
	connections map[net.Conn]struct{}
	shutdownCh  chan struct{}
}

func NewServer(config *Config) *Server {
	return &Server{
		config:      config,
		registry:    database.NewRegistry(),
		parser:      parser.New(),
		connections: make(map[net.Conn]struct{}),
		shutdownCh:  make(chan struct{}),
	}
}

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
			case <-s.shutdownCh:
				return nil
			default:
				if ne, ok := err.(net.Error); ok && ne.Temporary() {
					log.Printf("Temporary error accepting connection: %v", err)
					time.Sleep(100 * time.Millisecond)
					continue
				}
				return err
			}
		}

		if !s.trackConn(conn) {
			conn.Close()
			continue
		}

		go s.handleConnection(conn)
	}

}

func (s *Server) trackConn(c net.Conn) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.config.MaxConnections > 0 && len(s.connections) >= s.config.MaxConnections {
		log.Printf("Max connections reached, rejecting new connection: %v", c.RemoteAddr())
		return false
	}

	s.connections[c] = struct{}{}
	return true
}

func (s *Server) untrackConn(c net.Conn) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	delete(s.connections, c)
}

func (s *Server) handleConnection(conn net.Conn) {
	const clientBanner = `
██╗     ██╗███╗   ███╗███████╗██████╗ ██████╗ 
██║     ██║████╗ ████║██╔════╝██╔══██╗██╔══██╗
██║     ██║██╔████╔██║█████╗  ██║  ██║██████╔╝
██║     ██║██║╚██╔╝██║██╔══╝  ██║  ██║██╔══██╗
███████╗██║██║ ╚═╝ ██║███████╗██████╔╝██████╔╝
╚══════╝╚═╝╚═╝     ╚═╝╚══════╝╚═════╝ ╚═════╝

Commands: CREATE <database> | CONNECT <database> | SET key value | GET key | CANCEL
`
	_, _ = conn.Write([]byte(clientBanner))
	defer func() {
		s.untrackConn(conn)
		conn.Close()
	}()

	session := database.NewSession()
	reader := bufio.NewReader(conn)

	for {
		if s.config.ReadTimeout > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(s.config.ReadTimeout))
		}

		// If not idle, we are in an interactive session
		if session.State != database.StateIdle {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			s.handleInteractiveCommand(conn, session, strings.TrimSpace(line))
			continue
		}

		cmd, err := s.parser.ReadCommand(reader)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, net.ErrClosed) {
				log.Printf("Connection closed: %v", conn.RemoteAddr())
				return
			}

			return
		}

		s.handleCommand(conn, session, cmd)
	}
}

func (s *Server) handleInteractiveCommand(conn net.Conn, session *database.Session, input string) {
	if strings.ToUpper(input) == "CANCEL" {
		session.Reset()
		s.writeString(conn, "Operation cancelled.\n")
		return
	}

	switch session.State {
	case database.StateCreateWantRootUser:
		if input == "" {
			s.writeString(conn, parser.Error("Username cannot be empty.	"))
			s.writeString(conn, "Please enter a root username: ")
			return
		}
		session.TempUser = input
		session.State = database.StateCreateWantRootPassword
		s.writeString(conn, "Please enter a root password: ")
	case database.StateCreateWantRootPassword:
		if input == "" {
			s.writeString(conn, parser.Error("Password cannot be empty.	"))
			s.writeString(conn, "Please enter a root password: ")
			return
		}
		// Create the database with the root user and password
		err := s.registry.CreateDatabase(session.PendingDB, session.TempUser, input)
		if err != nil {
			s.writeString(conn, parser.Error(err.Error()))
			session.Reset()
			return
		}

		session.CurrentDB, _ = s.registry.GetDatabase(session.PendingDB)
		session.User = session.CurrentDB.Users[session.TempUser]
		s.writeString(conn, parser.Success(fmt.Sprintf("Database '%s' created successfully with root user '%s'.", session.PendingDB, session.TempUser)))
		log.Printf("Database '%s' created with root user '%s'", session.PendingDB, session.TempUser)
		session.Reset()

	case database.StateConnectWantUser:
		if input == "" {
			s.writeString(conn, parser.Error("Username cannot be empty.	"))
			s.writeString(conn, "Please enter your username: ")
			return
		}
		session.TempUser = input
		session.State = database.StateConnectWantPassword
		s.writeString(conn, "Please enter your password: ")

	case database.StateConnectWantPassword:
		if input == "" {
			s.writeString(conn, parser.Error("Password cannot be empty.	"))
			s.writeString(conn, "Please enter your password: ")
			return
		}

		db, err := s.registry.GetDatabase(session.PendingDB)
		if err != nil {
			s.writeString(conn, parser.Error("Database not found."))
			session.Reset()
			return
		}

		// Authenticate the user
		user, err := db.Authenticate(session.TempUser, input)
		if err != nil {
			s.writeString(conn, parser.Error("Authentication failed."))
			session.Reset()
			return
		}

		// Success - update session
		session.CurrentDB = db
		session.User = user
		s.writeString(conn, parser.Success(fmt.Sprintf("Connected to database '%s' as user '%s'.", db.Name, user.Username)))
		log.Printf("User '%s' connected to database '%s'", user.Username, db.Name)
		session.Reset()
	}
}

func (s *Server) handleCommand(conn net.Conn, session *database.Session, cmd *parser.Command) {
	switch cmd.Name {
	case "CREATE":
		if session.IsConnected() {
			s.writeString(conn, parser.Error("Already connected to a database. Disconnect first."))
			return
		}

		// Start Create DB flow
		session.PendingDB = cmd.Key
		session.State = database.StateCreateWantRootUser
		s.writeString(conn, "Please enter a root username: ")

	case "CONNECT":
		if session.IsConnected() {
			s.writeString(conn, parser.Error(fmt.Sprintf("Already connected to database '%s'. Disconnect first.", session.CurrentDB.Name)))
			return
		}
		// Check if the database exists
		if _, err := s.registry.GetDatabase(cmd.Key); err != nil {
			s.writeString(conn, parser.Error("Database not found."))
			return
		}

		// Start Connect flow
		session.PendingDB = cmd.Key
		session.State = database.StateConnectWantUser
		s.writeString(conn, "Please enter your username: ")

	case "SET":
		if !session.IsConnected() {
			s.writeString(conn, parser.Error("Not connected to any database."))
			return
		}
		if cmd.Key == "" || cmd.Value == "" {
			s.writeString(conn, parser.Error("SET command requires both key and value."))
			return
		}
		session.CurrentDB.Store.Set(cmd.Key, cmd.Value)
		s.writeString(conn, parser.OK())

	case "GET":
		if !session.IsConnected() {
			s.writeString(conn, parser.Error("Not connected to any database."))
			return
		}

		val, ok := session.CurrentDB.Store.Get(cmd.Key)
		if !ok {
			s.writeString(conn, parser.NotFound())
			return
		}
		s.writeString(conn, parser.Value(fmt.Sprintf("%v", val)))

	case "DELETE":
		if !session.IsConnected() {
			s.writeString(conn, parser.Error("Not connected to any database."))
			return
		}
		if cmd.Key == "" {
			s.writeString(conn, parser.Error("DELETE command requires a key."))
			return
		}
		if session.CurrentDB.Store.Delete(cmd.Key) {
			s.writeString(conn, parser.OK())
		} else {
			s.writeString(conn, parser.NotFound())
		}

	case "EXIT":
		s.writeString(conn, parser.OK())
		log.Printf("Client disconnected: %v", conn.RemoteAddr())
		s.CloseConnection(conn)
	}
}

func (s *Server) writeString(conn net.Conn, out string) {
	if s.config.WriteTimeout > 0 {
		_ = conn.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout))
	}

	_, _ = conn.Write([]byte(out))
}

func (s *Server) CloseConnection(conn net.Conn) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, exists := s.connections[conn]; exists {
		delete(s.connections, conn)
		_ = conn.Close()
		log.Printf("Connection closed: %v", conn.RemoteAddr())
	}
}

func (s *Server) Stop() error {
	close(s.shutdownCh)
	if s.listener != nil {
		_ = s.listener.Close()
	}
	s.mutex.Lock()
	for conn := range s.connections {
		_ = conn.Close()
	}
	s.mutex.Unlock()
	return nil
}
