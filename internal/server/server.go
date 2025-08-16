package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

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
	parser      *parser.Parser
	mutex       sync.Mutex
	connections map[net.Conn]struct{}
	shutdownCh  chan struct{}
}

func NewServer(config *Config, store *store.SharedStore) *Server {
	return &Server{
		config:      config,
		store:       store,
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
	defer func() {
		s.untrackConn(conn)
		conn.Close()
	}()

	reader := bufio.NewReader(conn)

	for {
		if s.config.ReadTimeout > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(s.config.ReadTimeout))
		}
		cmd, err := s.parser.ReadCommand(reader)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, net.ErrClosed) {
				log.Printf("Connection closed: %v", conn.RemoteAddr())
				return
			}

			return
		}

		switch cmd.Name {
		case "SET":
			s.store.Set(cmd.Key, cmd.Value)
			s.writeString(conn, parser.OK())
		case "GET":
			val, ok := s.store.Get(cmd.Key)
			if !ok {
				s.writeString(conn, parser.NotFound())
				continue
			}
			s.writeString(conn, parser.Value((fmt.Sprintf("%v", val))))
		default:
			s.writeString(conn, parser.Error("Unknown command: "+cmd.Name))
		}
	}
}

func (s *Server) writeString(conn net.Conn, out string) {
	if s.config.WriteTimeout > 0 {
		_ = conn.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout))
	}

	_, _ = conn.Write([]byte(out))
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
