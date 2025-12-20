package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ritik/limedb/internal/server"
)

func main() {
	config := &server.Config{
		Address:        ":8080",
		ReadTimeout:    120 * time.Second,
		WriteTimeout:   30 * time.Second,
		MaxConnections: 100,
	}
	lime := server.NewServer(config)
	go func() {
		if err := lime.Start(); err != nil {
			log.Printf("Server stopped: %v", err)
		}
	}()

	log.Printf("LimeDB server is running on %s", config.Address)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down server...")
	_ = lime.Stop()
	log.Println("Server shutdown complete")
}
