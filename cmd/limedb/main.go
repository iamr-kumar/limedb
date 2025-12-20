package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/ritik/limedb/internal_legacy/server"
)

func main() {
	config := &server.Config{
		Address:        ":8080",
		ReadTimeout:    0,
		WriteTimeout:   0,
		MaxConnections: 0,
	}
	srv := server.NewServer(config)
	go func() {
		if err := srv.Start(); err != nil {
			log.Printf("Server stopped: %v", err)
		}
	}()

	log.Printf("LimeDB server is running on %s", config.Address)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down server...")
	_ = srv.Stop()
	log.Println("Server shutdown complete")
}
