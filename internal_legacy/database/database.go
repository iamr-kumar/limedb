package database

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ritik/limedb/internal_legacy/store"
	"github.com/ritik/limedb/internal_legacy/wal"
	"golang.org/x/crypto/bcrypt"
)

type Database struct {
	Name      string
	Store     *store.SharedStore
	Users     map[string]*User
	CreatedAt time.Time
	mu        sync.RWMutex
}

type User struct {
	Username  string
	Hash      []byte
	IsRoot    bool
	CreatedAt time.Time
}

type Registry struct {
	dbs     map[string]*Database
	dataDir string
	mu      sync.RWMutex
}

type SessionState int

const (
	StateIdle SessionState = iota
	StateCreateWantRootUser
	StateCreateWantRootPassword
	StateConnectWantUser
	StateConnectWantPassword
)

type Session struct {
	CurrentDB    *Database
	User         *User
	State        SessionState
	PendingDB    string
	TempUser     string
	ConnectedAt  time.Time
	LastActivity time.Time
}

/*
NewRegistry creates a new database registry with default configuration.
It initializes with default data directory "./data" and prepares
for managing multiple database instances.
*/
func NewRegistry() *Registry {
	dataDir := getDefaultDataDir()
	// Ensure the data directory exists
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		panic(fmt.Sprintf("failed to create data directory: %v", err))
	}

	return &Registry{
		dbs:     make(map[string]*Database),
		dataDir: dataDir,
		mu:      sync.RWMutex{},
	}
}

/*
NewRegistryWithConfig creates a registry with custom data directory.
Useful for testing or custom deployments.
*/
func NewRegistryWithConfig(dataDir string) *Registry {
	if dataDir == "" {
		dataDir = getDefaultDataDir()
	}

	// Convert to absolute path
	absPath, err := filepath.Abs(dataDir)
	if err != nil {
		fmt.Printf("Warning: Could not resolve path %s: %v\n", dataDir, err)
		absPath = dataDir
	}

	// Ensure directory exists
	if err := os.MkdirAll(absPath, 0755); err != nil {
		fmt.Printf("Warning: Could not create data directory %s: %v\n", absPath, err)
	}

	return &Registry{
		dbs:     make(map[string]*Database),
		dataDir: absPath,
		mu:      sync.RWMutex{},
	}
}

/*
CreateDatabase creates a new database with a root user.
It initializes a WAL-backed store with immediate sync mode for durability
and sets up the initial authentication credentials.
*/
func (r *Registry) CreateDatabase(name string, rootUser string, rootPassword string) error {
	if name == "" {
		return errors.New("database name cannot be empty")
	}

	if rootUser == "" || rootPassword == "" {
		return errors.New("root user and password cannot be empty")
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(rootPassword), bcrypt.DefaultCost)
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.dbs[name]; exists {
		return errors.New("database already exists")
	}

	// Create store with proper configuration
	storeConfig := store.StoreConfig{
		Database:    name,
		DataDir:     r.dataDir,
		ShardCount:  256,
		WALSyncMode: wal.SyncImmediate, // Use immediate sync for data safety
	}

	s, err := store.NewSharedStore(storeConfig)
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}

	now := time.Now()
	db := &Database{
		Name:      name,
		Store:     s,
		Users:     make(map[string]*User),
		CreatedAt: now,
	}

	db.Users[rootUser] = &User{
		Username:  rootUser,
		Hash:      hash,
		IsRoot:    true,
		CreatedAt: now,
	}

	r.dbs[name] = db

	// Store database metadata
	metadataKey := fmt.Sprintf("__db:metadata:%s", name)
	metadata := map[string]interface{}{
		"name":       name,
		"created_at": now,
		"root_user":  rootUser,
	}
	db.Store.Set(metadataKey, metadata)

	return nil
}

/*
GetDatabase retrieves a database by name.
Returns an error if the database doesn't exist in the registry.
*/
func (r *Registry) GetDatabase(name string) (*Database, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	db, exists := r.dbs[name]
	if !exists {
		return nil, errors.New("database not found")
	}
	return db, nil
}

/*
Authenticate verifies user credentials against the database.
It uses bcrypt for secure password comparison and returns
the user object if authentication succeeds.
*/
func (db *Database) Authenticate(username string, password string) (*User, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	user, exists := db.Users[username]
	if !exists {
		return nil, errors.New("user not found")
	}

	err := bcrypt.CompareHashAndPassword(user.Hash, []byte(password))
	if err != nil {
		return nil, errors.New("invalid password")
	}
	return user, nil
}

/*
NewSession creates a new client session.
Sessions track connection state and handle multi-step operations
like database creation and authentication flows.
*/
func NewSession() *Session {
	return &Session{
		State:        StateIdle,
		LastActivity: time.Now(),
	}
}

/*
Reset clears the session state back to idle.
Used after completing or canceling multi-step operations.
*/
func (s *Session) Reset() {
	s.State = StateIdle
	s.PendingDB = ""
	s.TempUser = ""
}

/*
IsConnected checks if the session is connected to a database.
Returns true if currently connected, false otherwise.
*/
func (s *Session) IsConnected() bool {
	return s.CurrentDB != nil
}

// getDefaultDataDir returns the default data directory path
func getDefaultDataDir() string {
	// Option 1: Use home directory
	homeDir, err := os.UserHomeDir()
	if err == nil {
		return filepath.Join(homeDir, ".limedb", "data")
	}

	// Option 2: Use current working directory as fallback
	cwd, err := os.Getwd()
	if err == nil {
		return filepath.Join(cwd, "data")
	}

	// Use temp fir as final option
	return filepath.Join(os.TempDir(), "limedb", "data")
}

// GetDataPath returns the full path for a database's data files
func (r *Registry) GetDataPath(databaseName string) string {
	return filepath.Join(r.dataDir, databaseName)
}
