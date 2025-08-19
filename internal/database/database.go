package database

import (
	"errors"
	"sync"

	"github.com/ritik/limedb/internal/store"
	"golang.org/x/crypto/bcrypt"
)

type Database struct {
	Name  string
	Store *store.SharedStore
	Users map[string]*User
	mu    sync.RWMutex
}

type User struct {
	Username string
	Hash     []byte
	IsRoot   bool
}

type Registry struct {
	dbs map[string]*Database
	mu  sync.RWMutex
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
	CurrentDB *Database
	User      *User
	State     SessionState
	PendingDB string
	TempUser  string
}

func NewRegistry() *Registry {
	return &Registry{
		dbs: make(map[string]*Database),
		mu:  sync.RWMutex{},
	}
}

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
	db := &Database{
		Name:  name,
		Store: store.NewSharedStore(256), // Default shard count
		Users: make(map[string]*User),
	}

	db.Users[rootUser] = &User{
		Username: rootUser,
		Hash:     hash,
		IsRoot:   true,
	}

	r.dbs[name] = db
	return nil
}

func (r *Registry) GetDatabase(name string) (*Database, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	db, exists := r.dbs[name]
	if !exists {
		return nil, errors.New("database not found")
	}
	return db, nil
}

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

func NewSession() *Session {
	return &Session{
		State: StateIdle,
	}
}

func (s *Session) Reset() {
	s.State = StateIdle
	s.PendingDB = ""
	s.TempUser = ""
}

func (s *Session) IsConnected() bool {
	return s.CurrentDB != nil
}
