package config

import (
	"sync"
)

var (
	Endpoints   []string
	Verbose     bool
	Jwt         string
	Username    string
	Password    string
	ProgName    string
	OutputMutex sync.Mutex
)
