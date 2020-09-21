package tradelog

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/ghodss/yaml"
	_ "github.com/lib/pq"
)

// DBConnection represents the connection parameters to the postgres database
type DBConnection struct {
	Host     string `json:"host"`
	Port     uint16 `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	DBName   string `json:"db_name"`

	db *sql.DB
}

// FromYAML creates the connection from a yaml configuration
func FromYAML(bytes []byte) (c DBConnection, err error) {
	err = yaml.Unmarshal(bytes, &c)
	return
}

// FromJSON creates the connection from a json configuration
func FromJSON(bytes []byte) (c DBConnection, err error) {
	err = json.Unmarshal(bytes, &c)
	return
}

// Open opens the connection
func (c *DBConnection) Open() (err error) {
	c.db, err = sql.Open("postgres", c.connectionString())
	return
}

// Close closes the connection
func (c *DBConnection) Close() {
	c.db.Close()
}

// DB returns the sql.DB struct
func (c DBConnection) DB() *sql.DB {
	return c.db
}

func (c DBConnection) connectionString() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", c.Host, c.Port, c.User, c.Password, c.DBName)
}
