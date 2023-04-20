package sub

import (
	"time"

	"github.com/gocql/gocql"
)

type Details struct {
	ID          string    `json:"id"`
	PackageId   string    `json:"packageId"`
	StartDate   time.Time `json:"startDate"`
	EndDate     time.Time `json:"endDate"`
	RenewalDate time.Time `json:"renewalDate"`
	Status      string    `json:"status"`
}

type DB struct {
	s *gocql.Session
}

type Config struct {
	Hosts       []string
	Consistency uint16
}

// Connect to database
func InitDB(config *Config) *DB {
	cluster := gocql.NewCluster(config.Hosts...)
	cluster.Keyspace = "PandaSubs"
	cluster.Consistency = gocql.Consistency(config.Consistency)
	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	// Create table if not exists
	err = session.Query(`CREATE TABLE IF NOT EXISTS subscription (
        id text PRIMARY KEY,
        packageId text,
        startDate timestamp,
        endDate timestamp,
        renewalDate timestamp,
        status text
    )`).Exec()
	if err != nil {
		panic(err)
	}
	return &DB{s: session}
}

// Get details by ID
func (db *DB) GetById(id string) (*Details, error) {
	var details Details
	if err := db.s.Query(`SELECT * FROM subscription WHERE id = ?`, id).Scan(&details); err != nil {
		return nil, err
	}
	return &details, nil
}

// Insert new details
func (db *DB) Insert(details *Details) error {
	query := `INSERT INTO subscription (id, packageId, startDate, endDate, renewalDate, status) 
             VALUES (?, ?, ?, ?, ?, ?)`
	return db.s.Query(query, details.ID, details.PackageId, details.StartDate,
		details.EndDate, details.RenewalDate, details.Status).Exec()
}

// Update details
func (db *DB) UpdateById(id string, details *Details) error {
	query := `UPDATE subscription SET packageId = ?, startDate = ?, endDate = ?, 
             renewalDate = ?, status = ? WHERE id = ?`
	return db.s.Query(query, details.PackageId, details.StartDate, details.EndDate,
		details.RenewalDate, details.Status, id).Exec()
}

// Delete details
func (db *DB) DeleteById(id string) error {
	query := `DELETE FROM subscription WHERE id = ?`
	return db.s.Query(query, id).Exec()
}
