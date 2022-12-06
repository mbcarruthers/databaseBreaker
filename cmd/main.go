package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"log"
	"sync"
	"time"
)

var (
	database_url = "postgresql://root@127.0.0.1:26257/defaultdb?sslmode=disable"
)

type DataStore struct {
	Conn *pgx.Conn
}

// NewDataStore - creates a new database connection object
func NewDataStore(ctx context.Context, dbUrl string) (*DataStore, error) {
	if config, err := pgx.ParseConfig(dbUrl); err != nil {
		return nil, err
	} else if conn, err := pgx.ConnectConfig(ctx, config); err != nil {
		return nil, err
	} else {
		return &DataStore{
			Conn: conn,
		}, nil
	}

}

// DatabaseCircuit -  typed name for the database circuit
type DatabaseCircuit func(context.Context, string) (*DataStore, error)

// DatabaseBreaker - Creates a limited amount of retries on the database connection
func DatabaseBreaker(dbCircuit DatabaseCircuit, failureThreshold uint) DatabaseCircuit {
	var consecutiveFailures int = 0
	var lastAttempt = time.Now()
	var mu sync.RWMutex

	return func(ctx context.Context, dbUrl string) (*DataStore, error) {
		mu.RLock() // establish a read-lock
		d := consecutiveFailures - int(failureThreshold)

		if d >= 0 {
			shouldRetryAt := lastAttempt.Add(time.Second * 2 << d) // Note: 'Effectively' doubles the wait time for the next call.
			if !time.Now().After(shouldRetryAt) {
				mu.RUnlock()
				return nil, errors.New("service unavailable")
			}
		}

		mu.RUnlock()                     // release read-lock
		db, err := dbCircuit(ctx, dbUrl) // issue vanilla(non-wrapped) function call

		mu.Lock() // lock shared resources
		defer mu.Unlock()
		lastAttempt = time.Now() // record the time of db access attempt
		if err != nil {
			consecutiveFailures++ // count failure
			return db, err        // return values non-wrapped function would like a normal call
		}
		consecutiveFailures = 0 // reset failure count
		return db, nil          // return resources upon success
	}
}

func main() {
	NewDataStoreBreaker := DatabaseBreaker(NewDataStore, 4) // give 4 attempts

	roachdb, err := NewDataStoreBreaker(context.Background(), database_url)
	if err != nil {
		wait := make(chan struct{})
		dbStream := make(chan *DataStore)
		fmt.Println("Connecting....")
		go func(db *DataStore) {
			routineDeadline := time.Now().Add(time.Second * 32) // enough time for 8 attempts within 32 seconds. As an example, it will response with 'service unavailable' on the last attempt
			for range time.Tick(time.Second * 4) {
				if time.Now().After(routineDeadline) {
					log.Fatalf("Database connectivity could not be acquired")
				}
				log.Println("Trying to connect...")
				if db, err = NewDataStoreBreaker(context.Background(), database_url); err != nil {
					fmt.Printf("%s \n", err.Error())
				} else {
					dbStream <- db
					close(wait)
					break
				}
			}
		}(roachdb)
		roachdb = <-dbStream // send the database connection by channel
		<-wait
	}
	log.Println("database connected")
	ctx := context.Background()
	if tx, err := roachdb.Conn.Begin(ctx); err != nil {
		log.Println("Could not begin database transaction ", err)
	} else if _, err = tx.Exec(ctx, "CREATE DATABASE subjectives"); err != nil {
		log.Println("Error creating database")
	} else {
		err := tx.Commit(ctx)
		if err != nil {
			log.Fatalf("Could not commit creation of database %s \n", err.Error())
		}
	}
}
