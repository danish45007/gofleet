package common

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/joho/godotenv"
)

const (
	defaultHeartbeatInterval = 5 * time.Second
	maxRetries               = 5
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}
}

func checkEnvVars(envVar, envVarName string) []string {
	var missingEnvVars []string
	// Check if the environment variable is not set
	// If it is not set, add it to the list of missing environment variables
	// If it is set, return an empty list
	if envVar == "" {
		missingEnvVars = append(missingEnvVars, envVarName)
	}
	return missingEnvVars

}

func GetDBConnectionString() string {
	dbUser := os.Getenv("POSTGRES_USER")
	dbPassword := os.Getenv("POSTGRES_PASSWORD")
	dbName := os.Getenv("POSTGRES_DB")
	dbHost := os.Getenv("POSTGRES_HOST")
	if dbHost == "" {
		dbHost = "localhost"
	}
	// Check if the environment variables are set
	missingEnvVars := checkEnvVars(dbUser, "POSTGRES_USER")
	missingEnvVars = append(missingEnvVars, checkEnvVars(dbPassword, "POSTGRES_PASSWORD")...)
	missingEnvVars = append(missingEnvVars, checkEnvVars(dbName, "POSTGRES_HOST")...)

	if len(missingEnvVars) > 0 {
		// If any environment variables are missing, return an empty string
		log.Fatalf("The following required environment variables are not set: %s",
			strings.Join(missingEnvVars, ", "))
	}
	return fmt.Sprintf("postgres://%s:%s@%s:5432/%s", dbUser, dbPassword, dbHost, dbName)
}

func ConnectToDB(ctx context.Context, dbConnectionString string) (*pgxpool.Pool, error) {
	var dbPool *pgxpool.Pool
	var err error

	for retry := 0; retry < maxRetries; retry++ {
		if dbPool, err = pgxpool.Connect(ctx, dbConnectionString); err == nil {
			log.Printf("Connected to the database")
			return dbPool, nil
		}

		log.Printf("Error connecting to the database: %v", err)

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			// Continue with exponential backoff
			waitTime := time.Duration(1<<retry) * defaultHeartbeatInterval
			log.Printf("Retrying in %s seconds...", waitTime)
			time.Sleep(waitTime)
		}
	}

	log.Printf("Ran out of retries to connect to database after %s", maxRetries*defaultHeartbeatInterval)
	return nil, err
}
