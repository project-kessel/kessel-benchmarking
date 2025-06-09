package config

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type DBConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	DBName   string
}

func LoadDBConfig() DBConfig {
	_ = godotenv.Load()

	return DBConfig{
		Host:     os.Getenv("DB_HOST"),
		Port:     os.Getenv("DB_PORT"),
		User:     os.Getenv("DB_USER"),
		Password: os.Getenv("DB_PASSWORD"),
		DBName:   os.Getenv("DB_NAME"),
	}
}

func DropAndRecreateDatabase(cfg DBConfig) error {
	adminConnStr := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=postgres sslmode=disable",
		cfg.Host, cfg.Port, cfg.User, cfg.Password,
	)

	adminDB, err := sql.Open("postgres", adminConnStr)
	if err != nil {
		return fmt.Errorf("failed to connect to admin DB: %w", err)
	}
	defer func(adminDB *sql.DB) {
		err := adminDB.Close()
		if err != nil {

		}
	}(adminDB)

	// Safely quote the DB name
	quotedDBName := fmt.Sprintf(`"%s"`, cfg.DBName)

	// Drop the database
	_, err = adminDB.Exec(`DROP DATABASE IF EXISTS ` + quotedDBName)
	if err != nil {
		return fmt.Errorf("failed to drop database: %w", err)
	}

	log.Printf("✅ Dropped database %s\n", cfg.DBName)
	row := adminDB.QueryRow("SELECT 1 FROM pg_database WHERE datname = $1", cfg.DBName)
	var exists int
	err = row.Scan(&exists)
	if err == sql.ErrNoRows {
		fmt.Println("✅ Database successfully deleted")
	} else if err != nil {
		log.Fatalf("error checking database: %v", err)
	} else {
		fmt.Println("❌ Database still exists")
	}

	// Recreate the database
	_, err = adminDB.Exec(`CREATE DATABASE ` + quotedDBName)
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	log.Printf("✅ Recreated database %s\n", cfg.DBName)
	return nil
}

func ConnectDB() *gorm.DB {
	cfg := LoadDBConfig()

	dsn := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.DBName,
	)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent), // disable logging
	})
	if err != nil {
		panic(fmt.Sprintf("failed to connect to database: %v", err))
	}

	return db
}
