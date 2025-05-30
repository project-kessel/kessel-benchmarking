package config

import (
	"database/sql"
	"fmt"
	"github.com/yourusername/go-db-bench/db/schemas/option1_denormalized_reference_2_rep_tables/models"
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
	defer adminDB.Close()

	// Terminate any active connections
	_, _ = adminDB.Exec(`
		SELECT pg_terminate_backend(pid)
		FROM pg_stat_activity
		WHERE datname = $1 AND pid <> pg_backend_pid();`, cfg.DBName)

	// Safely quote the DB name
	quotedDBName := fmt.Sprintf(`"%s"`, cfg.DBName)

	// Drop the database
	_, err = adminDB.Exec(`DROP DATABASE IF EXISTS ` + quotedDBName)
	if err != nil {
		return fmt.Errorf("failed to drop database: %w", err)
	}

	// Recreate the database
	_, err = adminDB.Exec(`CREATE DATABASE ` + quotedDBName)
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	log.Printf("✅ Dropped and recreated database %s\n", cfg.DBName)
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

func ResetDatabase(db *gorm.DB) error {

	err := ResetSchema(db)
	if err != nil {
		return err
	}

	err = ResetSessionConfig(db)
	if err != nil {
		return err
	}

	err = ConfirmNoTables(db)
	if err != nil {
		return err
	}

	// GORM auto-migration to recreate tables
	err = db.AutoMigrate(
		&models.Resource{},
		&models.CommonRepresentation{},
		&models.ReporterRepresentation{},
		&models.RepresentationReference{},
	)
	if err != nil {
		return err
	}
	return err
}

func ResetSchema(db *gorm.DB) error {
	// Optional: Drop schema (PostgreSQL syntax)
	if err := db.Exec("DROP SCHEMA public CASCADE").Error; err != nil {
		return err
	}

	if err := db.Exec("CREATE SCHEMA public").Error; err != nil {
		return err
	}
	fmt.Println("✅ Dropped and recreated public schema")

	return nil
}

func ResetSessionConfig(db *gorm.DB) error {
	// Optional: reset planner/session settings
	err := db.Exec(`DISCARD ALL;`).Error
	if err != nil {
		return err
	}
	fmt.Println("✅ DISCARD ALL executed")
	err = db.Exec("RESET ALL").Error
	if err != nil {
		return err
	}

	fmt.Println("✅ RESET ALL executed")
	return err
}

func ConfirmNoTables(db *gorm.DB) error {
	// Confirm clean
	var tables []string
	if err := db.Raw(`SELECT tablename FROM pg_tables WHERE schemaname = 'public'`).Scan(&tables).Error; err != nil {
		return fmt.Errorf("failed to query tables: %w", err)
	}
	if len(tables) > 0 {
		return fmt.Errorf("❌ tables still exist after reset: %v", tables)
	}
	fmt.Println("✅ Verified: no user-defined tables remain")

	return nil
}
