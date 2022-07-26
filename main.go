package main

import (
	"crypto/sha256"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"code.sajari.com/docconv"
	"github.com/go-sql-driver/mysql"
)

var db *sql.DB

// Represents fields of a publication in the database
type Publication struct {
	ID           uint
	Title        string
	Author       string
	Date         time.Time
	Keyword      sql.NullString
	Abstract     sql.NullString
	Path         string
	PdfHash      sql.NullString
	Type         string
	PathZip      sql.NullString
	ZipHash      sql.NullString
	PathImg      sql.NullString
	PathUrl      sql.NullString
	Password     sql.NullString
	Text         sql.NullString
	LastModified time.Time
}

// Reads the PDF of a publication and updates the database's full text record if needed
func updateText(p Publication) {

	contents, err := docconv.ConvertPath(p.Path)
	if err != nil {
		log.Println(err)
		return
	}

	if !p.Text.Valid || contents.Body != p.Text.String {
		_, err = db.Exec("UPDATE publications SET text = ? WHERE id = ?", contents.Body, p.ID)
		if err != nil {
			log.Println(err)
		} else {
			log.Printf("Updated full text for Document ID %d\n", p.ID)
		}
	}
}

// updateHash updates a Publications PDF hash if it doesn't exist yet.
func updateHash(p Publication) {
	if p.PdfHash.Valid {
		return
	}

	f, err := os.Open(p.Path)
	if err != nil {
		log.Panic(err)
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		log.Panic(err)
	}

	hash := h.Sum(nil)

	db.Exec("UPDATE publications SET pdf_hash = ? WHERE id = ?", hash, p.ID)
	log.Printf("Updated PDF Hash for Publication %d\n", p.ID)
}

// Performs various maintenance operations on a publication from the databse
func docHandler(p Publication) {
	// Fix path
	p.Path = "/data/" + p.Path

	updateHash(p)
	updateText(p)

	// TODO: more maintenance...
}

// Processes each row of the database
func processDB() {
	rows, err := db.Query("SELECT * FROM publications")
	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	// handle all rows retrieved
	for rows.Next() {
		var p Publication

		if err := rows.Scan(&p.ID, &p.Title, &p.Author, &p.Date, &p.Keyword, &p.Abstract, &p.Path, &p.Type, &p.PathZip, &p.PathImg, &p.PathUrl, &p.Password, &p.Text); err != nil {
			log.Println(err)
		}

		go docHandler(p)
	}
}

func main() {
	time.Sleep(10 * time.Second)
	fmt.Println("Starting Beisetzer...")
	cfg := mysql.Config{
		User:                 os.Getenv("DB_USER"),
		Passwd:               os.Getenv("DB_PASSWORD"),
		Net:                  "tcp",
		Addr:                 "db:3306",
		DBName:               "ikm",
		AllowNativePasswords: true,
	}

	// Get a database handle
	var err error
	db, err = sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		log.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	db.SetMaxOpenConns(64)

	log.Println("Connected!")

	processDB()

	for range time.Tick(time.Hour) {
		processDB()
	}
}
