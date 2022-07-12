package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/ledongthuc/pdf"
)

var db *sql.DB

// Represents fields of a publication in the database
type Publication struct {
	ID       uint
	Title    string
	Author   string
	Year     uint
	Keyword  sql.NullString
	Abstract sql.NullString
	Path     string
	Type     string
	PathZip  sql.NullString
	PathImg  sql.NullString
	PathUrl  sql.NullString
	Password sql.NullString
	Text     sql.NullString
}

// Reads the PDF of a publication and updates the database's full text record if needed
func updateText(p Publication) {

	contents, err := ReadPdf(p.Path)
	if err != nil {
		log.Printf("Could not extract text from PDF for Document ID %d\n", p.ID)
		return
	}

	if !p.Text.Valid || contents != p.Text.String {
		_, err = db.Exec("UPDATE publications SET text = ? WHERE id = ?", contents, p.ID)
		if err != nil {
			log.Println(err)
		} else {
			fmt.Printf("Updated full text for Document ID %d\n", p.ID)
		}
	}
}

// Extract the plain text of a PDF
func ReadPdf(path string) (string, error) {
	f, r, err := pdf.Open(path)
	// remember close file
	defer f.Close()
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	b, err := r.GetPlainText()
	if err != nil {
		return "", err
	}
	buf.ReadFrom(b)
	return buf.String(), nil
}

// Performs various maintenance operations on a publication from the databse
func docHandler(p Publication) {
	// Fix path
	p.Path = "/data/" + p.Path

	updateText(p)

	// TODO: more maintenance...
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

	log.Println("Connected!")

	for range time.Tick(time.Minute) {

		rows, err := db.Query("SELECT * FROM publications")
		if err != nil {
			log.Fatal(err)
		}

		defer rows.Close()

		// handle all rows retrieved
		for rows.Next() {
			var p Publication

			if err := rows.Scan(&p.ID, &p.Title, &p.Author, &p.Year, &p.Keyword, &p.Abstract, &p.Path, &p.Type, &p.PathZip, &p.PathImg, &p.PathUrl, &p.Password, &p.Text); err != nil {
				log.Println(err)
			}

			go docHandler(p)
		}
	}
}
