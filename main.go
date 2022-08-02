package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"

	"code.sajari.com/docconv"
	"github.com/go-sql-driver/mysql"
	pdfcpu "github.com/pdfcpu/pdfcpu/pkg/api"
)

// DataDir is the directory containing all actual data files
const DataDir = "/data"

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

// Uses the largest image from a publication's PDF as the default image
func setImage(p Publication) {
	if p.PathImg.Valid {
		return
	}

	// Create a directory to dump everything into
	imgdir, err := os.MkdirTemp("", "litdb")
	if err != nil {
		log.Println(err)
	}
	defer os.RemoveAll(imgdir)

	// Dump the images
	err = pdfcpu.ExtractImagesFile(p.Path, imgdir, nil, nil)
	if err != nil {
		log.Print(err)
		return
	}

	// Sort images by size
	files, err := os.ReadDir(imgdir)
	if err != nil {
		log.Print(err)
		return
	}

	if len(files) < 1 {
		return
	}

	sort.Slice(files, func(i, j int) bool {
		first, err := files[i].Info()
		if err != nil {
			log.Print(err)
		}

		second, err := files[j].Info()
		if err != nil {
			log.Print(err)
		}

		return first.Size() < second.Size()
	})

	// Get the name and path of the largest file
	largest := files[len(files)-1].Name()
	largestPath := filepath.Join(imgdir, largest)

	// Grab the directory of the publication's other files
	baseDir := filepath.Dir(p.Path)
	targetPath := filepath.Join(baseDir, "auto-"+largest)

	// Move the image file
	err = os.Rename(largestPath, targetPath)
	if err != nil {
		fmt.Print(err)
		return
	}

	// Set it in the database
	_, err = db.Exec("UPDATE publications SET path_img = ? WHERE id = ?", targetPath, p.ID)
	if err != nil {
		log.Print(err)
	} else {
		log.Printf("Generated image for Document ID %d\n", p.ID)
	}
}

// updateHash updates a Publications PDF hash if it doesn't exist yet.
func updateHash(p Publication) {
	if p.PdfHash.Valid {
		return
	}

	f, err := os.Open(p.Path)
	if err != nil {
		log.Print(err)
		return
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		log.Print(err)
		return
	}

	hash := hex.EncodeToString(h.Sum(nil))

	_, err = db.Exec("UPDATE publications SET pdf_hash = ? WHERE id = ?", hash, p.ID)
	if err != nil {
		log.Println(err)
	} else {
		log.Printf("Updated PDF Hash for Publication %d\n", p.ID)
	}
}

// Performs various maintenance operations on a publication from the databse
func docHandler(p Publication) {
	// Fix path
	absPath, err := filepath.Abs(filepath.Join(DataDir, p.Path))
	if err != nil {
		log.Print(err)
		return
	}

	p.Path = absPath

	updateHash(p)
	updateText(p)
	setImage(p)
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

		if err := rows.Scan(&p.ID, &p.Title, &p.Author, &p.Date, &p.Keyword, &p.Abstract, &p.Path, &p.PdfHash,
			&p.Type, &p.PathZip, &p.ZipHash, &p.PathImg, &p.PathUrl, &p.Password, &p.Text, &p.LastModified); err != nil {
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
		ParseTime:            true,
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
