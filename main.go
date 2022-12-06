package main

import (
	"context"
	"fmt"
	"log"

	_ "github.com/go-kivik/couchdb/v3"
	kivik "github.com/go-kivik/kivik/v3"
)

type Publication struct {
	ID       string   `json:"_id"`
	Rev      string   `json:"_rev"`
	Title    string   `json:"title"`
	Author   string   `json:"author"`
	Date     string   `json:"date"`
	Keywords []string `json:"keywords"`
	Abstract string   `json:"abstract"`
	Type     string   `json:"type"`
}

var couchdb *kivik.DB

func process() {
	rows, err := couchdb.AllDocs(context.TODO(), kivik.Options{"include_docs": true})
	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		var doc Publication

		if err := rows.ScanDoc(&doc); err != nil {
			panic(err)
		}

		handle(&doc)
	}

	if rows.Err() != nil {
		panic(rows.Err())
	}
}

// Handle does some work with our publication
func handle(doc *Publication) {
	// remove empty keywords
	for len(doc.Keywords) > 0 && doc.Keywords[0] == "" {
		doc.Keywords = doc.Keywords[1:]
		fmt.Printf("[%s]\tRemoved empty string from keywords, %d remaining\n", doc.ID, len(doc.Keywords))
	}

	// CAUTION: THIS DELETES ALL ATTACHMENTS AT THE MOMENT.
	//
	// Consider using an alternative method that retains _attachment
	_, err := couchdb.Put(context.TODO(), doc.ID, doc)
	if err != nil {
		panic(err)
	}

}

func main() {
	client, err := kivik.New("couch", "http://admin:password@localhost:5984/")
	if err != nil {
		log.Fatal(err)
	}

	couchdb = client.DB(context.TODO(), "literaturdatenbank_copy")

	process()
}
