package main

import (
	"fmt"
	"github.com/kpfaulkner/blobsyncgo/pkg/blobsync"
	"log"
	"os"
)

func main() {
	fmt.Printf("so it begins....\n")

	accountName := "kenfau"
	accountKey := "EGcmJxoBbacqb9/rve0Q1d309yINOhFWhNURPGVyS0bgJUSp5RklhgjE/G3sDljg+S2nm8qpaz2Y9M8ufjYJWA=="
	bs := blobsync.NewBlobSync(accountName, accountKey)

	f,err := os.Open(`c:\temp\blobsync\test1.txt`)
	if err != nil {
		log.Fatalf("Unable to open file %s\n", err.Error())
	}

	bs.Upload(f, "blobsync", "test1.txt")
}
