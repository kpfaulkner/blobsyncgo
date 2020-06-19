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
	accountKey := ""
	bs := blobsync.NewBlobSync(accountName, accountKey)

	f,err := os.Open(`c:\temp\blobsync\test1.txt`)
	if err != nil {
		log.Fatalf("Unable to open file %s\n", err.Error())
	}

	bs.Upload(f, "blobsync", "test1.txt")
}
