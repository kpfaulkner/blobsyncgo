package main

import (
	"encoding/json"
	"fmt"
	"github.com/kpfaulkner/blobsyncgo/pkg/blobsync"
	"github.com/kpfaulkner/blobsyncgo/pkg/models"
	"log"
	"os"
	"os/user"
)

// read config from multiple locations.
// first try local dir...
// if fails, try ~/.blobsync/config.json
func readConfig() models.Config{
	var configFile *os.File
	var err error
	configFile, err = os.Open("config.json")
	if err != nil {
		// try and read home dir location.
		usr, err := user.Current()
		if err != nil {
			log.Fatal( err )
		}
		configPath := fmt.Sprintf("%s/.blobsync/config.json", usr.HomeDir)
		configFile, err = os.Open(configPath)
		if err != nil {
			log.Fatal( err )
		}
	}
	defer configFile.Close()

	config := models.Config{}
	jsonParser := json.NewDecoder(configFile)
	jsonParser.Decode(&config)

	return config
}


func main() {
	fmt.Printf("so it begins....\n")

	config := readConfig()
	bs := blobsync.NewBlobSync(config.AccountName, config.AccountKey)

	f,err := os.Open(`c:\temp\blobsync\test1.txt`)
	if err != nil {
		log.Fatalf("Unable to open file %s\n", err.Error())
	}

	bs.Upload(f, "blobsync", "test1.txt")

	sig, err := bs.DownloadSignatureForBlob("blobsync", "test1.txt")
	if err != nil {
		log.Fatalf("Unable to download sig %s\n", err.Error())
	}

	fmt.Printf("sig is %v\n", *sig)
}
