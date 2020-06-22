package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/kpfaulkner/blobsyncgo/pkg/blobsync"
	"github.com/kpfaulkner/blobsyncgo/pkg/signatures"
	"log"
	"os"
	"os/user"
)

// read config from multiple locations.
// first try local dir...
// if fails, try ~/.blobsync/config.json
func readConfig() signatures.Config {
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

	config := signatures.Config{}
	jsonParser := json.NewDecoder(configFile)
	jsonParser.Decode(&config)

	return config
}


func main() {
	fmt.Printf("so it begins....\n")

	filePath := flag.String("file", "", "path to file to upload")
	blobName := flag.String("blob", "", "name of blob")
	containerName := flag.String("container", "", "name of container")

	flag.Parse()

	if *filePath == "" || *blobName == "" || *containerName == "" {
		fmt.Printf("Error....\n")
	}

	config := readConfig()
	bs := blobsync.NewBlobSync(config.AccountName, config.AccountKey)

	f,err := os.Open(*filePath)
	if err != nil {
		log.Fatalf("Unable to open file %s\n", err.Error())
	}

	bs.Upload(f, *containerName, *blobName)

}
