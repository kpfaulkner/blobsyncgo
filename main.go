package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/kpfaulkner/blobsyncgo/pkg/blobsync"
	"github.com/kpfaulkner/blobsyncgo/pkg/signatures"
	"log"
	"net/http"
	_ "net/http/pprof"
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

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	download := flag.Bool("download", false, "Download blob to local, merging with file indicated")
	upload := flag.Bool("upload", false, "Upload file specified to blob/container")
	filePath := flag.String("file", "", "path to file to upload")
	blobName := flag.String("blob", "", "name of blob")
	containerName := flag.String("container", "", "name of container")
	verbose := flag.Bool("verbose", false, "verbose")

	flag.Parse()

	if *filePath == "" || *blobName == "" || *containerName == "" {
		fmt.Printf("Error....\n")
		return
	}

	if !(*download) && !(*upload) {
		fmt.Printf("Need to specify upload or downloads\n")
		return
	}

	config := readConfig()
	bs := blobsync.NewBlobSync(config.AccountName, config.AccountKey)

	if *upload {
		f, err := os.Open(*filePath)
		if err != nil {
			log.Fatalf("Unable to open file %s\n", err.Error())
		}

		bs.Upload(f, *containerName, *blobName, *verbose)
	}

	if *download {

		err := bs.Download(*filePath, *containerName, *blobName, *verbose)
		if err != nil {
			fmt.Printf("ERROR while downloading : %s\n", err.Error())
		}
	}

}
