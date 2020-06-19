package blobsync

import (
	"fmt"
	"github.com/kpfaulkner/blobsyncgo/pkg/azureutils"
	"os"
)

type BlobSync struct {

	// creds.
	blobAccountName string
	blobKey string

	// container
	blobContainer string

	// blob pipeline....  now the mechanism for accessing blobs.
	blobHandler azureutils.BlobHandler
}

func NewBlobSync(accountName string, accountKey string) BlobSync {
	bs := BlobSync{}
	bs.blobAccountName = accountName
	bs.blobKey = accountKey
	bs.blobHandler = azureutils.NewBlobHandler(accountName, accountKey)

	return bs
}

// Upload will upload the data from a reader.
// It will return the signature of the file uploaded.
// or an error if something went boom.
func (bs BlobSync) Upload(localFile *os.File, containerName string, blobName string ) (*Signature, error) {

	// does blob already exist?
	doesBlobExist := false  // in reality, check in Azure.
  doesSigExist := false // in reality, also check azure :)

  if doesBlobExist && doesSigExist {
  	// doing the tricky stuff.
  } else {

		err := bs.uploadAsNewBlob(localFile, containerName, blobName)
		if err != nil {
			fmt.Printf("Cannot upload as new blob: %s\n", err.Error())
			return nil, err
		}

		sig, err := bs.generateSig(localFile)
		if err != nil {
			fmt.Printf("Cannot generate sig:  %s\n", err.Error())
			return nil, err
		}

		err = bs.uploadSig(*sig, blobName)
		if err != nil {
			fmt.Printf("Cannot upload sig:  %s\n", err.Error())
			return nil, err
		}
	}

	return nil, nil
}

func (bs BlobSync) uploadAsNewBlob(localFile *os.File, containerName, blobName string) error {

	bs.blobHandler.CreateContainerURL(containerName)
	err := bs.blobHandler.UploadBlob(localFile, containerName, blobName)
	if err != nil {
		fmt.Printf("Unable to upload blob %s\n", err.Error())
		return err
	}

	return nil
}

func (bs BlobSync) generateSig(localFile *os.File) (*Signature, error) {

	return nil, nil
}


func (bs BlobSync) uploadSig(sig Signature, blobName string) error {

	return nil
}
