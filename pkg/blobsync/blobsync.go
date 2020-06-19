package blobsync

import (
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/kpfaulkner/blobsyncgo/pkg/azureutils"
	"io"
)

type BlobSync struct {

	// creds.
	blobAccountName string
	blobKey string

	// container
	blobContainer string

	// blob pipeline....  now the mechanism for accessing blobs.
	blobPipeline pipeline.Pipeline
}

func NewBlobSync(accountName string, accountKey string) BlobSync {
	bs := BlobSync{}
	bs.blobAccountName = accountName
	bs.blobKey = accountKey
	bs.blobPipeline = azureutils.CreateBlobClientPipeline(accountName, accountKey)

	return bs
}

// Upload will upload the data from a reader.
// It will return the signature of the file uploaded.
// or an error if something went boom.
func (bs BlobSync) Upload(localFile io.Reader, containerName string, blobName string ) (*Signature, error) {

	// does blob already exist?
	doesBlobExist := false  // in reality, check in Azure.
  doesSigExist := false // in reality, also check azure :)

  if doesBlobExist && doesSigExist {
  	// doing the tricky stuff.
  } else {

		err := bs.uploadAsNewBlob(localFile, blobName)
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
	} else {

	}
	return nil, nil
}

func (bs BlobSync) uploadAsNewBlob(localFile io.Reader, blobName string) error {

	return nil
}

func (bs BlobSync) generateSig(localFile io.Reader) (*Signature, error) {

	return nil, nil
}


func (bs BlobSync) uploadSig(sig Signature, blobName string) error {

	return nil
}
