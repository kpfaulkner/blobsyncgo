package blobsync

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/kpfaulkner/blobsyncgo/pkg/azureutils"
	"io/ioutil"
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

	// signatures...
	signatureHandler SignatureHandler
}

func NewBlobSync(accountName string, accountKey string) BlobSync {
	bs := BlobSync{}
	bs.blobAccountName = accountName
	bs.blobKey = accountKey
	bs.blobHandler = azureutils.NewBlobHandler(accountName, accountKey)
  bs.signatureHandler = NewSignatureHandler()

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

		err = bs.uploadSig(sig, containerName, blobName)
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

func (bs BlobSync) generateSig(localFile *os.File) (*SizeBasedCompleteSignature, error) {

	// rewind to begining of file.
	localFile.Seek(0,0)

	sig, err := CreateSignatureFromScratch(localFile)
	if err != nil {
		fmt.Printf("Cannot create signature %s\n", err.Error())
		return nil, err
	}
	return sig,nil
}


func (bs BlobSync) uploadSig(sig *SizeBasedCompleteSignature, containerName string, blobName string) error {

	sigBytes, _ := json.Marshal(sig)

	// write to temp file? seems silly, but cant get streaming working.
	_ = ioutil.WriteFile(`c:\temp\temp.sig`, sigBytes, 0644)
	f,_ := os.Open(`c:\temp\temp.sig`)
	bs.blobHandler.UploadBlob(f, containerName, blobName+".sig")

	return nil
}

// DownloadBlobToFile downloads blob and stores at localFilePath size.
// Not attempting interfaces yet, dont want the risk of a 2G blob being stored into memory :)
func (bs BlobSync) DownloadBlobToFile( localFilePath string, containerName string, blobName string ) error {

	f, err := os.Create(localFilePath)
	defer f.Close()

	if err != nil {
		fmt.Printf("Cannot open %s file for writing: %s\n", localFilePath, err.Error())
		return err
	}

	err = bs.blobHandler.DownloadBlob(f, containerName, blobName)
	if err != nil {
		fmt.Printf("Cannot download blob:  %s\n", err.Error())
		return err
	}

	return nil
}

// DownloadSignatureForBlob. Takes the blob name, appends the ".sig" to it
// returns the signature
func (bs BlobSync) DownloadSignatureForBlob( containerName string, blobName string ) (*SizeBasedCompleteSignature, error) {

	buffer := bytes.Buffer{}

	err := bs.blobHandler.DownloadBlobToBuffer(&buffer, containerName, blobName+".sig")
	if err != nil {
		fmt.Printf("Cannot download signature for blob %s : %s\n", blobName, err.Error())
		return nil, err
	}

	var sig SizeBasedCompleteSignature
	err = json.Unmarshal(buffer.Bytes(), &sig)
	if err != nil {
		fmt.Printf("Cannot unmarshal signature : %s\n",  err.Error())
		return nil, err
	}

	return &sig, nil

}

