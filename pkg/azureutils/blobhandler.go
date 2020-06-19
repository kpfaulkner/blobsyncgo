package azureutils

import (
	"context"
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"io"
	"log"
	"net/url"
	"os"
)

type BlobHandler struct {
	accountName string
	accountKey string
	blobPipeline pipeline.Pipeline
}

func NewBlobHandler(accountName string, accountKey string ) BlobHandler {
	bh := BlobHandler{}
	bh.accountName = accountName
	bh.accountKey = accountKey
	bh.blobPipeline = createBlobClientPipeline(accountName, accountKey)
	return bh
}

func createBlobClientPipeline(accountName string, accountKey string)  pipeline.Pipeline {
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal("Invalid credentials with error: " + err.Error())
	}
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})
	return p
}


func (bh BlobHandler) CreateContainerURL( containerName string ) (*azblob.ContainerURL, error) {
	URL, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s", bh.accountName, containerName))
	containerURL := azblob.NewContainerURL(*URL, bh.blobPipeline)
	ctx := context.Background() // This example uses a never-expiring context
	_, err := containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)

  if err != nil {
  	fmt.Printf("trying to create container that already exists (possibly) : %s\n", err.Error())
  	//return nil, err
  }

  return &containerURL, nil
}

// UploadBlob. Using os.File instead of a reader since want to use the UploadFileToBlockBlob
// instead of stream (which uses reader). Believe file allows easier concurrent upload of blocks.
func (bh BlobHandler) UploadBlob( file *os.File, containerName string, blobName string ) error {

  containerURL,_ := bh.CreateContainerURL(containerName)

	blobURL := containerURL.NewBlockBlobURL(blobName)

	ctx := context.Background() // This example uses a never-expiring context

	_, err := azblob.UploadFileToBlockBlob(ctx, file, blobURL, azblob.UploadToBlockBlobOptions{
																				BlockSize:   4 * 1024 * 1024,
																				Parallelism: 16})

  return err
}

func (bh BlobHandler) UploadBlobFromReader( reader io.Reader, containerName string, blobName string ) error {

	containerURL,_ := bh.CreateContainerURL(containerName)
	blobURL := containerURL.NewBlockBlobURL(blobName)
	ctx := context.Background() // This example uses a never-expiring context

	// dont use for big files... unsure about concurrency here.
	_, err := azblob.UploadStreamToBlockBlob(ctx, reader, blobURL, azblob.UploadStreamToBlockBlobOptions{ BufferSize: 100000})
	return err
}



func (bh BlobHandler) DownloadBlob( file *os.File, containerName string, blobName string) error {
	containerURL,_ := bh.CreateContainerURL(containerName)
	blobURL := containerURL.NewBlobURL(blobName)
	ctx := context.Background() // This example uses a never-expiring context
	err := azblob.DownloadBlobToFile(ctx, blobURL, 0, azblob.CountToEnd, file, azblob.DownloadFromBlobOptions{})
	return err
}
