package azureutils

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/kpfaulkner/blobsyncgo/pkg/signatures"
	"sort"

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

func checkIfDupe(uploadedBlockList []signatures.UploadedBlock, blockID string ) bool {
  for _,ub := range uploadedBlockList {
    if ub.BlockID == blockID {
    	return true
    }
  }
  return false
}


func (bh BlobHandler) PutBlockList( uploadedBlockList []signatures.UploadedBlock, containerName string, blobName string ) error {
	containerURL,_ := bh.CreateContainerURL(containerName)
	blobURL := containerURL.NewBlockBlobURL(blobName)

	blockIDs := []string{}
	for _,b := range uploadedBlockList {
		blockIDs = append(blockIDs, b.BlockID)
	}
	ctx := context.Background() // This example uses a never-expiring context
	_, err := blobURL.CommitBlockList(ctx, blockIDs,azblob.BlobHTTPHeaders{}, nil, azblob.BlobAccessConditions{} )
	return err

}


// WriteBytes, returns an UploadedBlock struct, giving a summary
func (bh BlobHandler) WriteBytes( offset int64, bytesRead int, data []byte, blobURL *azblob.BlockBlobURL, uploadedBlockList []signatures.UploadedBlock) (*signatures.UploadedBlock, error ) {

	sig,err := signatures.GenerateBlockSig(data, offset, bytesRead, 0 )
	if err != nil {
		return nil, err
	}

	blockID := base64.StdEncoding.EncodeToString(sig.MD5Signature[:])

	isDupe := checkIfDupe(uploadedBlockList, blockID)

	newBlock := signatures.UploadedBlock{
		BlockID: blockID,
		Offset: offset,
		Sig : *sig,
		Size : int64(bytesRead),
		IsNew: true,
		IsDuplicate : isDupe}

	// not a dupe, upload it.
	if !isDupe {
		ctx := context.Background() // This example uses a never-expiring context
		_, err = blobURL.StageBlock(ctx, blockID, bytes.NewReader(data), azblob.LeaseAccessConditions{}, nil )
		if err != nil {
			return nil, err
		}
	}

	return &newBlock, nil
}

func (bh BlobHandler) UploadBlob(localFile *os.File,
	containerName string, blobName string ) error  {

	stats, _ := localFile.Stat()
	remainingBytes := signatures.RemainingBytes{BeginOffset: 0, EndOffset: stats.Size() - 1}

	uploadBlockList, err := bh.UploadRemainingBytesAsBlocks(remainingBytes, localFile, containerName, blobName)
	if err != nil {
		return err
	}

	sort.Slice(uploadBlockList, func (i int, j int) bool {
		return uploadBlockList[i].Offset < uploadBlockList[j].Offset
	})

	err = bh.PutBlockList(uploadBlockList, containerName, blobName)
	return err
}


// UploadBlobAsBlocks. Using os.File instead of a reader since want to use the UploadFileToBlockBlob
// instead of stream (which uses reader).
// Upload remaining bytes as blocks. If need be, break remainingBytes into blocks that are default SignatureSize in length.
// Only uploading sequentially for the moment
/// Will optimise with a parallel version later.
func (bh BlobHandler) UploadRemainingBytesAsBlocks( remainingBytes signatures.RemainingBytes, localFile *os.File,
																										containerName string, blobName string ) ([]signatures.UploadedBlock, error ){

  containerURL,_ := bh.CreateContainerURL(containerName)
	blobURL := containerURL.NewBlockBlobURL(blobName)
	uploadedBlockList := []signatures.UploadedBlock{}

	// loop and write in blocks.
	offset := remainingBytes.BeginOffset

	for offset <= remainingBytes.EndOffset {

		var sizeToRead int64
		if remainingBytes.EndOffset - offset +1 > int64(signatures.SignatureSize) {
			sizeToRead = int64(signatures.SignatureSize)
		} else {
			sizeToRead = remainingBytes.EndOffset - offset +1
		}

		if sizeToRead > 0 {
			localFile.Seek(offset,0)
			bytesToRead := make([]byte, sizeToRead)

			bytesRead, err := localFile.ReadAt(bytesToRead, offset)
			if err != nil {
				return nil, err
			}

			uploadedBlock, err := bh.WriteBytes( offset, bytesRead, bytesToRead, &blobURL, uploadedBlockList  )
			if err != nil {
				return nil, err
			}
			uploadedBlockList = append(uploadedBlockList, *uploadedBlock)
			offset += sizeToRead
		}
	}

  return uploadedBlockList, nil
}

func (bh BlobHandler) UploadBlobFromReader( reader io.Reader, containerName string, blobName string ) error {

	containerURL,_ := bh.CreateContainerURL(containerName)
	blobURL := containerURL.NewBlockBlobURL(blobName)
	ctx := context.Background() // This example uses a never-expiring context

	// dont use for big files... unsure about concurrency here.
	_, err := azblob.UploadStreamToBlockBlob(ctx, reader, blobURL, azblob.UploadStreamToBlockBlobOptions{ BufferSize: 100000})
	return err
}

/*
func (bh BlobHandler) SetBlobAttribute(containerName string, blobName string ) error {

	containerURL,_ := bh.CreateContainerURL(containerName)
	blobURL := containerURL.NewBlockBlobURL(blobName)

	ctx := context.Background() // This example uses a never-expiring context

	// dont use for big files... unsure about concurrency here.
	_, err := azblob.UploadStreamToBlockBlob(ctx, reader, blobURL, azblob.UploadStreamToBlockBlobOptions{ BufferSize: 100000})
	return err
}  */


func (bh BlobHandler) DownloadBlob( file *os.File, containerName string, blobName string) error {
	containerURL,_ := bh.CreateContainerURL(containerName)
	blobURL := containerURL.NewBlobURL(blobName)
	ctx := context.Background() // This example uses a never-expiring context
	err := azblob.DownloadBlobToFile(ctx, blobURL, 0, azblob.CountToEnd, file, azblob.DownloadFromBlobOptions{})
	return err
}

func (bh BlobHandler) DownloadBlobToBuffer( buffer *bytes.Buffer, containerName string, blobName string) error {
	containerURL,_ := bh.CreateContainerURL(containerName)
	blobURL := containerURL.NewBlobURL(blobName)

	ctx := context.Background() // This example uses a never-expiring context
	downloadResponse, err := blobURL.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false)
	bodyStream := downloadResponse.Body(azblob.RetryReaderOptions{MaxRetryRequests: 20})
	_, err = buffer.ReadFrom(bodyStream)
	return err
}

