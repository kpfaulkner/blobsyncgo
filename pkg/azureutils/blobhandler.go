package azureutils

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/edsrzf/mmap-go"
	"github.com/kpfaulkner/blobsyncgo/pkg/signatures"
	"sort"
	"strings"
	"time"

	"io"
	"log"
	"net/url"
	"os"
)

type UploadMessage struct {
	Offset int64
	BytesRead int
	Data []byte
}

type BlobHandler struct {
	accountName string
	accountKey string
	blobPipeline pipeline.Pipeline

	TotalBytesUploaded int64
	TotalBytesDownloaded int64
}

func NewBlobHandler(accountName string, accountKey string ) BlobHandler {
	bh := BlobHandler{}
	bh.accountName = accountName
	bh.accountKey = accountKey
	bh.blobPipeline = createBlobClientPipeline(accountName, accountKey)
	bh.TotalBytesUploaded = 0
	bh.TotalBytesDownloaded = 0
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
  	//fmt.Printf("trying to create container that already exists (possibly) : %s\n", err.Error())
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
func (bh *BlobHandler) WriteBytesWithChannel( dataCh chan UploadMessage, uploadedBlockCh chan signatures.UploadedBlock, blobURL *azblob.BlockBlobURL) error {

	for data := range dataCh {
		if data.BytesRead == 0 {
			fmt.Printf("no bytes!!!\n")
		}
		sig, err := signatures.GenerateBlockSig(data.Data, data.Offset, data.BytesRead, 0)
		if err != nil {
			log.Fatalf("Unable to generate block sig %s\n", err.Error())
		}

		blockID := base64.StdEncoding.EncodeToString(sig.MD5Signature[:])
		newBlock := signatures.UploadedBlock{
			BlockID:     blockID,
			Offset:      data.Offset,
			Sig:         *sig,
			Size:        int64(data.BytesRead),
			IsNew:       true,
			IsDuplicate: false}

		// not a dupe, upload it.
		ctx := context.Background() // This example uses a never-expiring context
		_, err = blobURL.StageBlock(ctx, blockID, bytes.NewReader(data.Data), azblob.LeaseAccessConditions{}, nil)
		if err != nil {
			log.Fatalf("Unable to stage block: %s\n", err.Error())
		}
		fmt.Printf("uploaded offset %d\n", data.Offset)
		bh.TotalBytesUploaded += int64(data.BytesRead)
		uploadedBlockCh <- newBlock
	}

	return nil
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
																 containerName string, blobName string, verbose bool ) error  {

	stats, _ := localFile.Stat()
	remainingBytes := signatures.RemainingBytes{BeginOffset: 0, EndOffset: stats.Size() - 1}

	uploadBlockList, err := bh.UploadRemainingBytesAsBlocks(remainingBytes, localFile, containerName, blobName, verbose)
	if err != nil {
		return err
	}

	sort.Slice(uploadBlockList, func (i int, j int) bool {
		return uploadBlockList[i].Offset < uploadBlockList[j].Offset
	})

	err = bh.PutBlockList(uploadBlockList, containerName, blobName)
	return err
}

func (bh BlobHandler) launchConcurrentUploader(dataCh chan UploadMessage, uploadedBlockCh chan signatures.UploadedBlock, blobURL *azblob.BlockBlobURL) {

	maxUploaders := 100
	for i:=0; i<maxUploaders;i++ {
		go bh.WriteBytesWithChannel(dataCh, uploadedBlockCh, blobURL)
	}
}

// UploadBlobAsBlocks. Using os.File instead of a reader since want to use the UploadFileToBlockBlob
// instead of stream (which uses reader).
// Upload remaining bytes as blocks. If need be, break remainingBytes into blocks that are default SignatureSize in length.
// Only uploading sequentially for the moment
/// Will optimise with a parallel version later.
func (bh BlobHandler) UploadRemainingBytesAsBlocks( remainingBytes signatures.RemainingBytes, localFile *os.File,
																										containerName string, blobName string, verbose bool ) ([]signatures.UploadedBlock, error ){

  containerURL,_ := bh.CreateContainerURL(containerName)
	blobURL := containerURL.NewBlockBlobURL(blobName)
	uploadedBlockList := []signatures.UploadedBlock{}

	mm,err  := mmap.Map(localFile, mmap.RDONLY, 0)
	if err != nil {
		log.Fatalf("Unable to mmap the file: %s\n", err.Error())
	}
	defer mm.Unmap()

	// loop and write in blocks.
	offset := remainingBytes.BeginOffset
  total := 0
  dataCh := make(chan UploadMessage,100)

  // stupid large channel until I sort this shit out.
	uploadedBlockCh := make(chan signatures.UploadedBlock,100000)

	concurrentUpload := true
	// check if blobname ends in sig. Only parallel upload for non-sigs TODO(kpfaulkner) search out the bug.
	if strings.Contains(blobName, ".sig") {
		concurrentUpload = false
	}

	if concurrentUpload {
		bh.launchConcurrentUploader(dataCh, uploadedBlockCh, &blobURL)
	}

	// make out of loop
	//bytesToRead := make([]byte, signatures.SignatureSize)

	for offset <= remainingBytes.EndOffset {

		var sizeToRead int64
		if remainingBytes.EndOffset - offset +1 > int64(signatures.SignatureSize) {
			sizeToRead = int64(signatures.SignatureSize)
		} else {
			sizeToRead = remainingBytes.EndOffset - offset +1
			//bytesToRead = make([]byte, sizeToRead)
		}

		if sizeToRead > 0 {

			//buffer, _ := PopulateBuffer(&mm,offset, sizeToRead, remainingBytes.EndOffset)
			buffer := mm[offset:offset + sizeToRead]
      bytesRead := len(buffer)
			if bytesRead == 0 {
				break // ??? good idea?
			}

			if concurrentUpload {
				msg := UploadMessage{Data: buffer, Offset: offset, BytesRead: bytesRead}
				dataCh <- msg
			} else {
				uploadedBlock, err := bh.WriteBytes(offset, bytesRead, buffer, &blobURL, uploadedBlockList)
				if err != nil {
					return nil, err
				}
				total += int(bytesRead)
				if verbose {
					fmt.Printf("Uploaded %d : total %d\n", bytesRead, total)
				}
				uploadedBlockList = append(uploadedBlockList, *uploadedBlock)
			}

			offset += sizeToRead
		}
	}
	close(dataCh)

	if concurrentUpload {
		done := false
		for !done {

			select {
			  case uploadedBlock := <- uploadedBlockCh:
				  uploadedBlockList = append(uploadedBlockList, uploadedBlock)
				  case <- time.After(5 * time.Second):
				  	done = true
			}
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


func (bh BlobHandler) BlobExist( containerName string, blobName string) bool {
	containerURL,_ := bh.CreateContainerURL(containerName)
	blobURL := containerURL.NewBlobURL(blobName)

	ctx := context.Background() // This example uses a never-expiring context
	_, err := blobURL.GetProperties(ctx, azblob.BlobAccessConditions{})

	if err != nil {
		azErr := err.(azblob.StorageError)
		return azErr.Response().StatusCode != 404
	}

  return true
}


func (bh BlobHandler) DownloadBlob( file *os.File, containerName string, blobName string) error {
	containerURL,_ := bh.CreateContainerURL(containerName)
	blobURL := containerURL.NewBlobURL(blobName)
	ctx := context.Background() // This example uses a never-expiring context
	err := azblob.DownloadBlobToFile(ctx, blobURL, 0, azblob.CountToEnd, file, azblob.DownloadFromBlobOptions{})
	return err
}

func (bh BlobHandler) DownloadBlobToBuffer( buffer *bytes.Buffer, containerName string, blobName string) error {
	return bh.DownloadBlobRange(buffer, containerName, blobName, 0, azblob.CountToEnd)
}

// DownloadBlobRange downloads a subsection of a blob.
func (bh *BlobHandler) DownloadBlobRange(  buffer *bytes.Buffer, containerName string, blobName string, beginOffset int64, endOffset int64) error {
	containerURL,_ := bh.CreateContainerURL(containerName)
	blobURL := containerURL.NewBlobURL(blobName)

	ctx := context.Background() // This example uses a never-expiring context

	count := int64(0)

	// only recalculate count if NOT reading to end of file.
	if endOffset != azblob.CountToEnd {
		count = endOffset - beginOffset +1
	}

	downloadResponse, err := blobURL.Download(ctx, beginOffset, count, azblob.BlobAccessConditions{}, false)
	bodyStream := downloadResponse.Body(azblob.RetryReaderOptions{MaxRetryRequests: 20})
	_, err = buffer.ReadFrom(bodyStream)

	bh.TotalBytesDownloaded += count
	fmt.Printf("downloaded %d, total of %d\n", count, bh.TotalBytesDownloaded)

	return err
}

