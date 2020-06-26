package blobsync

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/kpfaulkner/blobsyncgo/pkg/azureutils"
	"github.com/kpfaulkner/blobsyncgo/pkg/signatures"
	"io"
	"io/ioutil"
	"os"
	"sort"
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
	signatureHandler signatures.SignatureHandler
}

func NewBlobSync(accountName string, accountKey string) BlobSync {
	bs := BlobSync{}
	bs.blobAccountName = accountName
	bs.blobKey = accountKey
	bs.blobHandler = azureutils.NewBlobHandler(accountName, accountKey)
  bs.signatureHandler = signatures.NewSignatureHandler()

	return bs
}

func (bs BlobSync) doesFileExist(localFilePath string ) bool {
	info, err := os.Stat(localFilePath)
	if os.IsNotExist(err) {
		return false
	}

	return !info.IsDir()
}

func (bs BlobSync) Download(localFilePath string, containerName string, blobName string, verbose bool ) error {

	if bs.doesFileExist(localFilePath) {
		// download sig for blob
		blobSig, err := bs.DownloadSignatureForBlob(containerName, blobName)
		if err != nil {
			fmt.Printf("Unable to get sig for blob %s : %s\n", blobName, err)
			return err
		}

		// search local file for blob sig details
		localFile,_ := os.Open(localFilePath)

		/*
    localSig, err := bs.generateSig(localFile)
		if err != nil {
			fmt.Printf("Cannot create signature %s\n", err.Error())
			return err
		} */

		searchResults, err := SearchLocalFileForSignatureForDownload( localFile, *blobSig )
		if err != nil {
			return err
		}

		byteRangesToDownload,err := bs.GenerateByteRangesOfBlobToDownload(searchResults.SignaturesToReuse, blobSig, containerName, blobName)
		if err != nil {
			return err
		}

		bs.RegenerateBlob(containerName, blobName, byteRangesToDownload, localFilePath, searchResults.SignaturesToReuse, blobSig)


		// regenerate blob

	} else {
		// download entire file.
		err := bs.DownloadBlobToFile( localFilePath, containerName, blobName)
    if err != nil {
    	return err
    }
	}

	return nil
}

func (bs BlobSync) RegenerateBlob(containerName string, blobName string, byteRangesToDownload []signatures.RemainingBytes,
										localFilePath string, reusableBlockSignatures []signatures.BlockSig, blobSig *signatures.SizeBasedCompleteSignature) error {

	allBlobSigs := signatures.ExpandSizeBasedCompleteSignature(*blobSig)
	reusableBlobLUT := generateBlockLUTFromBlockSigs(reusableBlockSignatures)
  offset := int64(0)

  localFile,_ := os.Open(localFilePath)
  newFile,_ := os.Create(localFilePath+".new")

  for _,sig := range allBlobSigs {

  	if sig.Size < 20000 {
  		fmt.Printf("here")
	  }

  	haveMatch := false
  	localSig, ok := reusableBlobLUT[sig.RollingSig]
  	if ok {
		  matchingLocalSig, hasMatch := returnMatchingSig( localSig, sig)
			if hasMatch {
				buffer := make([]byte, matchingLocalSig.Size)
				localFile.Seek( matchingLocalSig.Offset,0)
				bytesRead, err := localFile.Read(buffer)
				if err != nil {
					return err
				}
				fmt.Printf("%d bytes read\n", bytesRead)
				/*
				if bytesRead != matchingLocalSig.Size {
					return errors.New("Unable to read correct length of file.")
				}
        */
				newFile.Seek(sig.Offset,0)
				bytesWritten, err := newFile.Write(buffer)
				if err != nil {
					return err
				}
				if bytesWritten != matchingLocalSig.Size {
					return errors.New("Unable to write correct length of file.")
				}

				haveMatch = true
				offset += int64(matchingLocalSig.Size)
			}
	  }

	  if !haveMatch{
	  	byteRange,ok := getByteRangeForOffset( byteRangesToDownload, offset)
	  	if ok {
	  		blobBytes := bs.DownloadBytes(containerName, blobName, byteRange.BeginOffset, byteRange.EndOffset)
	  		newFile.Seek(sig.Offset,0)
	  		newFile.Write(blobBytes)
	  		offset += byteRange.EndOffset - byteRange.BeginOffset + 1
		  }
	  }
  }

  // rename .new to origina..... TODO(kpfaulkner)
  return nil
}

func (bs BlobSync) DownloadBytes(containerName string, blobName string, beginOffset int64, endOffset int64) []byte {

	buffer := bytes.Buffer{}
  bs.blobHandler.DownloadBlobRange(&buffer, containerName, blobName, beginOffset, endOffset)
	return buffer.Bytes()
}

func getByteRangeForOffset(byteRanges []signatures.RemainingBytes, offset int64) (*signatures.RemainingBytes, bool) {
	for _,br := range byteRanges {
		if br.BeginOffset == offset {
			return &br, true
		}
	}
	return nil, false
}

// returnMatchingSig finds a matching sig based on MD5.
// Returns pointer to the BlockSig and a bool indicating found or not.
// Could technically just return nil to indicate not found, but will stick with
// explicit bool for now.
func returnMatchingSig(sigsToReuse []signatures.BlockSig, sig signatures.BlockSig) (*signatures.BlockSig, bool) {
	for _,s := range sigsToReuse {
		if s.MD5Signature == sig.MD5Signature {
			return &s, true
		}
	}
	return nil, false

}

func findMatchingSig( sigsToReuse []signatures.BlockSig, sig signatures.BlockSig) bool {
	for _,s := range sigsToReuse {
		if s.MD5Signature == sig.MD5Signature {
			return true
		}
	}
	return false
}

func (bs BlobSync) GenerateByteRangesOfBlobToDownloadORIG(sigsToReuseList []signatures.BlockSig,
																											blobSig *signatures.SizeBasedCompleteSignature,
																											containerName string, blobName string) ([]signatures.RemainingBytes, error) {

  remainingBytesList := []signatures.RemainingBytes{}
  allBlobSigs := signatures.ExpandSizeBasedCompleteSignature(*blobSig)
  sort.Slice(allBlobSigs, func (i int, j int) bool {
  	return allBlobSigs[i].Offset < allBlobSigs[j].Offset
  })

  startOffsetToCopy := int64(0)
  for _, sig := range allBlobSigs {
  	haveMatchingSig := findMatchingSig(sigsToReuseList, sig)
  	if !haveMatchingSig {
  		remainingBytesList = append(remainingBytesList, signatures.RemainingBytes{BeginOffset: startOffsetToCopy, EndOffset: sig.Offset + int64(sig.Size) - 1})
  		startOffsetToCopy = sig.Offset + int64(sig.Size)
	  } else {

	  	// why on earth do I have this here and not just 1 out side of the if statement? Will code as per original but will definitely
	  	// need to revisit this.
	  	startOffsetToCopy = sig.Offset + int64(sig.Size)
	  }
  }

	return remainingBytesList, nil
}

func (bs BlobSync) GenerateByteRangesOfBlobToDownload(sigsToReuseList []signatures.BlockSig,
	blobSig *signatures.SizeBasedCompleteSignature,
	containerName string, blobName string) ([]signatures.RemainingBytes, error) {

	remainingBytesList := []signatures.RemainingBytes{}
	allBlobSigs := signatures.ExpandSizeBasedCompleteSignature(*blobSig)
	sort.Slice(allBlobSigs, func (i int, j int) bool {
		return allBlobSigs[i].Offset < allBlobSigs[j].Offset
	})

	startOffsetToCopy := int64(0)
	for _, sig := range allBlobSigs {
		haveMatchingSig := findMatchingSig(sigsToReuseList, sig)
		if !haveMatchingSig {
			remainingBytesList = append(remainingBytesList, signatures.RemainingBytes{BeginOffset: startOffsetToCopy, EndOffset: sig.Offset + int64(sig.Size) - 1})
			startOffsetToCopy = sig.Offset + int64(sig.Size)
		} else {

			// why on earth do I have this here and not just 1 out side of the if statement? Will code as per original but will definitely
			// need to revisit this.
			startOffsetToCopy = sig.Offset + int64(sig.Size)
		}
	}

	return remainingBytesList, nil
}




// Upload will upload the data from a reader.
// It will return the signature of the file uploaded.
// or an error if something went boom.
func (bs BlobSync) Upload(localFile *os.File, containerName string, blobName string, verbose bool ) error {

  if bs.blobHandler.BlobExist(containerName, blobName) && bs.blobHandler.BlobExist(containerName, blobName+".sig") {
  	// doing the tricky stuff.
  	return bs.uploadDeltaOnly(localFile, containerName, blobName, verbose)
  } else {
  	return bs.uploadBlobAndSigAsNew(localFile, containerName, blobName, verbose)
	}

	return nil
}

// uploadDeltaOnly hardest method of the entire project.
// 1. download signature
// 2. compare signature with local file.
// 3. determine new parts to upload
// 4. upload blocks
// 5. reconstruct blob from old and new blocks
// 6. upload signature
func (bs BlobSync) uploadDeltaOnly(localFile *os.File, containerName, blobName string, verbose bool) error {

  sig, err := bs.DownloadSignatureForBlob(containerName, blobName)
  if err != nil {
  	fmt.Printf("Unable to get sig for blob %s : %s\n", blobName, err)
  	return err
  }

  searchResults, err := SearchLocalFileForSignature( localFile,*sig )
  if err != nil {
  	return err
  }

	allBlocks, err := bs.uploadDelta(localFile, searchResults, containerName, blobName )
	if err != nil {
		return err
	}

	sig,_ = signatures.CreateSignatureFromNewAndReusedBlocks(allBlocks)
	err = bs.uploadSig(sig, containerName, blobName)
	if err != nil {
		fmt.Printf("Cannot upload sig:  %s\n", err.Error())
		return  err
	}

	return nil
}

func (bs BlobSync) uploadBytes(remainingBytes signatures.RemainingBytes, localFile *os.File, containerName, blobName string) ([]signatures.UploadedBlock, error ){

	bs.blobHandler.CreateContainerURL(containerName)
	_, err := bs.blobHandler.UploadRemainingBytesAsBlocks(remainingBytes, localFile, containerName, blobName, false)
	if err != nil {
		fmt.Printf("Unable to upload blob %s\n", err.Error())
		return nil, err
	}


	return nil, nil
}



func (bs BlobSync) uploadBlobAndSigAsNew(localFile *os.File, containerName, blobName string, verbose bool) error {

  bs.blobHandler.UploadBlob(localFile, containerName, blobName, verbose)

	sig, err := bs.generateSig(localFile)
	if err != nil {
		fmt.Printf("Cannot generate sig:  %s\n", err.Error())
		return err
	}

	err = bs.uploadSig(sig, containerName, blobName)
	if err != nil {
		fmt.Printf("Cannot upload sig:  %s\n", err.Error())
		return  err
	}

	// set MD5 for blob.
	_, err = bs.generateMD5String( localFile)
  if err != nil {
  	return err
  }

  /* Still need to sort this out.
	err = bs.setMD5ForBlob( md5Sig, containerName, blobname)
	if err != nil {
		fmt.Printf("Cannot set MD5 for blob  %s\n", err.Error())
		return  err
	}
 */
	return nil
}

func (bs BlobSync) generateMD5String(f *os.File) (string, error) {

	// back to beginning.
	f.Seek(0,0)

	//Open a new hash interface to write to
	hash := md5.New()

	//Copy the file in the hash interface and check for any error
	if _, err := io.Copy(hash, f); err != nil {
		return "", err
	}

	hashInBytes := hash.Sum(nil)[:16]

	// or just base64??
	returnMD5String := hex.EncodeToString(hashInBytes)
	return returnMD5String, nil
}

func (bs BlobSync) setMD5ForBlob(md5 string, containerName, blobName string) error {


	return nil
}

/*
func (bs BlobSync) uploadAsNewBlob(localFile *os.File, containerName, blobName string) error {

	bs.blobHandler.CreateContainerURL(containerName)
	err := bs.blobHandler.UploadRemainingBytesAsBlocks(localFile, containerName, blobName)
	if err != nil {
		fmt.Printf("Unable to upload blob %s\n", err.Error())
		return err
	}

	return nil
} */

func (bs BlobSync) generateSig(localFile *os.File) (*signatures.SizeBasedCompleteSignature, error) {

	// rewind to begining of file.
	localFile.Seek(0,0)

	sig, err := signatures.CreateSignatureFromScratch(localFile)
	if err != nil {
		fmt.Printf("Cannot create signature %s\n", err.Error())
		return nil, err
	}
	return sig,nil
}


func (bs BlobSync) uploadSig(sig *signatures.SizeBasedCompleteSignature, containerName string, blobName string) error {

	sigBytes, _ := json.Marshal(sig)

	// write to temp file? seems silly, but cant get streaming working.
	_ = ioutil.WriteFile(`c:\temp\temp.sig`, sigBytes, 0644)
	f,_ := os.Open(`c:\temp\temp.sig`)
	bs.blobHandler.UploadBlob(f, containerName, blobName+".sig", false)

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
func (bs BlobSync) DownloadSignatureForBlob( containerName string, blobName string ) (*signatures.SizeBasedCompleteSignature, error) {

	buffer := bytes.Buffer{}

	err := bs.blobHandler.DownloadBlobToBuffer(&buffer, containerName, blobName+".sig")
	if err != nil {
		fmt.Printf("Cannot download signature for blob %s : %s\n", blobName, err.Error())
		return nil, err
	}

	var sig signatures.SizeBasedCompleteSignature
	err = json.Unmarshal(buffer.Bytes(), &sig)
	if err != nil {
		fmt.Printf("Cannot unmarshal signature : %s\n",  err.Error())
		return nil, err
	}

	return &sig, nil

}

func DisplayUploadedBytes(uploadedBlocks []signatures.UploadedBlock) {
	total := 0
  for _,block := range uploadedBlocks {
  	total += int(block.Size)
  }

  fmt.Printf("total is %d\n", total)
}

func (bs BlobSync) uploadDelta(localFile *os.File, searchResults *signatures.SignatureSearchResults, containerName string, blobName string) ([]signatures.UploadedBlock, error) {

	allUploadedBlocks := []signatures.UploadedBlock{}

	for _,remainingBytes := range searchResults.ByteRangesToUpload {
		uploadedBlockList, err := bs.blobHandler.UploadRemainingBytesAsBlocks(remainingBytes, localFile, containerName, blobName, false)
		//uploadedBlockList, err := UploadBytes(remainingBytes, localFile, containerName, blobName)
		if err != nil {
			fmt.Printf("Cannot upload bytes: %s\n", err.Error())
		}
		allUploadedBlocks = append(allUploadedBlocks, uploadedBlockList...)
	}

	fmt.Printf("total bytes uploaded %d\n", bs.blobHandler.TotalBytesUploaded)
	DisplayUploadedBytes(allUploadedBlocks)

	for _, sig := range searchResults.SignaturesToReuse {
		blockID := base64.StdEncoding.EncodeToString(sig.MD5Signature[:])
		allUploadedBlocks = append(allUploadedBlocks, signatures.UploadedBlock{BlockID: blockID, Offset: sig.Offset, Size: int64(sig.Size), Sig: sig,IsNew: false})
	}

	sort.Slice(allUploadedBlocks, func (i int, j int) bool {
		return allUploadedBlocks[i].Offset < allUploadedBlocks[j].Offset
	})

	err := bs.blobHandler.PutBlockList(allUploadedBlocks, containerName, blobName)

	return allUploadedBlocks, err
}



func UploadBytes(remainingBytes signatures.RemainingBytes, localFile *os.File, containingName string, blobName string) ([]signatures.UploadedBlock, error) {

	return nil, nil
}

