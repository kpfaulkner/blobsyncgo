package blobsync

import (
	"bufio"
	"crypto/md5"
	"fmt"
	"hash/adler32"
	"os"
)

const (

	// need to remember what this is about :)
  SignatureSize int = 10000

)


// Cannot remember whats in this yet
type SignatureHandler struct {

}

func NewSignatureHandler() SignatureHandler {
	sh := SignatureHandler{}
	return sh
}

func CreateRollingSignature( byteBlock []byte, length int) RollingSignature {
	s1 := int64(0)
	s2 := int64(0)

	for i := 0; i< length ; i++ {
		s1 += int64(byteBlock[i])
	}

	for i := 0; i< length ; i++ {
		s2 +=  int64(length - i) *  int64(byteBlock[i])
	}

	sig := RollingSignature{}
	sig.Sig1 = s1
	sig.Sig2 = s2
  return sig
}

func CreateMD5Signature(byteBlock []byte, length int) [md5.Size]byte {
	return md5.Sum(byteBlock)
}

func generateBlockSig( buffer []byte, offset int64, blockSize int, id int ) (*BlockSig, error) {
	bs := BlockSig{}
	rollingSig := CreateRollingSignature(buffer, blockSize)
	md5Sig := CreateMD5Signature(buffer, blockSize)

	bs.RollingSig = rollingSig
	bs.MD5Signature = md5Sig
	bs.Offset = offset
	bs.BlockNo = id
	bs.Size = blockSize
	return &bs, nil
}

// CreateSignatureFromScratch reads a file, creates a signature.
func CreateSignatureFromScratch( filePath string ) (*SizeBasedCompleteSignature, error) {

	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	offset := int64(0)
	buffer := make([]byte, SignatureSize)
	idCount := 0
	//reader := bufio.NewReader(f)

	sigSizeLUT := make(map[int][]BlockSig)
	for n, err := f.ReadAt(buffer, offset); n > 0; {
		if err != nil {
			fmt.Printf("Cannot read file: %s\n", err.Error())
			return nil, err
		}

		blockSig,err := generateBlockSig( buffer, offset, n, idCount)
		if err != nil {
			fmt.Printf("error generatingBlockSig %s\n", err.Error())
			return nil, err
		}
		var blockSigArray []BlockSig{}
		var ok bool
		// check if signatures of size n exist in the dict already.
		blockSigArray, ok = sigSizeLUT[n]
		if !ok {
			blockSigArray = []BlockSig{}
		}

		blockSigArray = append(blockSigArray, *blockSig)
		sigSizeLUT[n] = blockSigArray

		offset += int64(n)
		idCount++
	}

  sizedBaseSignature := NewSizeBasedCompleteSignature()

  for k,v := range sigSizeLUT {
  	compSig := CompleteSignature{}
  	compSig.SignatureList = v
  	sizedBaseSignature.Signatures[k] = compSig
  }

  return &sizedBaseSignature, nil
}
