package signatures

import (
	"crypto/md5"
	"fmt"
	"io"
	"os"
)

const (

	// Default signature size in bytes
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

func GenerateBlockSig( buffer []byte, offset int64, blockSize int, id int ) (*BlockSig, error) {
	bs := BlockSig{}
	rollingSig := CreateRollingSignature(buffer, blockSize)
	md5Sig := CreateMD5Signature(buffer[:blockSize], blockSize)

	bs.RollingSig = rollingSig
	bs.MD5Signature = md5Sig
	bs.Offset = offset
	bs.BlockNo = id
	bs.Size = blockSize
	return &bs, nil
}

func CreateSignatureFromNewAndReusedBlocks(allBlocks []UploadedBlock) (*SizeBasedCompleteSignature, error) {

	sigLUT := make(map[int][]BlockSig)

	sigList := []BlockSig{}
	var ok bool
	for _,newBlock := range allBlocks {
		sigList, ok = sigLUT[newBlock.Sig.Size]
		if !ok {
			sigList = []BlockSig{}
		}

		sigList = append(sigList, newBlock.Sig)
		sigLUT[newBlock.Sig.Size] = sigList
	}

	sizedBasedSignature :=  NewSizeBasedCompleteSignature()
	for k,v := range sigLUT {
		compSig := CompleteSignature{ SignatureList: v}
		sizedBasedSignature.Signatures[k] = compSig
	}

  return &sizedBasedSignature, nil
}

// CreateSignatureFromScratch reads a file, creates a signature.
func CreateSignatureFromScratch( localFile *os.File ) (*SizeBasedCompleteSignature, error) {

	offset := int64(0)
	buffer := make([]byte, SignatureSize)
	idCount := 0
	//reader := bufio.NewReader(f)

	sigSizeLUT := make(map[int][]BlockSig)
	for {
		n, err := localFile.Read(buffer)

		if err != nil && err != io.EOF {
			fmt.Printf("Cannot read file: %s\n", err.Error())
			return nil, err
		}

		if err == io.EOF {
			break
		}

		blockSig,err := GenerateBlockSig( buffer, offset, n, idCount)
		if err != nil {
			fmt.Printf("error generatingBlockSig %s\n", err.Error())
			return nil, err
		}
		var blockSigArray []BlockSig
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

func RollSignature( length int64, previousByte byte, nextByte byte, existingSignature RollingSignature) RollingSignature {

	s1 := existingSignature.Sig1
	s2 := existingSignature.Sig2

	s1 = s1 - int64(previousByte) + int64(nextByte)
	s2 = s2 - (int64(previousByte) * length) + s1

  res := RollingSignature{Sig1: s1, Sig2:s2}
  return res
}
