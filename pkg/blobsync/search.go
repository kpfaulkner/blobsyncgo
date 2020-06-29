package blobsync

import (
	"fmt"
	"github.com/edsrzf/mmap-go"
	"github.com/kpfaulkner/blobsyncgo/pkg/azureutils"
	"github.com/kpfaulkner/blobsyncgo/pkg/signatures"
	"log"
	"os"
	"sort"
)

func getSignatureSizesDescending(  sig signatures.SizeBasedCompleteSignature) []int {

	l := []int{}
	for sigLength,_ := range sig.Signatures {
	  l = append(l, sigLength)
	}

	sort.Sort(sort.Reverse(sort.IntSlice(l)))
	return l
}

// hardest part...
// Search local file for all the data that is already in azure blob storage.
// Then determine which parts need to be uploaded.
func SearchLocalFileForSignature( localFile *os.File, sig signatures.SizeBasedCompleteSignature) (*signatures.SignatureSearchResults, error) {

	searchResults := signatures.NewSignatureSearchResults()
  stats, err := localFile.Stat()
  if err != nil {
  	return nil, err
  }

  fileLength := stats.Size()

  // signatures we can use.
  signaturesToReuse := []signatures.BlockSig{}
  signatureSizesArray := getSignatureSizesDescending(sig)

  remainingByteList := []signatures.RemainingBytes{}
  remainingByteList = append(remainingByteList, signatures.RemainingBytes{ BeginOffset: 0, EndOffset: fileLength - 1})

  for _,sigSize := range signatureSizesArray {

  	fmt.Printf("Processing sig size %d\n", sigSize)
  	// get all sigs of a particular size.
  	sigs := sig.Signatures[sigSize]
  	newRemainingByteList, newSignaturesToReuse, err := searchLocalFileForSignaturesOfGivenSize( sigs, localFile, remainingByteList, int64(sigSize), fileLength)
  	if err != nil {
  		return nil, err
	  }
	  signaturesToReuse = append(signaturesToReuse, newSignaturesToReuse...)
  	remainingByteList = newRemainingByteList
  }

  searchResults.ByteRangesToUpload = remainingByteList
  searchResults.SignaturesToReuse = signaturesToReuse
	return &searchResults, nil
}

// hardest part...
// Search local file for all the data that is already in azure blob storage.
// Then determine which parts are already local and do NOT need to be downloaded again.
func SearchLocalFileForSignatureForDownload( localFile *os.File, sig signatures.SizeBasedCompleteSignature) (*signatures.SignatureSearchResults, error) {

	searchResults := signatures.NewSignatureSearchResults()
	stats, err := localFile.Stat()
	if err != nil {
		return nil, err
	}

	fileLength := stats.Size()

	// signatures we can use.
	signaturesToReuse := []signatures.BlockSig{}
	signatureSizesArray := getSignatureSizesDescending(sig)

	remainingByteList := []signatures.RemainingBytes{}
	remainingByteList = append(remainingByteList, signatures.RemainingBytes{ BeginOffset: 0, EndOffset: fileLength - 1})

	for _,sigSize := range signatureSizesArray {

		// get all sigs of a particular size.
		sigs := sig.Signatures[sigSize]

		// if sigsize <= 100 then just copy the bytes...  maybe even do for 1000?
		if sigSize > 100 {
			newSignaturesToReuse, err := searchLocalFileForSignaturesOfGivenSizeForDownload(sigs, localFile, int64(sigSize))
			if err != nil {
				return nil, err
			}
			signaturesToReuse = append(signaturesToReuse, newSignaturesToReuse...)
		}
	}

	searchResults.ByteRangesToUpload = remainingByteList
	searchResults.SignaturesToReuse = signaturesToReuse
	return &searchResults, nil
}

func generateBlockLUTFromBlockSigs( bs []signatures.BlockSig) map[signatures.RollingSignature][]signatures.BlockSig {
	blockLUT := make(map[signatures.RollingSignature][]signatures.BlockSig)

	bsl := []signatures.BlockSig{}
	var ok bool
	for _, element := range bs {
		bsl, ok = blockLUT[element.RollingSig]
		addToList := false
		if ok {

			// rolling sig exists in blockLUT. Go and check if MD5's match.
			// if they do NOT match (ie its a new block with same rolling sig but not the same MD5) then
			// add to array which is the map value.
			for _, bs := range bsl {
				if bs.MD5Signature != element.MD5Signature {
					addToList = true
					break
				}
			}

			if addToList {
				bsl = append(bsl, element)
				blockLUT[element.RollingSig] = bsl
			} else {
				// otherwise, ignore it... we already have something with exact same rolling and md5 sigs.
			}

		} else {

			// doesn't exist in LUT... create it.
			bsl := []signatures.BlockSig{}
			bsl = append(bsl, element)
			blockLUT[element.RollingSig] = bsl

		}
	}

	return blockLUT
}

// searchLocalFileForSignaturesOfGivenSize goes through the remaining byte ranges (initially will be 0 -> end of file),
// and figure out which parts of the file match the signatures (ie can be reused)
func searchLocalFileForSignaturesOfGivenSize(sig signatures.CompleteSignature, localFile *os.File, remainingByteList []signatures.RemainingBytes,
																						 sigSize int64, fileLength int64 ) ([]signatures.RemainingBytes, []signatures.BlockSig, error) {

	windowSize := sigSize
	newRemainingBytes := []signatures.RemainingBytes{}
	sigLUT := generateBlockLUTFromBlockSigs(sig.SignatureList)
	//buffer := make([]byte, windowSize)
	offset := int64(0)
	signaturesToReuse := []signatures.BlockSig{}
  lastDisplayOffset := int64(0)

  mm,err  := mmap.Map(localFile, mmap.RDONLY, 0)
  if err != nil {
  	log.Fatalf("Unable to mmap the file: %s\n", err.Error())
  }
  defer mm.Unmap()

	// go through remaining byte ranges.
	for _, byteRange := range remainingByteList {
		fmt.Printf("Searching %d to %d, for sig size %d\n", byteRange.BeginOffset, byteRange.EndOffset, sigSize)
    byteRangeSize := byteRange.EndOffset - byteRange.BeginOffset + 1

		// if byte range is large... and signature size is small (what values???) then dont check.
		// We could end up with LOADS of tiny sig matching where ideally we'd use a larger new sig block.
		// The exception is when the sig size exactly matches the byterange size... then we allow it to check if the sig will match
		// in practice this allows small (1-2 byte sigs) to match the byte ranges.
    if byteRangeSize > 1000 && sigSize > 100 || byteRangeSize == sigSize {

	    // if byteRange is smaller than the key we're using, then there cannot be a match so add
	    // it to the newRemainingBytes list
    	if byteRange.EndOffset - byteRange.BeginOffset +1 >= windowSize {
    		offset = byteRange.BeginOffset
		    generateFreshSig := true
    		var currentSig signatures.RollingSignature
    		oldEndOffset := byteRange.BeginOffset

    		for {
    			if offset > lastDisplayOffset {
				    fmt.Printf("offset is %d : sig matches so far %d\n", offset, len(signaturesToReuse))

				    lastDisplayOffset = offset + 10000000

			    }

    			// generate fresh sig... not really rolling
    			if generateFreshSig {
    				buffer, err := azureutils.PopulateBuffer(&mm, offset, int64(windowSize), byteRange.EndOffset)
    				if err != nil {
							fmt.Printf("Cannot read file: %s\n", err.Error())
							return nil, nil, err
				    }
				    bytesRead := len(buffer)
				    currentSig = signatures.CreateRollingSignature(buffer, int(bytesRead))
				    generateFreshSig = false
			    } else {
			    	previousByte := mm[offset-1]
			    	nextByte := mm[offset + windowSize -1]
				    currentSig = signatures.RollSignature(windowSize, previousByte, nextByte, currentSig)
			    }

			    _, ok := sigLUT[currentSig]
			    if ok {
				    buffer, _ := azureutils.PopulateBuffer(&mm, offset, int64(windowSize), byteRange.EndOffset)
				    bytesRead := len(buffer)
				    md5Sig := signatures.CreateMD5Signature(buffer, int(bytesRead))
			      sigForCurrentRollingSig := sigLUT[currentSig]
			      sigMatchingRollingSigAndMD5, sigFound := getMatchingMD5Sig(sigForCurrentRollingSig, md5Sig)

			      if sigFound {
				      sigMatchingRollingSigAndMD5.Offset = offset
				      signaturesToReuse = append(signaturesToReuse, sigMatchingRollingSigAndMD5)
				      offset++
				      //offset += windowSize
				      oldEndOffset = offset
			      } else {
			      	offset++
			      	generateFreshSig = false
			      }
			    } else {
				    // no match. Just increment offset and generate rolling sig.
				    offset++
				    generateFreshSig = false
			    }

			    // lack of do-while.
			    if offset + windowSize > byteRange.EndOffset + 1 {
			    	break
			    }
		    }
		    if offset <= byteRange.EndOffset {
		    	newRemainingBytes = append(newRemainingBytes, signatures.RemainingBytes{BeginOffset: oldEndOffset, EndOffset: byteRange.EndOffset})
		    }
	    } else {
	    	newRemainingBytes = append(newRemainingBytes, byteRange)
	    }
    } else {
	    newRemainingBytes = append(newRemainingBytes, byteRange)
    }
	}
	return newRemainingBytes, signaturesToReuse, nil
}

// searchLocalFileForSignaturesOfGivenSizeForDownload goes through ENTIRE file looking for matches to
// existing blob signatures. This may be excessive, but could provide useful for minimising how much we're downloading.
func searchLocalFileForSignaturesOfGivenSizeForDownload(sig signatures.CompleteSignature, localFile *os.File,
	sigSize int64) ([]signatures.BlockSig, error) {

	windowSize := sigSize
	blobSigLUT := generateBlockLUTFromBlockSigs(sig.SignatureList)
	//buffer := make([]byte, windowSize)

	signaturesToReuse := []signatures.BlockSig{}
	lastDisplayOffset := int64(0)
	stats,_ := localFile.Stat()
	fileLength := stats.Size()
	var currentSig signatures.RollingSignature

	mm,err  := mmap.Map(localFile, mmap.RDONLY, 0)
	if err != nil {
		log.Fatalf("Unable to mmap the file: %s\n", err.Error())
	}
	defer mm.Unmap()

	offset := int64(0)
	generateFreshSig := true
	for offset + sigSize < fileLength {

		if offset > lastDisplayOffset {
			fmt.Printf("offset is %d\n", offset)
			lastDisplayOffset = offset + 100000
		}

		// generate fresh sig... not really rolling
		if generateFreshSig {
			buffer, err := azureutils.PopulateBuffer(&mm, offset, int64(windowSize), fileLength-1)
			if err != nil {
				fmt.Printf("Cannot read file: %s\n", err.Error())
				return nil, err
			}
			bytesRead := len(buffer)
			currentSig = signatures.CreateRollingSignature(buffer, int(bytesRead))
			generateFreshSig = false
		} else {
			previousByte := mm[offset-1]
			nextByte := mm[offset + windowSize -1]
			currentSig = signatures.RollSignature(windowSize, previousByte, nextByte, currentSig)

			/*
			// just for testing idea.
			buffer, err := azureutils.PopulateBuffer(&mm, offset, int64(windowSize), fileLength -1)
			if err != nil {
				fmt.Printf("Cannot read file: %s\n", err.Error())
				return nil, err
			}

			bytesRead := len(buffer)
			tempCompareSig := signatures.CreateRollingSignature(buffer, bytesRead)
			if currentSig != tempCompareSig {
				fmt.Printf("rolling vs new sig differ!!!\n")
			} */
		}

		_, ok := blobSigLUT[currentSig]
		if ok {
			buffer, _ := azureutils.PopulateBuffer(&mm, offset, int64(windowSize), fileLength-1)
			bytesRead := len(buffer)
			md5Sig := signatures.CreateMD5Signature(buffer, int(bytesRead))
			sigForCurrentRollingSig := blobSigLUT[currentSig]
			sigMatchingRollingSigAndMD5, sigFound := getMatchingMD5Sig(sigForCurrentRollingSig, md5Sig)

			if sigFound {

				// this is a copy of the sig.
				// Want LOCAL offset.
				sigMatchingRollingSigAndMD5.Offset = offset
				signaturesToReuse = append(signaturesToReuse, sigMatchingRollingSigAndMD5)
			}
		}
    offset++
	}
	return signaturesToReuse, nil
}

func getMatchingMD5Sig(matchingSigs []signatures.BlockSig, md5Sig [16]byte) (signatures.BlockSig,bool) {
	for _,s := range matchingSigs {
		if s.MD5Signature == md5Sig {
			return s,true
		}
	}

	// HATE returning a constructed empty,
	// but need a copy and obviously dont want a pointer.
  return signatures.BlockSig{}, false
}
