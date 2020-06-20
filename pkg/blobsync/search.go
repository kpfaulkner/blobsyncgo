package blobsync

import (
	"fmt"
	"os"
	"sort"
)

func getSignatureSizesDescending(  sig SizeBasedCompleteSignature ) []int {

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
func SearchLocalFileForSignature( localFile *os.File, sig SizeBasedCompleteSignature  ) (*SignatureSearchResults, error) {

	searchResults := NewSignatureSearchResults()
  stats, err := localFile.Stat()
  if err != nil {
  	return nil, err
  }

  fileLength := stats.Size()

  // signatures we can use.
  signaturesToReuse := []BlockSig{}
  signatureSizesArray := getSignatureSizesDescending(sig)

  remainingByteList := []RemainingBytes{}
  remainingByteList = append(remainingByteList, RemainingBytes{ BeginOffset: 0, EndOffset: fileLength - 1})

  for _,sigSize := range signatureSizesArray {

  	// seek back to begining of file.
  	localFile.Seek(0,0)

  	// get all sigs of a particular size.
  	sigs := sig.Signatures[sigSize]
  	newRemainingByteList, err := searchLocalFileForSignaturesOfGivenSize( sigs, localFile, remainingByteList, int64(sigSize), fileLength, signaturesToReuse)
  	if err != nil {
  		return nil, err
	  }
  	remainingByteList = newRemainingByteList
  }

  searchResults.ByteRangesToUpload = remainingByteList
  searchResults.SignaturesToReuse = signaturesToReuse
	return &searchResults, nil
}

func generateBlockLUT( sig CompleteSignature) map[RollingSignature][]BlockSig {
  blockLUT := make(map[RollingSignature][]BlockSig)

  bsl := []BlockSig{}
  var ok bool
  for _, element := range sig.SignatureList {
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
     	bsl := []BlockSig{}
     	bsl = append(bsl, element)
     	blockLUT[element.RollingSig] = bsl

     }
  }

  return blockLUT
}

// searchLocalFileForSignaturesOfGivenSize goes through the remaining byte ranges (initially will be 0 -> end of file),
// and figure out which parts of the file match the signatures (ie can be reused)
func searchLocalFileForSignaturesOfGivenSize(sig CompleteSignature, localFile *os.File, remainingByteList []RemainingBytes,
																						 sigSize int64, fileLength int64, signaturesToReuse []BlockSig) ([]RemainingBytes, error) {
	localFile.Seek(0,0)
	windowSize := sigSize
	newRemainingBytes := []RemainingBytes{}
	sigLUT := generateBlockLUT(sig)
	buffer := make([]byte, windowSize)
	offset := int64(0)

	// go through remaining byte ranges.
	for _, byteRange := range remainingByteList {
    byteRangeSize := byteRange.EndOffset - byteRange.BeginOffset + 1

		// if byte range is large... and signature size is small (what values???) then dont check.
		// We could end up with LOADS of tiny sig matching where ideally we'd use a larger new sig block.
		// The exception is when the sig size exactly matches the byterange size... then we allow it to check if the sig will match
		// in practice this allows small (1-2 byte sigs) to match the byte ranges.
    if byteRangeSize > 1000 && sigSize > 100 || byteRangeSize == sigSize {

	    // if byteRange is smaller than the key we're using, then there cannot be a match so add
	    // it to the newRemainingBytes list
    	if byteRange.EndOffset - byteRange.BeginOffset +1 > windowSize {
    		offset = byteRange.BeginOffset
		    generateFreshSig := true
    		var currentSig RollingSignature
    		oldEndOffset := byteRange.BeginOffset
    		for {

    			// generate fresh sig... not really rolling
    			if generateFreshSig {
    				bytesRead,err := localFile.ReadAt(buffer, offset)
    				if err != nil {
							fmt.Printf("Cannot read file: %s\n", err.Error())
							return nil, err
				    }
				    currentSig = CreateRollingSignature(buffer, bytesRead)
			    } else {
			    	// roll existing sig.
			    	localFile.Seek(offset-1, 0)
			    	b := make([]byte,1)
			    	_,_ = localFile.ReadAt(b,offset-1)
			    	previousByte := b[0]

				    _,_ = localFile.ReadAt(b, offset + windowSize - 1)
				    nextByte := b[0]
				    currentSig = RollSignature(windowSize, previousByte, nextByte, currentSig)
			    }

			    _, ok := sigLUT[currentSig]
			    if ok {
			      localFile.Seek(offset,0)
			      bytesRead, err := localFile.ReadAt(buffer, offset)
			      if err != nil {
			      	fmt.Printf("Unable to read file:  %s\n", err.Error())
			      	return nil, err
			      }

			      md5Sig := CreateMD5Signature(buffer, bytesRead)
			      sigForCurrentRollingSig := sigLUT[currentSig]
			      sigMatchingRollingSigAndMD5 := getMatchingMD5Sig(sigForCurrentRollingSig, md5Sig)

			      if sigMatchingRollingSigAndMD5 != nil {
			      	if oldEndOffset != offset {
			      		newRemainingBytes = append(newRemainingBytes, RemainingBytes{BeginOffset: oldEndOffset, EndOffset: offset-1})
				      }

				      sigMatchingRollingSigAndMD5.Offset = offset
				      signaturesToReuse = append(signaturesToReuse, *sigMatchingRollingSigAndMD5)
				      offset += windowSize
				      generateFreshSig = true
				      oldEndOffset = offset
			      } else {
			      	offset++
			      	generateFreshSig = false
			      }
			    } else {
				    // no match. Just increment offset and generate rolling sig.
				    offset++;
				    generateFreshSig = false;
			    }

			    // lack of do-while.
			    if offset + windowSize < byteRange.EndOffset + 1 {
			    	break
			    }
		    }
	    } else {
	    	newRemainingBytes = append(newRemainingBytes, byteRange)
	    }
    } else {
	    newRemainingBytes = append(newRemainingBytes, byteRange)
    }
	}
	return newRemainingBytes, nil
}

func getMatchingMD5Sig(matchingSigs []BlockSig, md5Sig [16]byte) *BlockSig {
	for _,s := range matchingSigs {
		if s.MD5Signature == md5Sig {
			return &s
		}
	}

  return nil
}