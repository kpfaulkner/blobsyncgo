package main

import (

	"flag"
	"fmt"
	"github.com/kpfaulkner/blobsyncgo/pkg/blobsync"

	"github.com/kpfaulkner/blobsyncgo/pkg/signatures"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"

)

func main() {
	fmt.Printf("so it begins....\n")

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	newFilePath := flag.String("newfilepath", "", "path to new file")
	existingFilePath := flag.String("existingfilepath", "", "path to existing")

	//verbose := flag.Bool("verbose", false, "verbose")

	flag.Parse()

	if *newFilePath == "" || *existingFilePath == "" {
		fmt.Printf("Error....\n")
		return
	}



	existingFile,_ := os.Open(*existingFilePath)
	newFile,_ := os.Open(*newFilePath)
	existingSig, _ := signatures.CreateSignatureFromScratch(existingFile)

  CompareSignatures(existingSig, newFile)
}

func getSigSizes( sig *signatures.SizeBasedCompleteSignature) []int {
	l := []int{}
	for k,_ := range sig.Signatures {
		l = append(l, k)
	}

	return l
}

// LUT based on MD5 ([16]byte)
func generateSigLUT( sigs []signatures.BlockSig) map[[16]byte]signatures.BlockSig {

	lut := make(map[[16]byte]signatures.BlockSig)
	for _,sig := range sigs {
		if _, ok := lut[sig.MD5Signature]; !ok {
			lut[sig.MD5Signature] = sig
		}
	}

	return lut
}




// given some existingSig, search through updated/newer file to see
// what parts already exist!
func CompareSignatures(existingSig *signatures.SizeBasedCompleteSignature, updatedFile *os.File) {

	searchResults, err := blobsync.SearchLocalFileForSignature(updatedFile, *existingSig)
	if err != nil {
		log.Fatalf("Cannot compare signatures %s\n", err.Error())
	}

	for _,sr := range searchResults.SignaturesToReuse {
		fmt.Printf("reusing %d\n", sr.Offset)
	}
	fmt.Printf("total sigs reused %d\n", len(searchResults.SignaturesToReuse))
	fmt.Printf("total ranges to download %d\n", len(searchResults.ByteRangesToUpload))


}

