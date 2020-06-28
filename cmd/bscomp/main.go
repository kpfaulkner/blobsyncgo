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

	"sort"
)

func main() {
	fmt.Printf("so it begins....\n")

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	file1Path := flag.String("file1", "", "path to file1")
	file2Path := flag.String("file2", "", "path to file2")

	//verbose := flag.Bool("verbose", false, "verbose")

	flag.Parse()

	if *file1Path == "" || *file2Path == "" {
		fmt.Printf("Error....\n")
		return
	}



	file1,_ := os.Open(*file1Path)
	file2,_ := os.Open(*file2Path)
	sig1, _ := signatures.CreateSignatureFromScratch(file1)
	sig2, _ := signatures.CreateSignatureFromScratch(file2)

  CompareSignatures(sig1, sig2)
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

	for
	// go othrough existing sizes.
	for _,ss := range existingSigSizes {

		// all sig2 sigs for a size ss
		sig2LUT := generateSigLUT( sig2.Signatures[ss].SignatureList)

		sizeCountMatch := 0
		sizeCountNoMatch := 0
		// check sig1 to see what matches we have.
		for _,sig := range sig1.Signatures[ss].SignatureList {
			if _, ok := sig2LUT[sig.MD5Signature]; ok {
				sizeCountMatch++
				fmt.Printf("HAVE MATCH %d, offset %d\n", sizeCountMatch, sig.Offset)
			} else {
				sizeCountNoMatch++
			}
		}
		fmt.Printf("Size %d has %d matches and %d no-matches\n", ss, sizeCountMatch, sizeCountNoMatch)
	}

}

