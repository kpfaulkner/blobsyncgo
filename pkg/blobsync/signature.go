package blobsync


type RollingSignature struct {
  Sig1 int64
  Sig2 int64
}

type BlockSig struct {
	Offset int64
	Size int
	RollingSig RollingSignature
	MD5Signature [16]byte
	BlockNo int

}

type Signature struct {

}

// all Sigs of a certain size.
// could do with some renaming.
type CompleteSignature struct {
	SignatureList []BlockSig
}

type SizeBasedCompleteSignature struct {
	Signatures map[int]CompleteSignature
}


func NewSizeBasedCompleteSignature() SizeBasedCompleteSignature{
	s := SizeBasedCompleteSignature{}
	s.Signatures = make(map[int]CompleteSignature)
	return s
}
