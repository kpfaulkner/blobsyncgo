package signatures


type RollingSignature struct {
  Sig1 int64
  Sig2 int64
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


func NewSizeBasedCompleteSignature() SizeBasedCompleteSignature {
	s := SizeBasedCompleteSignature{}
	s.Signatures = make(map[int]CompleteSignature)
	return s
}

// output to stdout.
func (s SizeBasedCompleteSignature) Display() {

}

// SaveToFile, should probably change to a io.Writer
func (s SizeBasedCompleteSignature) SaveToFile( fileName string) error {


	return nil
}

