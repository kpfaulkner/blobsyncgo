package signatures

type RemainingBytes struct {
  BeginOffset int64   // inclusive.
  EndOffset int64     // inclusive.
}

type SignatureSearchResults struct {
	SignaturesToReuse []BlockSig
	FileSize int64
	ByteRangesToUpload []RemainingBytes
}

func NewSignatureSearchResults() SignatureSearchResults {
	r := SignatureSearchResults{}
	r.SignaturesToReuse = []BlockSig{}
	r.ByteRangesToUpload = []RemainingBytes{}
	return r
}
