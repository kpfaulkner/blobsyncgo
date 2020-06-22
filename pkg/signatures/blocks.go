package signatures

type UploadedBlock struct {
	BlockID     string
	Offset      int64
	Sig         BlockSig
	Size        int64
	IsNew       bool
	IsDuplicate bool
}

type BlockSig struct {
	Offset int64
	Size int
	RollingSig RollingSignature
	MD5Signature [16]byte
	BlockNo int

}
