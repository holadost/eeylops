package badger_kv_store

import (
	"github.com/golang/glog"
	"unicode"
)

// BuildCFKey is a helper function that creates the key based on the CF.
func BuildCFKey(cf string, key []byte) []byte {
	if len(key) == 0 {
		glog.Fatalf("Unable to build key since key is empty")
	}
	return append(BuildCFPrefixBytes(cf), append(kDefaultColumnNamePrefixBytes, key...)...)
}

// BuildCFKeyWithCFPrefixBytes is a helper function that creates the key based using the given CF prefix bytes.
func BuildCFKeyWithCFPrefixBytes(cfPrefix []byte, key []byte) []byte {
	if len(key) == 0 || len(cfPrefix) == 0 {
		glog.Fatalf("Unable to build key since key is empty")
	}
	return append(cfPrefix, append(kDefaultColumnNamePrefixBytes, key...)...)
}

// BuildFirstCFKey is a helper function that creates the first key for a column family.
func BuildFirstCFKey(cf string) []byte {
	cfKey := BuildCFPrefixBytes(cf)
	return append(cfKey, make([]byte, kInternalMaxKeyLength-len(cfKey))...)
}

// BuildLastCFKey is a helper function that creates the last key for a column family.
func BuildLastCFKey(cf string) []byte {
	cfKey := BuildCFPrefixBytes(cf)
	remBytes := make([]byte, kInternalMaxKeyLength-len(cfKey))
	for ii := 0; ii < len(remBytes); ii++ {
		remBytes[ii] = byte(255)
	}
	return append(cfKey, remBytes...)
}

// BuildCFPrefixBytes is a helper function that creates the CF prefix bytes.
func BuildCFPrefixBytes(cf string) []byte {
	if len(cf) == 0 {
		cf = kDefaultCFName
	}
	return append([]byte(cf), kSeparatorBytes...)
}

// ExtractUserKey is a helper function that extracts the user key from the full key which also includes the CF name.
func ExtractUserKey(cf string, key []byte) []byte {
	if len(key) == 0 {
		glog.Fatalf("Unable to extract user key since the given full key is empty")
	}
	fullPrefix := append(BuildCFPrefixBytes(cf), kDefaultColumnNamePrefixBytes...)
	return key[len(fullPrefix):]
}

// IsColumnFamilyNameValid is a helper function that checks if the column family name is valid.
func IsColumnFamilyNameValid(name string) bool {
	if len(name) == 0 || len(name) > kMaxColumnFamilyLen {
		return false
	}
	for _, cc := range name {
		if unicode.IsDigit(cc) || unicode.IsLetter(cc) || cc == '_' {
			continue
		}
		return false
	}
	return true
}

// IsKeyValid is a helper function that checks if the key is valid. skipLowerBound flag can be used if we want
// to just check the upper bound and skip the lower bound.
func IsKeyValid(key []byte, skipLowerBound bool) bool {
	if len(key) > kMaxKeyLength {
		return false
	}
	if skipLowerBound {
		return true
	}
	if len(key) == 0 {
		return false
	}
	return true
}
