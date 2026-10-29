package storage

import (
	"encoding/binary"
	"hash/crc32"

	"blob-cache/common"
)

// On-disk layout of the blob cache:
//
// Blob file:
//
//	[8 bytes blob header: magic u32 "BLBC" | version u16 | flags u16]
//	[entry record 0][entry record 1]...
//
// Entry record (little endian):
//
//	u32 entryMagic  "BLOC"
//	u64 hash
//	u32 keyLen
//	u32 valueLen
//	u32 props       (encoded properties)
//	u32 crc32       (CRC-32/IEEE over key || value)
//	key bytes
//	value bytes
const (
	blobHeaderSize  = 8
	entryHeaderSize = 24
)

var (
	blobMagic  = uint32(0x434C4242) // "BLBC"
	blobVer    = uint16(1)
	entryMagic = uint32(0x434F4C42) // "BLOC"
)

// entryHeaderLen is the total length of one serialized entry.
func entryLen(keyLen, valueLen int) int {
	return entryHeaderSize + keyLen + valueLen
}

// maxEntrySize returns the largest entry that fits into a blob file.
func maxEntrySize(blobSize int64) int {
	return int(blobSize) - blobHeaderSize
}

// encodeProps packs the serializable property fields into one byte.
func encodeProps(p common.Properties) uint32 {
	var v uint32
	v |= uint32(p.Hint&0x3) | uint32(p.Location&0x3)<<2 | uint32(p.Age&0x3)<<4
	return v
}

// decodeProps unpacks the properties byte.
func decodeProps(v uint32) common.Properties {
	return common.Properties{
		Hint:     common.Hint(v & 0x3),
		Location: common.Location((v >> 2) & 0x3),
		Age:      common.Age((v >> 4) & 0x3),
	}
}

// appendBlobHeader appends the blob file header to buf.
func appendBlobHeader(buf []byte) []byte {
	var h [8]byte
	binary.LittleEndian.PutUint32(h[0:4], blobMagic)
	binary.LittleEndian.PutUint16(h[4:6], blobVer)
	binary.LittleEndian.PutUint16(h[6:8], 0)
	return append(buf, h[:]...)
}

func checkBlobHeader(buf []byte) bool {
	if len(buf) < blobHeaderSize {
		return false
	}
	return binary.LittleEndian.Uint32(buf[0:4]) == blobMagic &&
		binary.LittleEndian.Uint16(buf[4:6]) == blobVer
}

// appendEntry appends one serialized entry record to buf.
func appendEntry(buf []byte, hash uint64, key, value []byte, props common.Properties) []byte {
	var h [entryHeaderSize]byte
	binary.LittleEndian.PutUint32(h[0:4], entryMagic)
	binary.LittleEndian.PutUint64(h[4:12], hash)
	binary.LittleEndian.PutUint32(h[12:16], uint32(len(key)))
	binary.LittleEndian.PutUint32(h[16:20], uint32(len(value)))
	binary.LittleEndian.PutUint32(h[20:24], encodeProps(props))
	buf = append(buf, h[:]...)
	buf = append(buf, key...)
	buf = append(buf, value...)
	return buf
}

// entryHeader is the parsed header of one entry record.
type entryHeader struct {
	hash     uint64
	keyLen   int
	valueLen int
	props    common.Properties
	crc      uint32
}

// parseEntryHeader parses the header of an entry record at the start of
// buf. It reports ok=false if the buffer is too short or the magic does
// not match.
func parseEntryHeader(buf []byte) (h entryHeader, ok bool) {
	if len(buf) < entryHeaderSize {
		return h, false
	}
	if binary.LittleEndian.Uint32(buf[0:4]) != entryMagic {
		return h, false
	}
	h.hash = binary.LittleEndian.Uint64(buf[4:12])
	keyLen := int(binary.LittleEndian.Uint32(buf[12:16]))
	valueLen := int(binary.LittleEndian.Uint32(buf[16:20]))
	if keyLen < 0 || valueLen < 0 || keyLen > len(buf) || valueLen > len(buf) {
		return h, false
	}
	h.keyLen = keyLen
	h.valueLen = valueLen
	h.props = decodeProps(binary.LittleEndian.Uint32(buf[20:24]))
	h.crc = binary.LittleEndian.Uint32(buf[24-4 : 24])
	return h, true
}

// entryCRC computes the checksum of an entry payload.
func entryCRC(key, value []byte) uint32 {
	h := crc32.NewIEEE()
	h.Write(key)
	h.Write(value)
	return h.Sum32()
}

// entryPayload views the key and value bytes of a parsed record.
func entryPayload(buf []byte, h entryHeader) (key, value []byte, ok bool) {
	if len(buf) < h.keyLen+h.valueLen {
		return nil, nil, false
	}
	key = buf[:h.keyLen]
	value = buf[h.keyLen : h.keyLen+h.valueLen]
	return key, value, true
}
