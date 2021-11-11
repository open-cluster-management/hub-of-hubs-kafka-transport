package types

import "time"

const (
	// MsgIDKey is the common TransportMessage's ID tag.
	MsgIDKey = "id"
	// MsgTypeKey is the common TransportMessage's MsgType tag.
	MsgTypeKey = "msgType"

	// HeaderCompressionType is the key used in compression type header.
	HeaderCompressionType = "compressionType"
	// HeaderSizeKey is the key used in total bundle size header.
	HeaderSizeKey = "size"
	// HeaderOffsetKey is the key used in message fragment offset header.
	HeaderOffsetKey = "fragmentOffset"
	// HeaderDismantlingTimestamp is the key used in bundle dismantling time header.
	HeaderDismantlingTimestamp = "dismantlingTime"

	// TimeFormat is the format used to present the bundle's dismantling timestamp.
	TimeFormat = time.RFC3339
)
