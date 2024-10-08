package replication

const (
	// TenDB compressed event from 50
	TENDB_QUERY_COMPRESSED_EVENT          EventType = 50 + iota
	TENDB_WRITE_ROWS_COMPRESSED_EVENT_V1            = 51
	TENDB_UPDATE_ROWS_COMPRESSED_EVENT_V1           = 52
	TENDB_DELETE_ROWS_COMPRESSED_EVENT_V1           = 53
	TENDB_WRITE_ROWS_COMPRESSED_EVENT_V2            = 54
	TENDB_UPDATE_ROWS_COMPRESSED_EVENT_V2           = 55
	TENDB_DELETE_ROWS_COMPRESSED_EVENT_V2           = 56
)
