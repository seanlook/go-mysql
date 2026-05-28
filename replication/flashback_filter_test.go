package replication

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"testing"

	. "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/pkg"
	"github.com/go-mysql-org/go-mysql/pkg/db_table_filter"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/stretchr/testify/require"
)

// buildRowsEventRawData 构建一个简单的 RowsEvent 原始数据（含 event header）
// tableID: 表ID
// eventType: 事件类型
// columnCount: 列数
// rowData: 行数据（不含 bitmap 和 column count）
func buildRowsEventRawData(tableID uint64, eventType EventType, columnCount int, rowData []byte) []byte {
	// Event Header (19 bytes)
	header := make([]byte, EventHeaderSize)
	binary.LittleEndian.PutUint32(header[TimestampPos:], 1000000) // 原始时间戳
	header[EventTypePos] = byte(eventType)
	binary.LittleEndian.PutUint32(header[5:9], 0x0b)                                     // server_id
	binary.LittleEndian.PutUint32(header[EventSizPos:EventSizPos+4], uint32(0))           // event_size placeholder
	binary.LittleEndian.PutUint32(header[13:17], 0)                                       // log_pos
	binary.LittleEndian.PutUint16(header[17:19], 1)                                       // flags
	_ = binary.LittleEndian.Uint32(header[EventSizPos : EventSizPos+4])                   // placeholder

	// Event Body
	// table_id (6 bytes for v2)
	body := make([]byte, 0, 64)
	tidBuf := make([]byte, 6)
	tidBuf[0] = byte(tableID)
	tidBuf[1] = byte(tableID >> 8)
	tidBuf[2] = byte(tableID >> 16)
	tidBuf[3] = byte(tableID >> 24)
	tidBuf[4] = byte(tableID >> 32)
	tidBuf[5] = byte(tableID >> 40)
	body = append(body, tidBuf...)

	// flags (2 bytes)
	body = append(body, 0x01, 0x00)

	// extra_data_length (2 bytes, v2 only)
	body = append(body, 0x02, 0x00)

	// column_count (lenenc_int)
	body = append(body, byte(columnCount))

	// column bitmap1
	bitmapSize := (columnCount + 7) / 8
	bitmap := make([]byte, bitmapSize)
	for i := range bitmap {
		bitmap[i] = 0xFF // 所有列都包含
	}
	body = append(body, bitmap...)

	// 如果是 UPDATE 事件，需要第二个 bitmap
	if eventType == UPDATE_ROWS_EVENTv2 || eventType == UPDATE_ROWS_EVENTv1 {
		body = append(body, bitmap...)
	}

	// row data
	body = append(body, rowData...)

	// 更新 event_size
	totalSize := uint32(EventHeaderSize + len(body))
	binary.LittleEndian.PutUint32(header[EventSizPos:EventSizPos+4], totalSize)

	result := make([]byte, 0, len(header)+len(body))
	result = append(result, header...)
	result = append(result, body...)
	return result
}

// ============================================================
// 1. 闪回转换测试 - FlashbackData2
// ============================================================

// TestFlashbackInsertToDelete 验证 INSERT 事件闪回后变为 DELETE 事件
func TestFlashbackInsertToDelete(t *testing.T) {
	table := &TableMapEvent{
		tableIDSize: 6,
		TableID:     0x6c,
		Schema:      []byte("db"),
		Table:       []byte("tbl"),
		ColumnCount: 1,
		ColumnType:  []byte{MYSQL_TYPE_LONG},
		ColumnMeta:  []uint16{0},
		NullBitmap:  []byte{0x00},
	}

	// 构建 INT 值为 1 的行数据: null_bitmap(1 byte) + int32(4 bytes)
	rowData := []byte{0x00, 0x01, 0x00, 0x00, 0x00}

	rawData := buildRowsEventRawData(0x6c, WRITE_ROWS_EVENTv2, 1, rowData)

	e := &RowsEvent{
		Version:     2,
		tableIDSize: 6,
		tables:      map[uint64]*TableMapEvent{0x6c: table},
		eventType:   WRITE_ROWS_EVENTv2,
		flashback:   true,
		rawBytesNew: make([]byte, len(rawData)),
	}
	copy(e.rawBytesNew, rawData)

	// 解析 header
	bodyData := rawData[EventHeaderSize:]
	pos, err := e.DecodeHeader(bodyData)
	require.NoError(t, err)

	// 执行闪回
	err = e.FlashbackData2(pos, bodyData)
	require.NoError(t, err)

	// 验证事件类型变为 DELETE
	require.Equal(t, DELETE_ROWS_EVENTv2, e.eventType)
	// 验证 rawBytesNew 中的事件类型也变了
	require.Equal(t, byte(DELETE_ROWS_EVENTv2), e.rawBytesNew[EventTypePos])

	// 验证行数据正确解析
	require.Len(t, e.Rows, 1)
	require.Equal(t, int32(1), e.Rows[0][0])
}

// TestFlashbackDeleteToInsert 验证 DELETE 事件闪回后变为 INSERT 事件
func TestFlashbackDeleteToInsert(t *testing.T) {
	table := &TableMapEvent{
		tableIDSize: 6,
		TableID:     0x6c,
		Schema:      []byte("db"),
		Table:       []byte("tbl"),
		ColumnCount: 1,
		ColumnType:  []byte{MYSQL_TYPE_LONG},
		ColumnMeta:  []uint16{0},
		NullBitmap:  []byte{0x00},
	}

	// 构建 INT 值为 42 的行数据
	rowData := []byte{0x00, 0x2a, 0x00, 0x00, 0x00}

	rawData := buildRowsEventRawData(0x6c, DELETE_ROWS_EVENTv2, 1, rowData)

	e := &RowsEvent{
		Version:     2,
		tableIDSize: 6,
		tables:      map[uint64]*TableMapEvent{0x6c: table},
		eventType:   DELETE_ROWS_EVENTv2,
		flashback:   true,
		rawBytesNew: make([]byte, len(rawData)),
	}
	copy(e.rawBytesNew, rawData)

	bodyData := rawData[EventHeaderSize:]
	pos, err := e.DecodeHeader(bodyData)
	require.NoError(t, err)

	err = e.FlashbackData2(pos, bodyData)
	require.NoError(t, err)

	// 验证事件类型变为 INSERT
	require.Equal(t, WRITE_ROWS_EVENTv2, e.eventType)
	require.Equal(t, byte(WRITE_ROWS_EVENTv2), e.rawBytesNew[EventTypePos])

	// 验证行数据
	require.Len(t, e.Rows, 1)
	require.Equal(t, int32(42), e.Rows[0][0])
}

// TestFlashbackUpdateSwapImages 验证 UPDATE 事件闪回后前后镜像互换
func TestFlashbackUpdateSwapImages(t *testing.T) {
	table := &TableMapEvent{
		tableIDSize: 6,
		TableID:     0x6c,
		Schema:      []byte("db"),
		Table:       []byte("tbl"),
		ColumnCount: 1,
		ColumnType:  []byte{MYSQL_TYPE_LONG},
		ColumnMeta:  []uint16{0},
		NullBitmap:  []byte{0x00},
	}

	// UPDATE: before image (value=10) + after image (value=20)
	// null_bitmap(1) + int32(4) for each image
	rowData := []byte{
		0x00, 0x0a, 0x00, 0x00, 0x00, // BI: value=10
		0x00, 0x14, 0x00, 0x00, 0x00, // AI: value=20
	}

	rawData := buildRowsEventRawData(0x6c, UPDATE_ROWS_EVENTv2, 1, rowData)

	e := &RowsEvent{
		Version:     2,
		tableIDSize: 6,
		tables:      map[uint64]*TableMapEvent{0x6c: table},
		eventType:   UPDATE_ROWS_EVENTv2,
		needBitmap2: true,
		flashback:   true,
		rawBytesNew: make([]byte, len(rawData)),
	}
	copy(e.rawBytesNew, rawData)

	bodyData := rawData[EventHeaderSize:]
	pos, err := e.DecodeHeader(bodyData)
	require.NoError(t, err)

	err = e.FlashbackData2(pos, bodyData)
	require.NoError(t, err)

	// 验证事件类型仍然是 UPDATE
	require.Equal(t, UPDATE_ROWS_EVENTv2, e.eventType)

	// 验证前后镜像互换：原来 BI=10, AI=20，闪回后 BI=20, AI=10
	require.Len(t, e.Rows, 2)
	require.Equal(t, int32(20), e.Rows[0][0]) // 新的 BI（原来的 AI）
	require.Equal(t, int32(10), e.Rows[1][0]) // 新的 AI（原来的 BI）
}

// TestFlashbackMultipleRows 验证多行 INSERT 闪回
func TestFlashbackMultipleRows(t *testing.T) {
	table := &TableMapEvent{
		tableIDSize: 6,
		TableID:     0x6c,
		Schema:      []byte("db"),
		Table:       []byte("tbl"),
		ColumnCount: 1,
		ColumnType:  []byte{MYSQL_TYPE_LONG},
		ColumnMeta:  []uint16{0},
		NullBitmap:  []byte{0x00},
	}

	// 3 行 INSERT: value=1, value=2, value=3
	rowData := []byte{
		0x00, 0x01, 0x00, 0x00, 0x00,
		0x00, 0x02, 0x00, 0x00, 0x00,
		0x00, 0x03, 0x00, 0x00, 0x00,
	}

	rawData := buildRowsEventRawData(0x6c, WRITE_ROWS_EVENTv2, 1, rowData)

	e := &RowsEvent{
		Version:     2,
		tableIDSize: 6,
		tables:      map[uint64]*TableMapEvent{0x6c: table},
		eventType:   WRITE_ROWS_EVENTv2,
		flashback:   true,
		rawBytesNew: make([]byte, len(rawData)),
	}
	copy(e.rawBytesNew, rawData)

	bodyData := rawData[EventHeaderSize:]
	pos, err := e.DecodeHeader(bodyData)
	require.NoError(t, err)

	err = e.FlashbackData2(pos, bodyData)
	require.NoError(t, err)

	// 验证事件类型变为 DELETE
	require.Equal(t, DELETE_ROWS_EVENTv2, e.eventType)

	// 验证 3 行数据都正确解析
	require.Len(t, e.Rows, 3)
	require.Equal(t, int32(1), e.Rows[0][0])
	require.Equal(t, int32(2), e.Rows[1][0])
	require.Equal(t, int32(3), e.Rows[2][0])
}

// TestFlashbackMultipleUpdateRows 验证多行 UPDATE 闪回后前后镜像互换
func TestFlashbackMultipleUpdateRows(t *testing.T) {
	table := &TableMapEvent{
		tableIDSize: 6,
		TableID:     0x6c,
		Schema:      []byte("db"),
		Table:       []byte("tbl"),
		ColumnCount: 1,
		ColumnType:  []byte{MYSQL_TYPE_LONG},
		ColumnMeta:  []uint16{0},
		NullBitmap:  []byte{0x00},
	}

	// 2 行 UPDATE: row1 BI=10 AI=11, row2 BI=20 AI=21
	rowData := []byte{
		0x00, 0x0a, 0x00, 0x00, 0x00, // row1 BI: 10
		0x00, 0x0b, 0x00, 0x00, 0x00, // row1 AI: 11
		0x00, 0x14, 0x00, 0x00, 0x00, // row2 BI: 20
		0x00, 0x15, 0x00, 0x00, 0x00, // row2 AI: 21
	}

	rawData := buildRowsEventRawData(0x6c, UPDATE_ROWS_EVENTv2, 1, rowData)

	e := &RowsEvent{
		Version:     2,
		tableIDSize: 6,
		tables:      map[uint64]*TableMapEvent{0x6c: table},
		eventType:   UPDATE_ROWS_EVENTv2,
		needBitmap2: true,
		flashback:   true,
		rawBytesNew: make([]byte, len(rawData)),
	}
	copy(e.rawBytesNew, rawData)

	bodyData := rawData[EventHeaderSize:]
	pos, err := e.DecodeHeader(bodyData)
	require.NoError(t, err)

	err = e.FlashbackData2(pos, bodyData)
	require.NoError(t, err)

	// 验证 4 行（2对 BI/AI）
	require.Len(t, e.Rows, 4)
	// row1: 闪回后 BI=11(原AI), AI=10(原BI)
	require.Equal(t, int32(11), e.Rows[0][0])
	require.Equal(t, int32(10), e.Rows[1][0])
	// row2: 闪回后 BI=21(原AI), AI=20(原BI)
	require.Equal(t, int32(21), e.Rows[2][0])
	require.Equal(t, int32(20), e.Rows[3][0])
}

// TestFlashbackV1Events 验证 v1 版本事件的闪回
func TestFlashbackV1Events(t *testing.T) {
	table := &TableMapEvent{
		tableIDSize: 6,
		TableID:     0x6c,
		Schema:      []byte("db"),
		Table:       []byte("tbl"),
		ColumnCount: 1,
		ColumnType:  []byte{MYSQL_TYPE_LONG},
		ColumnMeta:  []uint16{0},
		NullBitmap:  []byte{0x00},
	}

	rowData := []byte{0x00, 0x05, 0x00, 0x00, 0x00}

	// 构建 v1 WRITE 事件（没有 extra_data_length 字段）
	header := make([]byte, EventHeaderSize)
	binary.LittleEndian.PutUint32(header[TimestampPos:], 1000000)
	header[EventTypePos] = byte(WRITE_ROWS_EVENTv1)
	binary.LittleEndian.PutUint32(header[5:9], 0x0b)

	body := make([]byte, 0, 32)
	// table_id (6 bytes)
	tidBuf := make([]byte, 6)
	tidBuf[0] = 0x6c
	body = append(body, tidBuf...)
	// flags
	body = append(body, 0x01, 0x00)
	// v1 没有 extra_data_length
	// column_count
	body = append(body, 0x01)
	// bitmap
	body = append(body, 0xFF)
	// row data
	body = append(body, rowData...)

	totalSize := uint32(EventHeaderSize + len(body))
	binary.LittleEndian.PutUint32(header[EventSizPos:EventSizPos+4], totalSize)
	binary.LittleEndian.PutUint16(header[17:19], 1)

	rawData := append(header, body...)

	e := &RowsEvent{
		Version:     1,
		tableIDSize: 6,
		tables:      map[uint64]*TableMapEvent{0x6c: table},
		eventType:   WRITE_ROWS_EVENTv1,
		flashback:   true,
		rawBytesNew: make([]byte, len(rawData)),
	}
	copy(e.rawBytesNew, rawData)

	bodyData := rawData[EventHeaderSize:]
	pos, err := e.DecodeHeader(bodyData)
	require.NoError(t, err)

	err = e.FlashbackData2(pos, bodyData)
	require.NoError(t, err)

	// 验证 v1 INSERT 变为 v1 DELETE
	require.Equal(t, DELETE_ROWS_EVENTv1, e.eventType)
	require.Equal(t, byte(DELETE_ROWS_EVENTv1), e.rawBytesNew[EventTypePos])
	require.Len(t, e.Rows, 1)
	require.Equal(t, int32(5), e.Rows[0][0])
}

// ============================================================
// 2. RowsFilter 行过滤测试 - 不同字段类型
// ============================================================

// TestRowsFilterCompile 验证 RowsFilter 表达式编译
func TestRowsFilterCompile(t *testing.T) {
	testCases := []struct {
		name    string
		expr    string
		wantErr bool
	}{
		{"简单等值比较", "col[0] == 1", false},
		{"字符串比较", `col[0] == "hello"`, false},
		{"大于比较", "col[0] > 100", false},
		{"逻辑与", "col[0] > 1 && col[1] < 10", false},
		{"逻辑或", `col[0] == 1 || col[0] == 2`, false},
		{"无效表达式", "col[0] ===", true},
		{"包含函数", `col[0] contains "test"`, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rf, err := NewRowsFilter(tc.expr)
			if tc.wantErr {
				require.Error(t, err)
				require.Nil(t, rf)
			} else {
				require.NoError(t, err)
				require.NotNil(t, rf)
				require.NotNil(t, rf.CompiledColumnFilterExpr)
			}
		})
	}
}

// TestRowsFilterIntType 验证 INT 类型字段的行过滤
func TestRowsFilterIntType(t *testing.T) {
	table := &TableMapEvent{
		tableIDSize: 6,
		TableID:     0x6c,
		Schema:      []byte("db"),
		Table:       []byte("tbl"),
		ColumnCount: 1,
		ColumnType:  []byte{MYSQL_TYPE_LONG},
		ColumnMeta:  []uint16{0},
		NullBitmap:  []byte{0x00},
	}

	// 3 行: value=1, value=5, value=10
	rowData := []byte{
		0x00, 0x01, 0x00, 0x00, 0x00,
		0x00, 0x05, 0x00, 0x00, 0x00,
		0x00, 0x0a, 0x00, 0x00, 0x00,
	}

	rawData := buildRowsEventRawData(0x6c, WRITE_ROWS_EVENTv2, 1, rowData)

	// 过滤条件: col[0] > 3
	rf, err := NewRowsFilter("col[0] > 3")
	require.NoError(t, err)

	e := &RowsEvent{
		Version:     2,
		tableIDSize: 6,
		tables:      map[uint64]*TableMapEvent{0x6c: table},
		eventType:   WRITE_ROWS_EVENTv2,
		flashback:   true,
		rowsFilter:  rf,
		rawBytesNew: make([]byte, len(rawData)),
	}
	copy(e.rawBytesNew, rawData)

	bodyData := rawData[EventHeaderSize:]
	pos, err := e.DecodeHeader(bodyData)
	require.NoError(t, err)

	err = e.FlashbackData2(pos, bodyData)
	require.NoError(t, err)

	// 只有 value=5 和 value=10 满足 col[0] > 3
	require.Equal(t, 2, e.RowsMatched)
	require.Len(t, e.Rows, 2)
	require.Equal(t, int32(5), e.Rows[0][0])
	require.Equal(t, int32(10), e.Rows[1][0])
}

// TestRowsFilterUint64Overflow 验证 BIGINT UNSIGNED 大值（超过 int64 最大值）不会溢出
func TestRowsFilterUint64Overflow(t *testing.T) {
	table := &TableMapEvent{
		tableIDSize:      6,
		TableID:          0x6c,
		Schema:           []byte("db"),
		Table:            []byte("tbl"),
		ColumnCount:      1,
		ColumnType:       []byte{MYSQL_TYPE_LONGLONG},
		ColumnMeta:       []uint16{0},
		NullBitmap:       []byte{0x00},
		SignednessBitmap: []byte{0x80}, // bit=1 表示 unsigned
	}

	// uint64 值 16141183638984196173 = 0xE00101000000004D
	// 小端序: 4D 00 00 00 00 01 01 E0
	// 该值超过 int64 最大值 9223372036854775807，验证不会溢出
	rowData := []byte{
		0x00, 0x4D, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0xE0, // row1: 16141183638984196173
		0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // row2: 1
	}

	rawData := buildRowsEventRawData(0x6c, WRITE_ROWS_EVENTv2, 1, rowData)

	// 过滤条件: col[0] > 9223372036854775807 (大于 int64 最大值)
	rf, err := NewRowsFilter("col[0] > 9223372036854775807")
	require.NoError(t, err)

	e := &RowsEvent{
		Version:     2,
		tableIDSize: 6,
		tables:      map[uint64]*TableMapEvent{0x6c: table},
		eventType:   WRITE_ROWS_EVENTv2,
		flashback:   true,
		rowsFilter:  rf,
		rawBytesNew: make([]byte, len(rawData)),
	}
	copy(e.rawBytesNew, rawData)

	bodyData := rawData[EventHeaderSize:]
	pos, err := e.DecodeHeader(bodyData)
	require.NoError(t, err)

	err = e.FlashbackData2(pos, bodyData)
	require.NoError(t, err)

	// 16141183638984196173 > 9223372036854775807，应该匹配
	// 1 < 9223372036854775807，不匹配
	require.Equal(t, 1, e.RowsMatched)
	require.Len(t, e.Rows, 1)
	require.Equal(t, uint64(16141183638984196173), e.Rows[0][0])

	// 子测试：精确等值匹配大 uint64 值
	t.Run("精确等值匹配", func(t *testing.T) {
		rawData2 := buildRowsEventRawData(0x6c, WRITE_ROWS_EVENTv2, 1, rowData)

		rf2, err := NewRowsFilter("col[0] == 16141183638984196173")
		require.NoError(t, err)

		e2 := &RowsEvent{
			Version:     2,
			tableIDSize: 6,
			tables:      map[uint64]*TableMapEvent{0x6c: table},
			eventType:   WRITE_ROWS_EVENTv2,
			flashback:   true,
			rowsFilter:  rf2,
			rawBytesNew: make([]byte, len(rawData2)),
		}
		copy(e2.rawBytesNew, rawData2)

		bodyData2 := rawData2[EventHeaderSize:]
		pos2, err := e2.DecodeHeader(bodyData2)
		require.NoError(t, err)

		err = e2.FlashbackData2(pos2, bodyData2)
		require.NoError(t, err)

		// 精确匹配 16141183638984196173，只有 row1 匹配
		require.Equal(t, 1, e2.RowsMatched)
		require.Len(t, e2.Rows, 1)
		require.Equal(t, uint64(16141183638984196173), e2.Rows[0][0])
	})
}

// TestRowsFilterStringType 验证 STRING/VARCHAR 类型字段的行过滤
func TestRowsFilterStringType(t *testing.T) {
	table := &TableMapEvent{
		tableIDSize: 6,
		TableID:     0x6c,
		Schema:      []byte("db"),
		Table:       []byte("tbl"),
		ColumnCount: 2,
		ColumnType:  []byte{MYSQL_TYPE_LONG, MYSQL_TYPE_VARCHAR},
		ColumnMeta:  []uint16{0, 100}, // VARCHAR(100)
		NullBitmap:  []byte{0x00},
	}

	// 构建 2 行: (1, "hello"), (2, "world")
	// row1: null_bitmap(1) + int32(4) + varchar_len(1) + varchar_data
	// row2: null_bitmap(1) + int32(4) + varchar_len(1) + varchar_data
	rowData := []byte{
		// row1: id=1, name="hello"
		0x00,                         // null bitmap
		0x01, 0x00, 0x00, 0x00,       // INT: 1
		0x05, 'h', 'e', 'l', 'l', 'o', // VARCHAR: "hello" (len=5)
		// row2: id=2, name="world"
		0x00,                         // null bitmap
		0x02, 0x00, 0x00, 0x00,       // INT: 2
		0x05, 'w', 'o', 'r', 'l', 'd', // VARCHAR: "world" (len=5)
	}

	rawData := buildRowsEventRawData(0x6c, WRITE_ROWS_EVENTv2, 2, rowData)

	// 过滤条件: col[1] == "hello"
	rf, err := NewRowsFilter(`col[1] == "hello"`)
	require.NoError(t, err)

	e := &RowsEvent{
		Version:     2,
		tableIDSize: 6,
		tables:      map[uint64]*TableMapEvent{0x6c: table},
		eventType:   WRITE_ROWS_EVENTv2,
		flashback:   true,
		rowsFilter:  rf,
		rawBytesNew: make([]byte, len(rawData)),
	}
	copy(e.rawBytesNew, rawData)

	bodyData := rawData[EventHeaderSize:]
	pos, err := e.DecodeHeader(bodyData)
	require.NoError(t, err)

	err = e.FlashbackData2(pos, bodyData)
	require.NoError(t, err)

	// 只有 "hello" 匹配
	require.Equal(t, 1, e.RowsMatched)
	require.Len(t, e.Rows, 1)
	require.Equal(t, int32(1), e.Rows[0][0])
	require.Equal(t, "hello", e.Rows[0][1])
}

// TestRowsFilterNoMatch 验证没有行匹配时 rawBytesNew 被截断
func TestRowsFilterNoMatch(t *testing.T) {
	table := &TableMapEvent{
		tableIDSize: 6,
		TableID:     0x6c,
		Schema:      []byte("db"),
		Table:       []byte("tbl"),
		ColumnCount: 1,
		ColumnType:  []byte{MYSQL_TYPE_LONG},
		ColumnMeta:  []uint16{0},
		NullBitmap:  []byte{0x00},
	}

	// value=1
	rowData := []byte{0x00, 0x01, 0x00, 0x00, 0x00}

	rawData := buildRowsEventRawData(0x6c, WRITE_ROWS_EVENTv2, 1, rowData)

	// 过滤条件: col[0] > 100 (不匹配)
	rf, err := NewRowsFilter("col[0] > 100")
	require.NoError(t, err)

	e := &RowsEvent{
		Version:     2,
		tableIDSize: 6,
		tables:      map[uint64]*TableMapEvent{0x6c: table},
		eventType:   WRITE_ROWS_EVENTv2,
		flashback:   true,
		rowsFilter:  rf,
		rawBytesNew: make([]byte, len(rawData)),
	}
	copy(e.rawBytesNew, rawData)

	bodyData := rawData[EventHeaderSize:]
	pos, err := e.DecodeHeader(bodyData)
	require.NoError(t, err)

	err = e.FlashbackData2(pos, bodyData)
	require.NoError(t, err)

	// 没有匹配行
	require.Equal(t, 0, e.RowsMatched)
	require.Len(t, e.Rows, 0)
	// rawBytesNew 应该被截断到只有 header 大小
	require.Equal(t, EventHeaderSize, len(e.rawBytesNew))
}

// TestRowsFilterMultiColumn 验证多列组合过滤
func TestRowsFilterMultiColumn(t *testing.T) {
	table := &TableMapEvent{
		tableIDSize: 6,
		TableID:     0x6c,
		Schema:      []byte("db"),
		Table:       []byte("tbl"),
		ColumnCount: 2,
		ColumnType:  []byte{MYSQL_TYPE_LONG, MYSQL_TYPE_LONG},
		ColumnMeta:  []uint16{0, 0},
		NullBitmap:  []byte{0x00},
	}

	// 3 行: (1,100), (2,200), (3,300)
	rowData := []byte{
		0x00, 0x01, 0x00, 0x00, 0x00, 0x64, 0x00, 0x00, 0x00, // (1, 100)
		0x00, 0x02, 0x00, 0x00, 0x00, 0xc8, 0x00, 0x00, 0x00, // (2, 200)
		0x00, 0x03, 0x00, 0x00, 0x00, 0x2c, 0x01, 0x00, 0x00, // (3, 300)
	}

	rawData := buildRowsEventRawData(0x6c, WRITE_ROWS_EVENTv2, 2, rowData)

	// 过滤条件: col[0] >= 2 && col[1] > 150
	rf, err := NewRowsFilter("col[0] >= 2 && col[1] > 150")
	require.NoError(t, err)

	e := &RowsEvent{
		Version:     2,
		tableIDSize: 6,
		tables:      map[uint64]*TableMapEvent{0x6c: table},
		eventType:   WRITE_ROWS_EVENTv2,
		flashback:   true,
		rowsFilter:  rf,
		rawBytesNew: make([]byte, len(rawData)),
	}
	copy(e.rawBytesNew, rawData)

	bodyData := rawData[EventHeaderSize:]
	pos, err := e.DecodeHeader(bodyData)
	require.NoError(t, err)

	err = e.FlashbackData2(pos, bodyData)
	require.NoError(t, err)

	// (2,200) 和 (3,300) 满足条件
	require.Equal(t, 2, e.RowsMatched)
	require.Len(t, e.Rows, 2)
	require.Equal(t, int32(2), e.Rows[0][0])
	require.Equal(t, int32(200), e.Rows[0][1])
	require.Equal(t, int32(3), e.Rows[1][0])
	require.Equal(t, int32(300), e.Rows[1][1])
}

// TestRowsFilterTinyIntType 验证 TINYINT 类型字段的行过滤
func TestRowsFilterTinyIntType(t *testing.T) {
	table := &TableMapEvent{
		tableIDSize: 6,
		TableID:     0x6c,
		Schema:      []byte("db"),
		Table:       []byte("tbl"),
		ColumnCount: 1,
		ColumnType:  []byte{MYSQL_TYPE_TINY},
		ColumnMeta:  []uint16{0},
		NullBitmap:  []byte{0x00},
	}

	// 3 行 TINYINT: value=1, value=5, value=10
	rowData := []byte{
		0x00, 0x01, // row1: value=1
		0x00, 0x05, // row2: value=5
		0x00, 0x0a, // row3: value=10
	}

	rawData := buildRowsEventRawData(0x6c, WRITE_ROWS_EVENTv2, 1, rowData)

	// 过滤条件: col[0] == 5
	rf, err := NewRowsFilter("col[0] == 5")
	require.NoError(t, err)

	e := &RowsEvent{
		Version:     2,
		tableIDSize: 6,
		tables:      map[uint64]*TableMapEvent{0x6c: table},
		eventType:   WRITE_ROWS_EVENTv2,
		flashback:   true,
		rowsFilter:  rf,
		rawBytesNew: make([]byte, len(rawData)),
	}
	copy(e.rawBytesNew, rawData)

	bodyData := rawData[EventHeaderSize:]
	pos, err := e.DecodeHeader(bodyData)
	require.NoError(t, err)

	err = e.FlashbackData2(pos, bodyData)
	require.NoError(t, err)

	// 只有 value=5 匹配
	require.Equal(t, 1, e.RowsMatched)
	require.Len(t, e.Rows, 1)
	require.Equal(t, int8(5), e.Rows[0][0])
}

// TestRowsFilterNullValue 验证 NULL 值的行过滤
func TestRowsFilterNullValue(t *testing.T) {
	table := &TableMapEvent{
		tableIDSize: 6,
		TableID:     0x6c,
		Schema:      []byte("db"),
		Table:       []byte("tbl"),
		ColumnCount: 2,
		ColumnType:  []byte{MYSQL_TYPE_LONG, MYSQL_TYPE_LONG},
		ColumnMeta:  []uint16{0, 0},
		NullBitmap:  []byte{0x02}, // 第二列可为 NULL
	}

	// row1: id=1, col2=NULL (null bitmap bit 1 set)
	// row2: id=2, col2=100
	rowData := []byte{
		0x02, 0x01, 0x00, 0x00, 0x00, // row1: null_bitmap=0x02(col2 is null), id=1
		0x00, 0x02, 0x00, 0x00, 0x00, 0x64, 0x00, 0x00, 0x00, // row2: null_bitmap=0x00, id=2, col2=100
	}

	rawData := buildRowsEventRawData(0x6c, WRITE_ROWS_EVENTv2, 2, rowData)

	// 过滤条件: col[0] == 2 (只匹配第二行)
	rf, err := NewRowsFilter("col[0] == 2")
	require.NoError(t, err)

	e := &RowsEvent{
		Version:     2,
		tableIDSize: 6,
		tables:      map[uint64]*TableMapEvent{0x6c: table},
		eventType:   WRITE_ROWS_EVENTv2,
		flashback:   true,
		rowsFilter:  rf,
		rawBytesNew: make([]byte, len(rawData)),
	}
	copy(e.rawBytesNew, rawData)

	bodyData := rawData[EventHeaderSize:]
	pos, err := e.DecodeHeader(bodyData)
	require.NoError(t, err)

	err = e.FlashbackData2(pos, bodyData)
	require.NoError(t, err)

	require.Equal(t, 1, e.RowsMatched)
	require.Len(t, e.Rows, 1)
	require.Equal(t, int32(2), e.Rows[0][0])
	require.Equal(t, int32(100), e.Rows[0][1])
}

// TestRowsFilterAllNumberToString 验证 AllNumberToString 模式
func TestRowsFilterAllNumberToString(t *testing.T) {
	table := &TableMapEvent{
		tableIDSize: 6,
		TableID:     0x6c,
		Schema:      []byte("db"),
		Table:       []byte("tbl"),
		ColumnCount: 1,
		ColumnType:  []byte{MYSQL_TYPE_LONG},
		ColumnMeta:  []uint16{0},
		NullBitmap:  []byte{0x00},
	}

	// value=42
	rowData := []byte{0x00, 0x2a, 0x00, 0x00, 0x00}

	rawData := buildRowsEventRawData(0x6c, WRITE_ROWS_EVENTv2, 1, rowData)

	// 使用字符串比较（AllNumberToString=true 时数字会转为字符串）
	rf, err := NewRowsFilter(`col[0] == "42"`)
	require.NoError(t, err)
	rf.AllNumberToString = true

	e := &RowsEvent{
		Version:     2,
		tableIDSize: 6,
		tables:      map[uint64]*TableMapEvent{0x6c: table},
		eventType:   WRITE_ROWS_EVENTv2,
		flashback:   true,
		rowsFilter:  rf,
		rawBytesNew: make([]byte, len(rawData)),
	}
	copy(e.rawBytesNew, rawData)

	bodyData := rawData[EventHeaderSize:]
	pos, err := e.DecodeHeader(bodyData)
	require.NoError(t, err)

	err = e.FlashbackData2(pos, bodyData)
	require.NoError(t, err)

	// AllNumberToString 模式下，int32(42) 转为 "42" 进行比较
	require.Equal(t, 1, e.RowsMatched)
}

// TestConvertColumnValueToWhereCond 验证列值转换函数
func TestConvertColumnValueToWhereCond(t *testing.T) {
	t.Run("nil值保持不变", func(t *testing.T) {
		cols := []interface{}{nil, int32(1)}
		result := convertColumnValueToWhereCond(cols, false)
		require.Nil(t, result[0])
		require.Equal(t, int32(1), result[1])
	})

	t.Run("长字符串截断到1024", func(t *testing.T) {
		longStr := make([]byte, 2000)
		for i := range longStr {
			longStr[i] = 'a'
		}
		cols := []interface{}{string(longStr)}
		result := convertColumnValueToWhereCond(cols, false)
		require.Len(t, result[0].(string), 1024)
	})

	t.Run("长bytes截断到1024", func(t *testing.T) {
		longBytes := make([]byte, 2000)
		for i := range longBytes {
			longBytes[i] = 0x41
		}
		cols := []interface{}{longBytes}
		result := convertColumnValueToWhereCond(cols, false)
		require.Len(t, result[0].([]byte), 1024)
	})

	t.Run("AllNumberToString转换", func(t *testing.T) {
		cols := []interface{}{int32(42), float64(3.14), uint64(100)}
		result := convertColumnValueToWhereCond(cols, true)
		require.Equal(t, "42", result[0])
		require.Equal(t, "3.14", result[1])
		require.Equal(t, "100", result[2])
	})

	t.Run("不转换数字", func(t *testing.T) {
		cols := []interface{}{int32(42), float64(3.14)}
		result := convertColumnValueToWhereCond(cols, false)
		require.Equal(t, int32(42), result[0])
		require.Equal(t, float64(3.14), result[1])
	})
}

// ============================================================
// 3. 闪回 + RowsFilter 组合场景测试
// ============================================================

// TestFlashbackWithRowsFilterInsert 验证闪回+行过滤组合：INSERT 事件
func TestFlashbackWithRowsFilterInsert(t *testing.T) {
	table := &TableMapEvent{
		tableIDSize: 6,
		TableID:     0x6c,
		Schema:      []byte("db"),
		Table:       []byte("tbl"),
		ColumnCount: 1,
		ColumnType:  []byte{MYSQL_TYPE_LONG},
		ColumnMeta:  []uint16{0},
		NullBitmap:  []byte{0x00},
	}

	// 3 行: value=1, value=5, value=10
	rowData := []byte{
		0x00, 0x01, 0x00, 0x00, 0x00,
		0x00, 0x05, 0x00, 0x00, 0x00,
		0x00, 0x0a, 0x00, 0x00, 0x00,
	}

	rawData := buildRowsEventRawData(0x6c, WRITE_ROWS_EVENTv2, 1, rowData)

	// 过滤条件: col[0] >= 5
	rf, err := NewRowsFilter("col[0] >= 5")
	require.NoError(t, err)

	e := &RowsEvent{
		Version:     2,
		tableIDSize: 6,
		tables:      map[uint64]*TableMapEvent{0x6c: table},
		eventType:   WRITE_ROWS_EVENTv2,
		flashback:   true,
		rowsFilter:  rf,
		rawBytesNew: make([]byte, len(rawData)),
	}
	copy(e.rawBytesNew, rawData)

	bodyData := rawData[EventHeaderSize:]
	pos, err := e.DecodeHeader(bodyData)
	require.NoError(t, err)

	err = e.FlashbackData2(pos, bodyData)
	require.NoError(t, err)

	// 闪回: INSERT → DELETE，且只保留 col[0] >= 5 的行
	require.Equal(t, DELETE_ROWS_EVENTv2, e.eventType)
	require.Equal(t, 2, e.RowsMatched)
	require.Len(t, e.Rows, 2)
	require.Equal(t, int32(5), e.Rows[0][0])
	require.Equal(t, int32(10), e.Rows[1][0])
}

// TestFlashbackWithRowsFilterUpdate 验证闪回+行过滤组合：UPDATE 事件
func TestFlashbackWithRowsFilterUpdate(t *testing.T) {
	table := &TableMapEvent{
		tableIDSize: 6,
		TableID:     0x6c,
		Schema:      []byte("db"),
		Table:       []byte("tbl"),
		ColumnCount: 1,
		ColumnType:  []byte{MYSQL_TYPE_LONG},
		ColumnMeta:  []uint16{0},
		NullBitmap:  []byte{0x00},
	}

	// 2 行 UPDATE: row1 BI=1 AI=11, row2 BI=5 AI=55
	rowData := []byte{
		0x00, 0x01, 0x00, 0x00, 0x00, // row1 BI: 1
		0x00, 0x0b, 0x00, 0x00, 0x00, // row1 AI: 11
		0x00, 0x05, 0x00, 0x00, 0x00, // row2 BI: 5
		0x00, 0x37, 0x00, 0x00, 0x00, // row2 AI: 55
	}

	rawData := buildRowsEventRawData(0x6c, UPDATE_ROWS_EVENTv2, 1, rowData)

	// 过滤条件: col[0] >= 5 (基于 BI 过滤)
	rf, err := NewRowsFilter("col[0] >= 5")
	require.NoError(t, err)

	e := &RowsEvent{
		Version:     2,
		tableIDSize: 6,
		tables:      map[uint64]*TableMapEvent{0x6c: table},
		eventType:   UPDATE_ROWS_EVENTv2,
		needBitmap2: true,
		flashback:   true,
		rowsFilter:  rf,
		rawBytesNew: make([]byte, len(rawData)),
	}
	copy(e.rawBytesNew, rawData)

	bodyData := rawData[EventHeaderSize:]
	pos, err := e.DecodeHeader(bodyData)
	require.NoError(t, err)

	err = e.FlashbackData2(pos, bodyData)
	require.NoError(t, err)

	// 只有 row2 (BI=5) 满足过滤条件
	require.Equal(t, UPDATE_ROWS_EVENTv2, e.eventType)
	require.Equal(t, 1, e.RowsMatched)
	// 闪回后前后镜像互换: BI=55(原AI), AI=5(原BI)
	require.Len(t, e.Rows, 2)
	require.Equal(t, int32(55), e.Rows[0][0]) // 新 BI（原 AI）
	require.Equal(t, int32(5), e.Rows[1][0])  // 新 AI（原 BI）
}

// TestFlashbackWithRowsFilterAllFiltered 验证闪回+行过滤：所有行都被过滤掉
func TestFlashbackWithRowsFilterAllFiltered(t *testing.T) {
	table := &TableMapEvent{
		tableIDSize: 6,
		TableID:     0x6c,
		Schema:      []byte("db"),
		Table:       []byte("tbl"),
		ColumnCount: 1,
		ColumnType:  []byte{MYSQL_TYPE_LONG},
		ColumnMeta:  []uint16{0},
		NullBitmap:  []byte{0x00},
	}

	// 2 行: value=1, value=2
	rowData := []byte{
		0x00, 0x01, 0x00, 0x00, 0x00,
		0x00, 0x02, 0x00, 0x00, 0x00,
	}

	rawData := buildRowsEventRawData(0x6c, WRITE_ROWS_EVENTv2, 1, rowData)

	// 过滤条件: col[0] > 100 (都不匹配)
	rf, err := NewRowsFilter("col[0] > 100")
	require.NoError(t, err)

	e := &RowsEvent{
		Version:     2,
		tableIDSize: 6,
		tables:      map[uint64]*TableMapEvent{0x6c: table},
		eventType:   WRITE_ROWS_EVENTv2,
		flashback:   true,
		rowsFilter:  rf,
		rawBytesNew: make([]byte, len(rawData)),
	}
	copy(e.rawBytesNew, rawData)

	bodyData := rawData[EventHeaderSize:]
	pos, err := e.DecodeHeader(bodyData)
	require.NoError(t, err)

	err = e.FlashbackData2(pos, bodyData)
	require.NoError(t, err)

	// 所有行被过滤，rawBytesNew 截断到 header
	require.Equal(t, 0, e.RowsMatched)
	require.Len(t, e.Rows, 0)
	require.Equal(t, EventHeaderSize, len(e.rawBytesNew))
}

// TestFlashbackWithConvUpdateToWrite 验证闪回 + convUpdateToWrite 模式
func TestFlashbackWithConvUpdateToWrite(t *testing.T) {
	table := &TableMapEvent{
		tableIDSize: 6,
		TableID:     0x6c,
		Schema:      []byte("db"),
		Table:       []byte("tbl"),
		ColumnCount: 1,
		ColumnType:  []byte{MYSQL_TYPE_LONG},
		ColumnMeta:  []uint16{0},
		NullBitmap:  []byte{0x00},
	}

	// UPDATE: BI=10, AI=20
	rowData := []byte{
		0x00, 0x0a, 0x00, 0x00, 0x00, // BI: 10
		0x00, 0x14, 0x00, 0x00, 0x00, // AI: 20
	}

	rawData := buildRowsEventRawData(0x6c, UPDATE_ROWS_EVENTv2, 1, rowData)

	e := &RowsEvent{
		Version:           2,
		tableIDSize:       6,
		tables:            map[uint64]*TableMapEvent{0x6c: table},
		eventType:         UPDATE_ROWS_EVENTv2,
		needBitmap2:       true,
		flashback:         true,
		convUpdateToWrite: true,
		rawBytesNew:       make([]byte, len(rawData)),
	}
	copy(e.rawBytesNew, rawData)

	bodyData := rawData[EventHeaderSize:]
	pos, err := e.DecodeHeader(bodyData)
	require.NoError(t, err)

	err = e.FlashbackData2(pos, bodyData)
	require.NoError(t, err)

	// convUpdateToWrite: UPDATE 先闪回（交换类型不变），再转为 WRITE
	// 闪回逻辑先将 UPDATE 保持，然后 convUpdateToWrite 将其转为 WRITE
	require.Equal(t, WRITE_ROWS_EVENTv2, e.eventType)
	require.Equal(t, byte(WRITE_ROWS_EVENTv2), e.rawBytesNew[EventTypePos])
}

// TestFlashbackEndToEnd 使用 parser 完整流程测试闪回
func TestFlashbackEndToEnd(t *testing.T) {
	// FORMAT_DESCRIPTION_EVENT
	fdeData := []byte{0x64, 0x61, 0x72, 0x63, 0xf, 0xb, 0x0, 0x0, 0x0, 0x77, 0x0, 0x0, 0x0, 0x7b, 0x0, 0x0, 0x0, 0x1, 0x0, 0x4, 0x0, 0x35, 0x2e, 0x37, 0x2e, 0x32, 0x32, 0x2d, 0x6c, 0x6f, 0x67, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x64, 0x61, 0x72, 0x63, 0x13, 0x38, 0xd, 0x0, 0x8, 0x0, 0x12, 0x0, 0x4, 0x4, 0x4, 0x4, 0x12, 0x0, 0x0, 0x5f, 0x0, 0x4, 0x1a, 0x8, 0x0, 0x0, 0x0, 0x8, 0x8, 0x8, 0x2, 0x0, 0x0, 0x0, 0xa, 0xa, 0xa, 0x2a, 0x2a, 0x0, 0x12, 0x34, 0x0, 0x1, 0xb8, 0x78, 0x9d, 0xfe}
	// TABLE MAP EVENT tb(INT)
	tmeData := []byte{0x8d, 0x61, 0x72, 0x63, 0x13, 0xb, 0x0, 0x0, 0x0, 0x2c, 0x0, 0x0, 0x0, 0xa7, 0x0, 0x0, 0x0, 0x1, 0x0, 0x6c, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x2, 0x64, 0x62, 0x0, 0x3, 0x74, 0x62, 0x6c, 0x0, 0x1, 0x3, 0x0, 0x0, 0x63, 0x17, 0xe6, 0xf0}
	// WRITE_ROWS_EVENTv2 rows INT(1)
	rowsData := []byte{0xb6, 0x61, 0x72, 0x63, 0x1e, 0xb, 0x0, 0x0, 0x0, 0x28, 0x0, 0x0, 0x0, 0xcf, 0x0, 0x0, 0x0, 0x1, 0x0, 0x6c, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x2, 0x0, 0x1, 0xff, 0x0, 0x1, 0x0, 0x0, 0x0, 0xf9, 0xf7, 0x89, 0x2a}

	var binlogStream bytes.Buffer
	binlogStream.Write(fdeData)
	binlogStream.Write(tmeData)
	binlogStream.Write(rowsData)

	parser := NewBinlogParser()
	parser.Flashback = true
	parser.PrintEventInfo = &PrintEventInfo{}

	var events []*BinlogEvent
	err := parser.ParseReader(&binlogStream, func(e *BinlogEvent) error {
		events = append(events, e)
		return nil
	})
	require.NoError(t, err)

	// 找到 RowsEvent
	var rowsEvent *BinlogEvent
	for _, ev := range events {
		if isRowsEvent(ev.Header.EventType) {
			rowsEvent = ev
			break
		}
	}
	require.NotNil(t, rowsEvent)

	// 验证 WRITE 变为 DELETE
	require.Equal(t, DELETE_ROWS_EVENTv2, rowsEvent.Header.EventType)

	// 验证行数据
	re, ok := rowsEvent.Event.(*RowsEvent)
	require.True(t, ok)
	require.Len(t, re.Rows, 1)
	require.Equal(t, int32(1), re.Rows[0][0])
}

// TestFlashbackEndToEndWithFilter 使用 parser 完整流程测试闪回+行过滤
func TestFlashbackEndToEndWithFilter(t *testing.T) {
	// FORMAT_DESCRIPTION_EVENT (checksum=CRC32)
	fdeData := []byte{0x64, 0x61, 0x72, 0x63, 0xf, 0xb, 0x0, 0x0, 0x0, 0x77, 0x0, 0x0, 0x0, 0x7b, 0x0, 0x0, 0x0, 0x1, 0x0, 0x4, 0x0, 0x35, 0x2e, 0x37, 0x2e, 0x32, 0x32, 0x2d, 0x6c, 0x6f, 0x67, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x64, 0x61, 0x72, 0x63, 0x13, 0x38, 0xd, 0x0, 0x8, 0x0, 0x12, 0x0, 0x4, 0x4, 0x4, 0x4, 0x12, 0x0, 0x0, 0x5f, 0x0, 0x4, 0x1a, 0x8, 0x0, 0x0, 0x0, 0x8, 0x8, 0x8, 0x2, 0x0, 0x0, 0x0, 0xa, 0xa, 0xa, 0x2a, 0x2a, 0x0, 0x12, 0x34, 0x0, 0x1, 0xb8, 0x78, 0x9d, 0xfe}
	// TABLE MAP EVENT tb(INT)
	tmeData := []byte{0x8d, 0x61, 0x72, 0x63, 0x13, 0xb, 0x0, 0x0, 0x0, 0x2c, 0x0, 0x0, 0x0, 0xa7, 0x0, 0x0, 0x0, 0x1, 0x0, 0x6c, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x2, 0x64, 0x62, 0x0, 0x3, 0x74, 0x62, 0x6c, 0x0, 0x1, 0x3, 0x0, 0x0, 0x63, 0x17, 0xe6, 0xf0}
	// WRITE_ROWS_EVENTv2 rows INT(1)
	rowsData := []byte{0xb6, 0x61, 0x72, 0x63, 0x1e, 0xb, 0x0, 0x0, 0x0, 0x28, 0x0, 0x0, 0x0, 0xcf, 0x0, 0x0, 0x0, 0x1, 0x0, 0x6c, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x2, 0x00, 0x1, 0xff, 0x0, 0x01, 0x0, 0x0, 0x0, 0xf9, 0xf7, 0x89, 0x2a}

	var binlogStream bytes.Buffer
	binlogStream.Write(fdeData)
	binlogStream.Write(tmeData)
	binlogStream.Write(rowsData)

	parser := NewBinlogParser()
	parser.Flashback = true
	parser.PrintEventInfo = &PrintEventInfo{}

	// 设置行过滤: col[0] == 1
	rf, err := NewRowsFilter("col[0] == 1")
	require.NoError(t, err)
	parser.RowsFilter = rf

	var events []*BinlogEvent
	err = parser.ParseReader(&binlogStream, func(e *BinlogEvent) error {
		events = append(events, e)
		return nil
	})
	require.NoError(t, err)

	// 找到 RowsEvent
	var rowsEvent *BinlogEvent
	for _, ev := range events {
		if isRowsEvent(ev.Header.EventType) {
			rowsEvent = ev
			break
		}
	}
	require.NotNil(t, rowsEvent)

	// 验证闪回: WRITE → DELETE
	require.Equal(t, DELETE_ROWS_EVENTv2, rowsEvent.Header.EventType)

	// 验证行过滤: col[0] == 1 匹配
	re, ok := rowsEvent.Event.(*RowsEvent)
	require.True(t, ok)
	require.Equal(t, 1, re.RowsMatched)
}

// TestDecodeData2WithRowsFilter 验证非闪回模式下的行过滤（DecodeData2）
func TestDecodeData2WithRowsFilter(t *testing.T) {
	table := &TableMapEvent{
		tableIDSize: 6,
		TableID:     0x6c,
		Schema:      []byte("db"),
		Table:       []byte("tbl"),
		ColumnCount: 1,
		ColumnType:  []byte{MYSQL_TYPE_LONG},
		ColumnMeta:  []uint16{0},
		NullBitmap:  []byte{0x00},
	}

	// 3 行: value=1, value=5, value=10
	rowData := []byte{
		0x00, 0x01, 0x00, 0x00, 0x00,
		0x00, 0x05, 0x00, 0x00, 0x00,
		0x00, 0x0a, 0x00, 0x00, 0x00,
	}

	rawData := buildRowsEventRawData(0x6c, WRITE_ROWS_EVENTv2, 1, rowData)

	// 过滤条件: col[0] == 5
	rf, err := NewRowsFilter("col[0] == 5")
	require.NoError(t, err)

	e := &RowsEvent{
		Version:     2,
		tableIDSize: 6,
		tables:      map[uint64]*TableMapEvent{0x6c: table},
		eventType:   WRITE_ROWS_EVENTv2,
		flashback:   false, // 非闪回模式
		rowsFilter:  rf,
		rawBytesNew: make([]byte, len(rawData)),
	}
	copy(e.rawBytesNew, rawData)

	bodyData := rawData[EventHeaderSize:]
	pos, err := e.DecodeHeader(bodyData)
	require.NoError(t, err)

	// 调用 DecodeData2（非闪回路径）
	err = e.DecodeData2(pos, bodyData)
	require.NoError(t, err)

	// 只有 value=5 匹配
	require.Equal(t, 1, e.RowsMatched)
	require.Len(t, e.Rows, 1)
	require.Equal(t, int32(5), e.Rows[0][0])
}

// TestDecodeData2UpdateWithRowsFilter 验证非闪回模式下 UPDATE 事件的行过滤
func TestDecodeData2UpdateWithRowsFilter(t *testing.T) {
	table := &TableMapEvent{
		tableIDSize: 6,
		TableID:     0x6c,
		Schema:      []byte("db"),
		Table:       []byte("tbl"),
		ColumnCount: 1,
		ColumnType:  []byte{MYSQL_TYPE_LONG},
		ColumnMeta:  []uint16{0},
		NullBitmap:  []byte{0x00},
	}

	// 2 行 UPDATE: row1 BI=1 AI=11, row2 BI=5 AI=55
	rowData := []byte{
		0x00, 0x01, 0x00, 0x00, 0x00, // row1 BI: 1
		0x00, 0x0b, 0x00, 0x00, 0x00, // row1 AI: 11
		0x00, 0x05, 0x00, 0x00, 0x00, // row2 BI: 5
		0x00, 0x37, 0x00, 0x00, 0x00, // row2 AI: 55
	}

	rawData := buildRowsEventRawData(0x6c, UPDATE_ROWS_EVENTv2, 1, rowData)

	// 过滤条件: col[0] >= 5 (基于 BI 过滤)
	rf, err := NewRowsFilter("col[0] >= 5")
	require.NoError(t, err)

	e := &RowsEvent{
		Version:     2,
		tableIDSize: 6,
		tables:      map[uint64]*TableMapEvent{0x6c: table},
		eventType:   UPDATE_ROWS_EVENTv2,
		needBitmap2: true,
		flashback:   false,
		rowsFilter:  rf,
		rawBytesNew: make([]byte, len(rawData)),
	}
	copy(e.rawBytesNew, rawData)

	bodyData := rawData[EventHeaderSize:]
	pos, err := e.DecodeHeader(bodyData)
	require.NoError(t, err)

	err = e.DecodeData2(pos, bodyData)
	require.NoError(t, err)

	// 只有 row2 (BI=5) 满足过滤条件，保留 BI 和 AI
	require.Equal(t, 1, e.RowsMatched)
	require.Len(t, e.Rows, 2) // BI + AI
	require.Equal(t, int32(5), e.Rows[0][0])  // BI
	require.Equal(t, int32(55), e.Rows[1][0]) // AI
}

// TestDbTableFilterMatch 验证库表过滤：匹配的表应该被闪回处理
func TestDbTableFilterMatch(t *testing.T) {
	table := &TableMapEvent{
		tableIDSize: 6,
		TableID:     0x6c,
		Schema:      []byte("testdb"),
		Table:       []byte("orders"),
		ColumnCount: 1,
		ColumnType:  []byte{MYSQL_TYPE_LONG},
		ColumnMeta:  []uint16{0},
		NullBitmap:  []byte{0x00},
	}

	rowData := []byte{
		0x00, 0x01, 0x00, 0x00, 0x00, // value=1
	}

	rawData := buildRowsEventRawData(0x6c, WRITE_ROWS_EVENTv2, 1, rowData)

	// 创建库表过滤: include testdb.orders
	filter, err := db_table_filter.NewFilter(
		[]string{"testdb"}, []string{"orders"},
		[]string{}, []string{},
	)
	require.NoError(t, err)
	err = filter.DbTableFilterCompile()
	require.NoError(t, err)

	e := &RowsEvent{
		Version:       2,
		tableIDSize:   6,
		tables:        map[uint64]*TableMapEvent{0x6c: table},
		eventType:     WRITE_ROWS_EVENTv2,
		flashback:     true,
		dbTableFilter: filter,
		rawBytesNew:   make([]byte, len(rawData)),
	}
	copy(e.rawBytesNew, rawData)

	bodyData := rawData[EventHeaderSize:]
	err = flashbackRowsEventFunc(e, bodyData)
	require.NoError(t, err)

	// 匹配的表应该被闪回处理: WRITE → DELETE
	require.Equal(t, DELETE_ROWS_EVENTv2, e.eventType)
	require.Len(t, e.Rows, 1)
	require.Equal(t, int32(1), e.Rows[0][0])
}

// TestDbTableFilterNotMatch 验证库表过滤：不匹配的表应该被跳过
func TestDbTableFilterNotMatch(t *testing.T) {
	table := &TableMapEvent{
		tableIDSize: 6,
		TableID:     0x6c,
		Schema:      []byte("otherdb"),
		Table:       []byte("users"),
		ColumnCount: 1,
		ColumnType:  []byte{MYSQL_TYPE_LONG},
		ColumnMeta:  []uint16{0},
		NullBitmap:  []byte{0x00},
	}

	rowData := []byte{
		0x00, 0x01, 0x00, 0x00, 0x00,
	}

	rawData := buildRowsEventRawData(0x6c, WRITE_ROWS_EVENTv2, 1, rowData)

	// 创建库表过滤: include testdb.orders，不包含 otherdb.users
	filter, err := db_table_filter.NewFilter(
		[]string{"testdb"}, []string{"orders"},
		[]string{}, []string{},
	)
	require.NoError(t, err)
	err = filter.DbTableFilterCompile()
	require.NoError(t, err)

	e := &RowsEvent{
		Version:       2,
		tableIDSize:   6,
		tables:        map[uint64]*TableMapEvent{0x6c: table},
		eventType:     WRITE_ROWS_EVENTv2,
		flashback:     true,
		dbTableFilter: filter,
		rawBytesNew:   make([]byte, len(rawData)),
	}
	copy(e.rawBytesNew, rawData)

	bodyData := rawData[EventHeaderSize:]
	err = flashbackRowsEventFunc(e, bodyData)
	require.NoError(t, err)

	// 不匹配的表应该被跳过: rawBytesNew 被截断为 EventHeaderSize
	require.Equal(t, EventHeaderSize, len(e.rawBytesNew))
	// 事件类型不变（未被闪回处理）
	require.Equal(t, WRITE_ROWS_EVENTv2, e.eventType)
}

// TestDbTableFilterWildcard 验证库表过滤：通配符匹配
func TestDbTableFilterWildcard(t *testing.T) {
	table := &TableMapEvent{
		tableIDSize: 6,
		TableID:     0x6c,
		Schema:      []byte("testdb"),
		Table:       []byte("order_2024"),
		ColumnCount: 1,
		ColumnType:  []byte{MYSQL_TYPE_LONG},
		ColumnMeta:  []uint16{0},
		NullBitmap:  []byte{0x00},
	}

	rowData := []byte{
		0x00, 0x05, 0x00, 0x00, 0x00,
	}

	rawData := buildRowsEventRawData(0x6c, DELETE_ROWS_EVENTv2, 1, rowData)

	// 创建库表过滤: include testdb.order%（通配符匹配 order_2024）
	filter, err := db_table_filter.NewFilter(
		[]string{"testdb"}, []string{"order%"},
		[]string{}, []string{},
	)
	require.NoError(t, err)
	err = filter.DbTableFilterCompile()
	require.NoError(t, err)

	e := &RowsEvent{
		Version:       2,
		tableIDSize:   6,
		tables:        map[uint64]*TableMapEvent{0x6c: table},
		eventType:     DELETE_ROWS_EVENTv2,
		flashback:     true,
		dbTableFilter: filter,
		rawBytesNew:   make([]byte, len(rawData)),
	}
	copy(e.rawBytesNew, rawData)

	bodyData := rawData[EventHeaderSize:]
	err = flashbackRowsEventFunc(e, bodyData)
	require.NoError(t, err)

	// 通配符匹配成功: DELETE → WRITE
	require.Equal(t, WRITE_ROWS_EVENTv2, e.eventType)
	require.Len(t, e.Rows, 1)
	require.Equal(t, int32(5), e.Rows[0][0])
}

// TestDbTableFilterExclude 验证库表过滤：排除规则
func TestDbTableFilterExclude(t *testing.T) {
	table := &TableMapEvent{
		tableIDSize: 6,
		TableID:     0x6c,
		Schema:      []byte("testdb"),
		Table:       []byte("audit_log"),
		ColumnCount: 1,
		ColumnType:  []byte{MYSQL_TYPE_LONG},
		ColumnMeta:  []uint16{0},
		NullBitmap:  []byte{0x00},
	}

	rowData := []byte{
		0x00, 0x01, 0x00, 0x00, 0x00,
	}

	rawData := buildRowsEventRawData(0x6c, WRITE_ROWS_EVENTv2, 1, rowData)

	// 创建库表过滤: include testdb.*，但排除 audit_log
	filter, err := db_table_filter.NewFilter(
		[]string{"testdb"}, []string{"*"},
		[]string{}, []string{"audit_log"},
	)
	require.NoError(t, err)
	err = filter.DbTableFilterCompile()
	require.NoError(t, err)

	e := &RowsEvent{
		Version:       2,
		tableIDSize:   6,
		tables:        map[uint64]*TableMapEvent{0x6c: table},
		eventType:     WRITE_ROWS_EVENTv2,
		flashback:     true,
		dbTableFilter: filter,
		rawBytesNew:   make([]byte, len(rawData)),
	}
	copy(e.rawBytesNew, rawData)

	bodyData := rawData[EventHeaderSize:]
	err = flashbackRowsEventFunc(e, bodyData)
	require.NoError(t, err)

	// audit_log 被排除，不应该被闪回处理
	require.Equal(t, EventHeaderSize, len(e.rawBytesNew))
	require.Equal(t, WRITE_ROWS_EVENTv2, e.eventType)
}

// TestDbTableFilterExcludeDb 验证库表过滤：排除整个库
func TestDbTableFilterExcludeDb(t *testing.T) {
	table := &TableMapEvent{
		tableIDSize: 6,
		TableID:     0x6c,
		Schema:      []byte("infodba_schema"),
		Table:       []byte("some_table"),
		ColumnCount: 1,
		ColumnType:  []byte{MYSQL_TYPE_LONG},
		ColumnMeta:  []uint16{0},
		NullBitmap:  []byte{0x00},
	}

	rowData := []byte{
		0x00, 0x01, 0x00, 0x00, 0x00,
	}

	rawData := buildRowsEventRawData(0x6c, WRITE_ROWS_EVENTv2, 1, rowData)

	// 创建库表过滤: include *.*，但排除 infodba_schema 库
	filter, err := db_table_filter.NewFilter(
		[]string{"*"}, []string{"*"},
		[]string{"infodba_schema"}, []string{},
	)
	require.NoError(t, err)
	err = filter.DbTableFilterCompile()
	require.NoError(t, err)

	e := &RowsEvent{
		Version:       2,
		tableIDSize:   6,
		tables:        map[uint64]*TableMapEvent{0x6c: table},
		eventType:     WRITE_ROWS_EVENTv2,
		flashback:     true,
		dbTableFilter: filter,
		rawBytesNew:   make([]byte, len(rawData)),
	}
	copy(e.rawBytesNew, rawData)

	bodyData := rawData[EventHeaderSize:]
	err = flashbackRowsEventFunc(e, bodyData)
	require.NoError(t, err)

	// infodba_schema 库被排除
	require.Equal(t, EventHeaderSize, len(e.rawBytesNew))
	require.Equal(t, WRITE_ROWS_EVENTv2, e.eventType)
}

// TestRenameRulePlain 验证 RenameRule plain 格式 (aa->bb)
func TestRenameRulePlain(t *testing.T) {
	rules, err := pkg.NewRenameRule([]string{"olddb->newdb"})
	require.NoError(t, err)

	require.Equal(t, "newdb", rules.GetNewName("olddb"))
	require.Equal(t, "other", rules.GetNewName("other")) // 不匹配的保持不变
}

// TestRenameRuleCurly 验证 RenameRule curly 格式 ({,} 匹配)
func TestRenameRuleCurly(t *testing.T) {
	// rename abc{,.bak} >> abc -> abc.bak
	rules, err := pkg.NewRenameRule([]string{"abc{,.bak}"})
	require.NoError(t, err)

	require.Equal(t, "abc.bak", rules.GetNewName("abc"))
	require.Equal(t, "other", rules.GetNewName("other"))
}

// TestRenameRuleCurlyWithPrefix 验证 RenameRule curly 格式带前后缀
func TestRenameRuleCurlyWithPrefix(t *testing.T) {
	// rename hhh{abc,abc_bak}_0 >> hhhabc_0 -> hhhabc_bak_0
	rules, err := pkg.NewRenameRule([]string{"hhh{abc,abc_bak}_0"})
	require.NoError(t, err)

	require.Equal(t, "hhhabc_bak_0", rules.GetNewName("hhhabc_0"))
	require.Equal(t, "other", rules.GetNewName("other"))
}

// TestRenameRuleMultiple 验证多条 RenameRule 混合使用
func TestRenameRuleMultiple(t *testing.T) {
	rules, err := pkg.NewRenameRule([]string{
		"db1->db1_bak",
		"db2{,.bak}",
	})
	require.NoError(t, err)

	require.Equal(t, "db1_bak", rules.GetNewName("db1"))
	require.Equal(t, "db2.bak", rules.GetNewName("db2"))
	require.Equal(t, "db3", rules.GetNewName("db3")) // 不匹配的保持不变
}

// TestRenameRuleInvalid 验证无效的 RenameRule
func TestRenameRuleInvalid(t *testing.T) {
	// 空的 oldName
	_, err := pkg.NewRenameRule([]string{"->newdb"})
	require.Error(t, err)

	// 无效格式
	_, err = pkg.NewRenameRule([]string{"invalid_rule_no_arrow"})
	require.Error(t, err)

	// 空规则应该被跳过
	rules, err := pkg.NewRenameRule([]string{""})
	require.NoError(t, err)
	require.Equal(t, "anything", rules.GetNewName("anything"))
}

// TestRenameRuleMustMatch 验证 GetNewNameMustMatch 方法
func TestRenameRuleMustMatch(t *testing.T) {
	rules, err := pkg.NewRenameRule([]string{"olddb->newdb"})
	require.NoError(t, err)

	// 匹配的
	newName, err := rules.GetNewNameMustMatch("olddb")
	require.NoError(t, err)
	require.Equal(t, "newdb", newName)

	// 不匹配的应该报错
	_, err = rules.GetNewNameMustMatch("unknown")
	require.Error(t, err)
}

// TestDecodeAndRename 验证 TableMapEvent 的 DecodeAndRename 功能
func TestDecodeAndRename(t *testing.T) {
	rules, err := pkg.NewRenameRule([]string{"testdb->newdb"})
	require.NoError(t, err)

	// 构建 TableMapEvent 的 body data
	// table_id (6 bytes) + flags (2 bytes) + schema_length (1 byte) + schema + 0x00 + table_length (1 byte) + table + 0x00 + column_count + column_types + meta_length + null_bitmap
	schema := []byte("testdb")
	tableName := []byte("orders")

	var body []byte
	// table_id: 0x6c (6 bytes little-endian)
	body = append(body, 0x6c, 0x00, 0x00, 0x00, 0x00, 0x00)
	// flags (2 bytes)
	body = append(body, 0x01, 0x00)
	// schema_length
	body = append(body, byte(len(schema)))
	// schema
	body = append(body, schema...)
	// 0x00 separator
	body = append(body, 0x00)
	// table_length
	body = append(body, byte(len(tableName)))
	// table
	body = append(body, tableName...)
	// 0x00 separator
	body = append(body, 0x00)
	// column_count (length-encoded int): 1 column
	body = append(body, 0x01)
	// column_type: MYSQL_TYPE_LONG
	body = append(body, MYSQL_TYPE_LONG)
	// meta_length (length-encoded string): 0 bytes meta
	body = append(body, 0x00)
	// null_bitmap: 1 byte for 1 column
	body = append(body, 0x00)

	// 构建完整的 rawData (header + body)
	header := make([]byte, EventHeaderSize)
	binary.LittleEndian.PutUint32(header[TimestampPos:], 1000000)
	header[EventTypePos] = byte(TABLE_MAP_EVENT)
	binary.LittleEndian.PutUint32(header[EventSizPos:], uint32(EventHeaderSize+len(body)))

	rawData := append(header, body...)

	te := &TableMapEvent{
		tableIDSize: 6,
		rawBytesNew: make([]byte, len(rawData)),
	}
	copy(te.rawBytesNew, rawData)
	te.SetRenameRule(rules)

	err = te.DecodeAndRename(body)
	require.NoError(t, err)

	// 验证 schema 被重命名
	require.Equal(t, []byte("newdb"), te.NewSchema)
	require.Equal(t, []byte("testdb"), te.Schema) // 原始 schema 不变
	require.Equal(t, []byte("orders"), te.Table)
	require.Equal(t, uint64(0x6c), te.TableID)
	require.Equal(t, uint64(1), te.ColumnCount)

	// 验证 rawBytesNew 中包含新的 schema
	// rawBytesNew 应该包含 "newdb" 而不是 "testdb"
	require.True(t, bytes.Contains(te.rawBytesNew, []byte("newdb")))
}

// TestDecodeAndRenameWithCurlyRule 验证 curly 格式的 rename 在 TableMapEvent 中的应用
func TestDecodeAndRenameWithCurlyRule(t *testing.T) {
	rules, err := pkg.NewRenameRule([]string{"test{db,db_bak}"})
	require.NoError(t, err)

	schema := []byte("testdb")
	tableName := []byte("users")

	var body []byte
	body = append(body, 0x6c, 0x00, 0x00, 0x00, 0x00, 0x00) // table_id
	body = append(body, 0x01, 0x00)                           // flags
	body = append(body, byte(len(schema)))                    // schema_length
	body = append(body, schema...)                            // schema
	body = append(body, 0x00)                                 // separator
	body = append(body, byte(len(tableName)))                 // table_length
	body = append(body, tableName...)                         // table
	body = append(body, 0x00)                                 // separator
	body = append(body, 0x01)                                 // column_count
	body = append(body, MYSQL_TYPE_LONG)                      // column_type
	body = append(body, 0x00)                                 // meta_length
	body = append(body, 0x00)                                 // null_bitmap

	header := make([]byte, EventHeaderSize)
	binary.LittleEndian.PutUint32(header[TimestampPos:], 1000000)
	header[EventTypePos] = byte(TABLE_MAP_EVENT)
	binary.LittleEndian.PutUint32(header[EventSizPos:], uint32(EventHeaderSize+len(body)))

	rawData := append(header, body...)

	te := &TableMapEvent{
		tableIDSize: 6,
		rawBytesNew: make([]byte, len(rawData)),
	}
	copy(te.rawBytesNew, rawData)
	te.SetRenameRule(rules)

	err = te.DecodeAndRename(body)
	require.NoError(t, err)

	// testdb -> testdb_bak
	require.Equal(t, []byte("testdb_bak"), te.NewSchema)
	require.Equal(t, []byte("testdb"), te.Schema)
	require.Equal(t, []byte("users"), te.Table)
	require.True(t, bytes.Contains(te.rawBytesNew, []byte("testdb_bak")))
}

// ============================================================
// 使用真实 binlog rows_event 数据的 DbTableFilter 测试
// ============================================================

// getRealBinlogData 返回真实的 binlog 事件数据
// FORMAT_DESCRIPTION_EVENT + TABLE_MAP_EVENT(db.tbl, INT) + WRITE_ROWS_EVENTv2(value=1)
func getRealBinlogData() (fdeData, tmeData, rowsData []byte) {
	// FORMAT_DESCRIPTION_EVENT (MySQL 5.7.22-log, checksum=CRC32)
	fdeData = []byte{0x64, 0x61, 0x72, 0x63, 0xf, 0xb, 0x0, 0x0, 0x0, 0x77, 0x0, 0x0, 0x0, 0x7b, 0x0, 0x0, 0x0, 0x1, 0x0, 0x4, 0x0, 0x35, 0x2e, 0x37, 0x2e, 0x32, 0x32, 0x2d, 0x6c, 0x6f, 0x67, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x64, 0x61, 0x72, 0x63, 0x13, 0x38, 0xd, 0x0, 0x8, 0x0, 0x12, 0x0, 0x4, 0x4, 0x4, 0x4, 0x12, 0x0, 0x0, 0x5f, 0x0, 0x4, 0x1a, 0x8, 0x0, 0x0, 0x0, 0x8, 0x8, 0x8, 0x2, 0x0, 0x0, 0x0, 0xa, 0xa, 0xa, 0x2a, 0x2a, 0x0, 0x12, 0x34, 0x0, 0x1, 0xb8, 0x78, 0x9d, 0xfe}
	// TABLE_MAP_EVENT: schema="db", table="tbl", 1 column(INT), table_id=0x6c
	tmeData = []byte{0x8d, 0x61, 0x72, 0x63, 0x13, 0xb, 0x0, 0x0, 0x0, 0x2c, 0x0, 0x0, 0x0, 0xa7, 0x0, 0x0, 0x0, 0x1, 0x0, 0x6c, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x2, 0x64, 0x62, 0x0, 0x3, 0x74, 0x62, 0x6c, 0x0, 0x1, 0x3, 0x0, 0x0, 0x63, 0x17, 0xe6, 0xf0}
	// WRITE_ROWS_EVENTv2: table_id=0x6c, 1 row, INT value=1
	rowsData = []byte{0xb6, 0x61, 0x72, 0x63, 0x1e, 0xb, 0x0, 0x0, 0x0, 0x28, 0x0, 0x0, 0x0, 0xcf, 0x0, 0x0, 0x0, 0x1, 0x0, 0x6c, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x2, 0x00, 0x1, 0xff, 0x0, 0x01, 0x0, 0x0, 0x0, 0xf9, 0xf7, 0x89, 0x2a}
	return
}

// TestDbTableFilterRealBinlogMatch 使用真实 binlog 数据验证库表过滤：匹配的表被闪回处理
func TestDbTableFilterRealBinlogMatch(t *testing.T) {
	fdeData, tmeData, rowsData := getRealBinlogData()

	var binlogStream bytes.Buffer
	binlogStream.Write(fdeData)
	binlogStream.Write(tmeData)
	binlogStream.Write(rowsData)

	parser := NewBinlogParser()
	parser.Flashback = true
	parser.PrintEventInfo = &PrintEventInfo{}

	// 设置库表过滤: include db.tbl（精确匹配真实 binlog 中的库表）
	filter, err := db_table_filter.NewFilter(
		[]string{"db"}, []string{"tbl"},
		[]string{}, []string{},
	)
	require.NoError(t, err)
	err = filter.DbTableFilterCompile()
	require.NoError(t, err)
	parser.TableFilter = filter

	var events []*BinlogEvent
	err = parser.ParseReader(&binlogStream, func(e *BinlogEvent) error {
		events = append(events, e)
		return nil
	})
	require.NoError(t, err)

	// 找到 RowsEvent
	var rowsEvent *BinlogEvent
	for _, ev := range events {
		if isRowsEvent(ev.Header.EventType) {
			rowsEvent = ev
			break
		}
	}
	require.NotNil(t, rowsEvent, "应该找到 RowsEvent，因为 db.tbl 匹配过滤条件")

	// 验证闪回: WRITE → DELETE
	require.Equal(t, DELETE_ROWS_EVENTv2, rowsEvent.Header.EventType)

	// 验证行数据正确解析
	re, ok := rowsEvent.Event.(*RowsEvent)
	require.True(t, ok)
	require.Len(t, re.Rows, 1)
	require.Equal(t, int32(1), re.Rows[0][0])
}

// TestDbTableFilterRealBinlogNotMatch 使用真实 binlog 数据验证库表过滤：不匹配的表被跳过
func TestDbTableFilterRealBinlogNotMatch(t *testing.T) {
	fdeData, tmeData, rowsData := getRealBinlogData()

	var binlogStream bytes.Buffer
	binlogStream.Write(fdeData)
	binlogStream.Write(tmeData)
	binlogStream.Write(rowsData)

	parser := NewBinlogParser()
	parser.Flashback = true
	parser.PrintEventInfo = &PrintEventInfo{}

	// 设置库表过滤: include otherdb.other_table（不匹配真实 binlog 中的 db.tbl）
	filter, err := db_table_filter.NewFilter(
		[]string{"otherdb"}, []string{"other_table"},
		[]string{}, []string{},
	)
	require.NoError(t, err)
	err = filter.DbTableFilterCompile()
	require.NoError(t, err)
	parser.TableFilter = filter

	var events []*BinlogEvent
	err = parser.ParseReader(&binlogStream, func(e *BinlogEvent) error {
		events = append(events, e)
		return nil
	})
	require.NoError(t, err)

	// 找到 RowsEvent
	var rowsEvent *BinlogEvent
	for _, ev := range events {
		if isRowsEvent(ev.Header.EventType) {
			rowsEvent = ev
			break
		}
	}
	require.NotNil(t, rowsEvent)

	// 不匹配的表: RowsEvent 的 rawBytesNew 应该被截断（只有 header 大小）
	re, ok := rowsEvent.Event.(*RowsEvent)
	require.True(t, ok)
	require.Equal(t, EventHeaderSize, len(re.rawBytesNew))
}

// TestDbTableFilterRealBinlogWildcardMatch 使用真实 binlog 数据验证通配符匹配
func TestDbTableFilterRealBinlogWildcardMatch(t *testing.T) {
	fdeData, tmeData, rowsData := getRealBinlogData()

	var binlogStream bytes.Buffer
	binlogStream.Write(fdeData)
	binlogStream.Write(tmeData)
	binlogStream.Write(rowsData)

	parser := NewBinlogParser()
	parser.Flashback = true
	parser.PrintEventInfo = &PrintEventInfo{}

	// 设置库表过滤: include db.tb%（通配符匹配 tbl）
	filter, err := db_table_filter.NewFilter(
		[]string{"db"}, []string{"tb%"},
		[]string{}, []string{},
	)
	require.NoError(t, err)
	err = filter.DbTableFilterCompile()
	require.NoError(t, err)
	parser.TableFilter = filter

	var events []*BinlogEvent
	err = parser.ParseReader(&binlogStream, func(e *BinlogEvent) error {
		events = append(events, e)
		return nil
	})
	require.NoError(t, err)

	// 找到 RowsEvent
	var rowsEvent *BinlogEvent
	for _, ev := range events {
		if isRowsEvent(ev.Header.EventType) {
			rowsEvent = ev
			break
		}
	}
	require.NotNil(t, rowsEvent)

	// 通配符匹配成功: WRITE → DELETE
	require.Equal(t, DELETE_ROWS_EVENTv2, rowsEvent.Header.EventType)

	re, ok := rowsEvent.Event.(*RowsEvent)
	require.True(t, ok)
	require.Len(t, re.Rows, 1)
	require.Equal(t, int32(1), re.Rows[0][0])
}

// TestDbTableFilterRealBinlogExcludeTable 使用真实 binlog 数据验证排除表
func TestDbTableFilterRealBinlogExcludeTable(t *testing.T) {
	fdeData, tmeData, rowsData := getRealBinlogData()

	var binlogStream bytes.Buffer
	binlogStream.Write(fdeData)
	binlogStream.Write(tmeData)
	binlogStream.Write(rowsData)

	parser := NewBinlogParser()
	parser.Flashback = true
	parser.PrintEventInfo = &PrintEventInfo{}

	// 设置库表过滤: include db.*，但排除 tbl
	filter, err := db_table_filter.NewFilter(
		[]string{"db"}, []string{"*"},
		[]string{}, []string{"tbl"},
	)
	require.NoError(t, err)
	err = filter.DbTableFilterCompile()
	require.NoError(t, err)
	parser.TableFilter = filter

	var events []*BinlogEvent
	err = parser.ParseReader(&binlogStream, func(e *BinlogEvent) error {
		events = append(events, e)
		return nil
	})
	require.NoError(t, err)

	// 找到 RowsEvent
	var rowsEvent *BinlogEvent
	for _, ev := range events {
		if isRowsEvent(ev.Header.EventType) {
			rowsEvent = ev
			break
		}
	}
	require.NotNil(t, rowsEvent)

	// tbl 被排除: rawBytesNew 应该被截断
	re, ok := rowsEvent.Event.(*RowsEvent)
	require.True(t, ok)
	require.Equal(t, EventHeaderSize, len(re.rawBytesNew))
}

// TestDbTableFilterRealBinlogExcludeDb 使用真实 binlog 数据验证排除整个库
func TestDbTableFilterRealBinlogExcludeDb(t *testing.T) {
	fdeData, tmeData, rowsData := getRealBinlogData()

	var binlogStream bytes.Buffer
	binlogStream.Write(fdeData)
	binlogStream.Write(tmeData)
	binlogStream.Write(rowsData)

	parser := NewBinlogParser()
	parser.Flashback = true
	parser.PrintEventInfo = &PrintEventInfo{}

	// 设置库表过滤: include *.*，但排除 db 库
	filter, err := db_table_filter.NewFilter(
		[]string{"*"}, []string{"*"},
		[]string{"db"}, []string{},
	)
	require.NoError(t, err)
	err = filter.DbTableFilterCompile()
	require.NoError(t, err)
	parser.TableFilter = filter

	var events []*BinlogEvent
	err = parser.ParseReader(&binlogStream, func(e *BinlogEvent) error {
		events = append(events, e)
		return nil
	})
	require.NoError(t, err)

	// 找到 RowsEvent
	var rowsEvent *BinlogEvent
	for _, ev := range events {
		if isRowsEvent(ev.Header.EventType) {
			rowsEvent = ev
			break
		}
	}
	require.NotNil(t, rowsEvent)

	// db 库被排除: rawBytesNew 应该被截断
	re, ok := rowsEvent.Event.(*RowsEvent)
	require.True(t, ok)
	require.Equal(t, EventHeaderSize, len(re.rawBytesNew))
}

// TestDbTableFilterRealBinlogWithRowsFilter 使用真实 binlog 数据验证库表过滤 + 行过滤组合
func TestDbTableFilterRealBinlogWithRowsFilter(t *testing.T) {
	fdeData, tmeData, rowsData := getRealBinlogData()

	var binlogStream bytes.Buffer
	binlogStream.Write(fdeData)
	binlogStream.Write(tmeData)
	binlogStream.Write(rowsData)

	parser := NewBinlogParser()
	parser.Flashback = true
	parser.PrintEventInfo = &PrintEventInfo{}

	// 设置库表过滤: include db.tbl
	filter, err := db_table_filter.NewFilter(
		[]string{"db"}, []string{"tbl"},
		[]string{}, []string{},
	)
	require.NoError(t, err)
	err = filter.DbTableFilterCompile()
	require.NoError(t, err)
	parser.TableFilter = filter

	// 设置行过滤: col[0] == 1
	rf, err := NewRowsFilter("col[0] == 1")
	require.NoError(t, err)
	parser.RowsFilter = rf

	var events []*BinlogEvent
	err = parser.ParseReader(&binlogStream, func(e *BinlogEvent) error {
		events = append(events, e)
		return nil
	})
	require.NoError(t, err)

	// 找到 RowsEvent
	var rowsEvent *BinlogEvent
	for _, ev := range events {
		if isRowsEvent(ev.Header.EventType) {
			rowsEvent = ev
			break
		}
	}
	require.NotNil(t, rowsEvent)

	// 库表匹配 + 行过滤匹配: WRITE → DELETE
	require.Equal(t, DELETE_ROWS_EVENTv2, rowsEvent.Header.EventType)

	re, ok := rowsEvent.Event.(*RowsEvent)
	require.True(t, ok)
	require.Equal(t, 1, re.RowsMatched)
	require.Len(t, re.Rows, 1)
	require.Equal(t, int32(1), re.Rows[0][0])
}

// TestDbTableFilterRealBinlogMatchButRowsFilterNotMatch 使用真实 binlog 数据验证库表匹配但行过滤不匹配
func TestDbTableFilterRealBinlogMatchButRowsFilterNotMatch(t *testing.T) {
	fdeData, tmeData, rowsData := getRealBinlogData()

	var binlogStream bytes.Buffer
	binlogStream.Write(fdeData)
	binlogStream.Write(tmeData)
	binlogStream.Write(rowsData)

	parser := NewBinlogParser()
	parser.Flashback = true
	parser.PrintEventInfo = &PrintEventInfo{}

	// 设置库表过滤: include db.tbl（匹配）
	filter, err := db_table_filter.NewFilter(
		[]string{"db"}, []string{"tbl"},
		[]string{}, []string{},
	)
	require.NoError(t, err)
	err = filter.DbTableFilterCompile()
	require.NoError(t, err)
	parser.TableFilter = filter

	// 设置行过滤: col[0] > 100（不匹配，因为值是 1）
	rf, err := NewRowsFilter("col[0] > 100")
	require.NoError(t, err)
	parser.RowsFilter = rf

	var events []*BinlogEvent
	err = parser.ParseReader(&binlogStream, func(e *BinlogEvent) error {
		events = append(events, e)
		return nil
	})
	require.NoError(t, err)

	// 找到 RowsEvent
	var rowsEvent *BinlogEvent
	for _, ev := range events {
		if isRowsEvent(ev.Header.EventType) {
			rowsEvent = ev
			break
		}
	}
	require.NotNil(t, rowsEvent)

	// 库表匹配但行过滤不匹配: rawBytesNew 被截断
	re, ok := rowsEvent.Event.(*RowsEvent)
	require.True(t, ok)
	require.Equal(t, 0, re.RowsMatched)
	require.Equal(t, EventHeaderSize, len(re.rawBytesNew))
}

// TestDbTableFilterRealBinlogAllWildcard 使用真实 binlog 数据验证 *.* 全匹配
func TestDbTableFilterRealBinlogAllWildcard(t *testing.T) {
	fdeData, tmeData, rowsData := getRealBinlogData()

	var binlogStream bytes.Buffer
	binlogStream.Write(fdeData)
	binlogStream.Write(tmeData)
	binlogStream.Write(rowsData)

	parser := NewBinlogParser()
	parser.Flashback = true
	parser.PrintEventInfo = &PrintEventInfo{}

	// 设置库表过滤: include *.*（全匹配）
	filter, err := db_table_filter.NewFilter(
		[]string{"*"}, []string{"*"},
		[]string{}, []string{},
	)
	require.NoError(t, err)
	err = filter.DbTableFilterCompile()
	require.NoError(t, err)
	parser.TableFilter = filter

	var events []*BinlogEvent
	err = parser.ParseReader(&binlogStream, func(e *BinlogEvent) error {
		events = append(events, e)
		return nil
	})
	require.NoError(t, err)

	// 找到 RowsEvent
	var rowsEvent *BinlogEvent
	for _, ev := range events {
		if isRowsEvent(ev.Header.EventType) {
			rowsEvent = ev
			break
		}
	}
	require.NotNil(t, rowsEvent)

	// *.* 全匹配: WRITE → DELETE
	require.Equal(t, DELETE_ROWS_EVENTv2, rowsEvent.Header.EventType)

	re, ok := rowsEvent.Event.(*RowsEvent)
	require.True(t, ok)
	require.Len(t, re.Rows, 1)
	require.Equal(t, int32(1), re.Rows[0][0])
}

// ============================================================
// 负数测试
// ============================================================

// TestRowsFilterNegativeInt 验证负数 INT 值的行过滤
func TestRowsFilterNegativeInt(t *testing.T) {
	table := &TableMapEvent{
		tableIDSize: 6,
		TableID:     0x6c,
		Schema:      []byte("db"),
		Table:       []byte("tbl"),
		ColumnCount: 1,
		ColumnType:  []byte{MYSQL_TYPE_LONG},
		ColumnMeta:  []uint16{0},
		NullBitmap:  []byte{0x00},
	}

	// INT 负数在二进制中以补码形式存储（uint32 表示）
	// -1 = 0xFFFFFFFF, -5 = 0xFFFFFFFB, -100 = 0xFFFFFF9C
	// 10 = 0x0000000A
	rowData := []byte{
		0x00, 0xFF, 0xFF, 0xFF, 0xFF, // row1: -1 (uint32: 4294967295)
		0x00, 0xFB, 0xFF, 0xFF, 0xFF, // row2: -5 (uint32: 4294967291)
		0x00, 0x9C, 0xFF, 0xFF, 0xFF, // row3: -100 (uint32: 4294967196)
		0x00, 0x0A, 0x00, 0x00, 0x00, // row4: 10 (uint32: 10)
	}

	rawData := buildRowsEventRawData(0x6c, WRITE_ROWS_EVENTv2, 1, rowData)

	t.Run("过滤大于阈值的int32值", func(t *testing.T) {
		// 负数以 int32 有符号存储
		// 过滤条件: col[0] > -3 (只有 -1 和 10 满足)
		rf, err := NewRowsFilter("col[0] > -3")
		require.NoError(t, err)

		e := &RowsEvent{
			Version:     2,
			tableIDSize: 6,
			tables:      map[uint64]*TableMapEvent{0x6c: table},
			eventType:   WRITE_ROWS_EVENTv2,
			flashback:   true,
			rowsFilter:  rf,
			rawBytesNew: make([]byte, len(rawData)),
		}
		copy(e.rawBytesNew, rawData)

		bodyData := rawData[EventHeaderSize:]
		pos, err := e.DecodeHeader(bodyData)
		require.NoError(t, err)

		err = e.FlashbackData2(pos, bodyData)
		require.NoError(t, err)

		// -1 > -3 满足, -5 < -3 不满足, -100 < -3 不满足, 10 > -3 满足
		require.Equal(t, 2, e.RowsMatched)
		require.Len(t, e.Rows, 2)
		require.Equal(t, int32(-1), e.Rows[0][0])
		require.Equal(t, int32(10), e.Rows[1][0])
	})

	t.Run("精确匹配负数", func(t *testing.T) {
		// -100 精确匹配
		rf, err := NewRowsFilter("col[0] == -100")
		require.NoError(t, err)

		e := &RowsEvent{
			Version:     2,
			tableIDSize: 6,
			tables:      map[uint64]*TableMapEvent{0x6c: table},
			eventType:   WRITE_ROWS_EVENTv2,
			flashback:   true,
			rowsFilter:  rf,
			rawBytesNew: make([]byte, len(rawData)),
		}
		copy(e.rawBytesNew, rawData)

		bodyData := rawData[EventHeaderSize:]
		pos, err := e.DecodeHeader(bodyData)
		require.NoError(t, err)

		err = e.FlashbackData2(pos, bodyData)
		require.NoError(t, err)

		// 只有 -100 匹配
		require.Equal(t, 1, e.RowsMatched)
		require.Len(t, e.Rows, 1)
		require.Equal(t, int32(-100), e.Rows[0][0])
	})

	t.Run("小于比较过滤", func(t *testing.T) {
		// 过滤条件: col[0] < 0 (负数都满足: -1, -5, -100)
		rf, err := NewRowsFilter("col[0] < 0")
		require.NoError(t, err)

		e := &RowsEvent{
			Version:     2,
			tableIDSize: 6,
			tables:      map[uint64]*TableMapEvent{0x6c: table},
			eventType:   WRITE_ROWS_EVENTv2,
			flashback:   true,
			rowsFilter:  rf,
			rawBytesNew: make([]byte, len(rawData)),
		}
		copy(e.rawBytesNew, rawData)

		bodyData := rawData[EventHeaderSize:]
		pos, err := e.DecodeHeader(bodyData)
		require.NoError(t, err)

		err = e.FlashbackData2(pos, bodyData)
		require.NoError(t, err)

		// -1, -5, -100 都满足 < 0，10 不满足
		require.Equal(t, 3, e.RowsMatched)
		require.Len(t, e.Rows, 3)
		require.Equal(t, int32(-1), e.Rows[0][0])
		require.Equal(t, int32(-5), e.Rows[1][0])
		require.Equal(t, int32(-100), e.Rows[2][0])
	})
}

// TestRowsFilterNegativeTinyInt 验证负数 TINYINT 值的行过滤
func TestRowsFilterNegativeTinyInt(t *testing.T) {
	table := &TableMapEvent{
		tableIDSize: 6,
		TableID:     0x6c,
		Schema:      []byte("db"),
		Table:       []byte("tbl"),
		ColumnCount: 1,
		ColumnType:  []byte{MYSQL_TYPE_TINY},
		ColumnMeta:  []uint16{0},
		NullBitmap:  []byte{0x00},
	}

	// TINYINT: -1=0xFF, -10=0xF6, 5=0x05
	rowData := []byte{
		0x00, 0xFF, // row1: -1 (int8: -1)
		0x00, 0xF6, // row2: -10 (int8: -10)
		0x00, 0x05, // row3: 5 (int8: 5)
	}

	rawData := buildRowsEventRawData(0x6c, WRITE_ROWS_EVENTv2, 1, rowData)

	// 过滤条件: col[0] < 0 (只有 -1 和 -10 满足)
	rf, err := NewRowsFilter("col[0] < 0")
	require.NoError(t, err)

	e := &RowsEvent{
		Version:     2,
		tableIDSize: 6,
		tables:      map[uint64]*TableMapEvent{0x6c: table},
		eventType:   WRITE_ROWS_EVENTv2,
		flashback:   true,
		rowsFilter:  rf,
		rawBytesNew: make([]byte, len(rawData)),
	}
	copy(e.rawBytesNew, rawData)

	bodyData := rawData[EventHeaderSize:]
	pos, err := e.DecodeHeader(bodyData)
	require.NoError(t, err)

	err = e.FlashbackData2(pos, bodyData)
	require.NoError(t, err)

	// -1 和 -10 满足 < 0
	require.Equal(t, 2, e.RowsMatched)
	require.Len(t, e.Rows, 2)
	require.Equal(t, int8(-1), e.Rows[0][0])
	require.Equal(t, int8(-10), e.Rows[1][0])
}

// TestRowsFilterNegativeBigInt 验证负数 BIGINT 值的行过滤
func TestRowsFilterNegativeBigInt(t *testing.T) {
	table := &TableMapEvent{
		tableIDSize: 6,
		TableID:     0x6c,
		Schema:      []byte("db"),
		Table:       []byte("tbl"),
		ColumnCount: 1,
		ColumnType:  []byte{MYSQL_TYPE_LONGLONG},
		ColumnMeta:  []uint16{0},
		NullBitmap:  []byte{0x00},
	}

	// BIGINT: -1 = 0xFFFFFFFFFFFFFFFF, 100 = 0x0000000000000064
	// -1 小端序: FF FF FF FF FF FF FF FF
	// 100 小端序: 64 00 00 00 00 00 00 00
	rowData := []byte{
		0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // row1: -1 (int64: -1)
		0x00, 0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // row2: 100
	}

	rawData := buildRowsEventRawData(0x6c, WRITE_ROWS_EVENTv2, 1, rowData)

	// 过滤条件: col[0] < 0 (只有 -1 满足)
	rf, err := NewRowsFilter("col[0] < 0")
	require.NoError(t, err)

	e := &RowsEvent{
		Version:     2,
		tableIDSize: 6,
		tables:      map[uint64]*TableMapEvent{0x6c: table},
		eventType:   WRITE_ROWS_EVENTv2,
		flashback:   true,
		rowsFilter:  rf,
		rawBytesNew: make([]byte, len(rawData)),
	}
	copy(e.rawBytesNew, rawData)

	bodyData := rawData[EventHeaderSize:]
	pos, err := e.DecodeHeader(bodyData)
	require.NoError(t, err)

	err = e.FlashbackData2(pos, bodyData)
	require.NoError(t, err)

	// -1 满足 < 0，100 不满足
	require.Equal(t, 1, e.RowsMatched)
	require.Len(t, e.Rows, 1)
	require.Equal(t, int64(-1), e.Rows[0][0])
}

// ============================================================
// 压缩格式 binlog 过滤测试
// ============================================================

// buildCompressedRowsEventRawData 构建压缩格式的 RowsEvent 原始数据
// 使用 zlib 压缩行数据部分
func buildCompressedRowsEventRawData(tableID uint64, eventType EventType, columnCount int, rowData []byte) []byte {
	// Event Header (19 bytes)
	header := make([]byte, EventHeaderSize)
	binary.LittleEndian.PutUint32(header[TimestampPos:], 1000000)
	header[EventTypePos] = byte(eventType)
	binary.LittleEndian.PutUint32(header[5:9], 0x0b)
	binary.LittleEndian.PutUint16(header[17:19], 1)

	// Event Body
	body := make([]byte, 0, 64)
	// table_id (6 bytes for v2)
	tidBuf := make([]byte, 6)
	tidBuf[0] = byte(tableID)
	tidBuf[1] = byte(tableID >> 8)
	tidBuf[2] = byte(tableID >> 16)
	tidBuf[3] = byte(tableID >> 24)
	tidBuf[4] = byte(tableID >> 32)
	tidBuf[5] = byte(tableID >> 40)
	body = append(body, tidBuf...)

	// flags (2 bytes)
	body = append(body, 0x01, 0x00)

	// extra_data_length (2 bytes, v2 only)
	body = append(body, 0x02, 0x00)

	// column_count (lenenc_int)
	body = append(body, byte(columnCount))

	// column bitmap1
	bitmapSize := (columnCount + 7) / 8
	bitmap := make([]byte, bitmapSize)
	for i := range bitmap {
		bitmap[i] = 0xFF
	}
	body = append(body, bitmap...)

	// 如果是 UPDATE 压缩事件，需要第二个 bitmap
	if eventType == TENDB_UPDATE_ROWS_COMPRESSED_EVENT_V2 || eventType == TENDB_UPDATE_ROWS_COMPRESSED_EVENT_V1 {
		body = append(body, bitmap...)
	}

	// 压缩行数据部分
	// DecompressMariadbData 格式:
	// byte 0: headerSize = data[0] & 0x07 (uncompressed size 的字节数)
	// bytes 1..1+headerSize: uncompressed data size (big-endian)
	// remaining: zlib compressed data
	var compressedBuf bytes.Buffer
	zlibWriter := zlib.NewWriter(&compressedBuf)
	zlibWriter.Write(rowData)
	zlibWriter.Close()

	// headerSize = 3 (足够表示大多数测试数据的长度)
	uncompressedSize := len(rowData)
	compressHeader := make([]byte, 4) // 1 byte headerSize + 3 bytes size
	compressHeader[0] = 0x03          // headerSize = 3
	compressHeader[1] = byte(uncompressedSize >> 16)
	compressHeader[2] = byte(uncompressedSize >> 8)
	compressHeader[3] = byte(uncompressedSize)

	body = append(body, compressHeader...)
	body = append(body, compressedBuf.Bytes()...)

	// 更新 event_size
	totalSize := uint32(EventHeaderSize + len(body))
	binary.LittleEndian.PutUint32(header[EventSizPos:EventSizPos+4], totalSize)

	result := make([]byte, 0, len(header)+len(body))
	result = append(result, header...)
	result = append(result, body...)
	return result
}

// TestCompressedWriteRowsFlashback 验证 TENDB_WRITE_ROWS_COMPRESSED_EVENT_V2 的闪回
func TestCompressedWriteRowsFlashback(t *testing.T) {
	table := &TableMapEvent{
		tableIDSize: 6,
		TableID:     0x6c,
		Schema:      []byte("db"),
		Table:       []byte("tbl"),
		ColumnCount: 1,
		ColumnType:  []byte{MYSQL_TYPE_LONG},
		ColumnMeta:  []uint16{0},
		NullBitmap:  []byte{0x00},
	}

	// 2 行: value=42, value=100
	rowData := []byte{
		0x00, 0x2a, 0x00, 0x00, 0x00, // row1: 42
		0x00, 0x64, 0x00, 0x00, 0x00, // row2: 100
	}

	rawData := buildCompressedRowsEventRawData(0x6c, TENDB_WRITE_ROWS_COMPRESSED_EVENT_V2, 1, rowData)

	e := &RowsEvent{
		Version:     2,
		tableIDSize: 6,
		tables:      map[uint64]*TableMapEvent{0x6c: table},
		eventType:   TENDB_WRITE_ROWS_COMPRESSED_EVENT_V2,
		compressed:  true,
		flashback:   true,
		rawBytesNew: make([]byte, len(rawData)),
	}
	copy(e.rawBytesNew, rawData)

	bodyData := rawData[EventHeaderSize:]
	pos, err := e.DecodeHeader(bodyData)
	require.NoError(t, err)

	err = e.FlashbackData2(pos, bodyData)
	require.NoError(t, err)

	// 闪回: WRITE → DELETE
	require.Equal(t, DELETE_ROWS_EVENTv2, e.eventType)
	require.Equal(t, byte(DELETE_ROWS_EVENTv2), e.rawBytesNew[EventTypePos])

	// 验证行数据正确解压和解析
	require.Len(t, e.Rows, 2)
	require.Equal(t, int32(42), e.Rows[0][0])
	require.Equal(t, int32(100), e.Rows[1][0])
}

// TestCompressedUpdateRowsFlashback 验证 TENDB_UPDATE_ROWS_COMPRESSED_EVENT_V2 的闪回
func TestCompressedUpdateRowsFlashback(t *testing.T) {
	table := &TableMapEvent{
		tableIDSize: 6,
		TableID:     0x6c,
		Schema:      []byte("db"),
		Table:       []byte("tbl"),
		ColumnCount: 1,
		ColumnType:  []byte{MYSQL_TYPE_LONG},
		ColumnMeta:  []uint16{0},
		NullBitmap:  []byte{0x00},
	}

	// UPDATE: BI=10, AI=20
	rowData := []byte{
		0x00, 0x0a, 0x00, 0x00, 0x00, // BI: 10
		0x00, 0x14, 0x00, 0x00, 0x00, // AI: 20
	}

	rawData := buildCompressedRowsEventRawData(0x6c, TENDB_UPDATE_ROWS_COMPRESSED_EVENT_V2, 1, rowData)

	e := &RowsEvent{
		Version:     2,
		tableIDSize: 6,
		tables:      map[uint64]*TableMapEvent{0x6c: table},
		eventType:   TENDB_UPDATE_ROWS_COMPRESSED_EVENT_V2,
		compressed:  true,
		needBitmap2: true,
		flashback:   true,
		rawBytesNew: make([]byte, len(rawData)),
	}
	copy(e.rawBytesNew, rawData)

	bodyData := rawData[EventHeaderSize:]
	pos, err := e.DecodeHeader(bodyData)
	require.NoError(t, err)

	err = e.FlashbackData2(pos, bodyData)
	require.NoError(t, err)

	// 闪回: TENDB_UPDATE_COMPRESSED → UPDATE，前后镜像互换
	require.Equal(t, UPDATE_ROWS_EVENTv2, e.eventType)
	require.Equal(t, byte(UPDATE_ROWS_EVENTv2), e.rawBytesNew[EventTypePos])

	// 验证前后镜像互换
	require.Len(t, e.Rows, 2)
	require.Equal(t, int32(20), e.Rows[0][0]) // 新 BI（原 AI）
	require.Equal(t, int32(10), e.Rows[1][0]) // 新 AI（原 BI）
}

// TestCompressedRowsWithRowsFilter 验证压缩事件 + 行过滤
func TestCompressedRowsWithRowsFilter(t *testing.T) {
	table := &TableMapEvent{
		tableIDSize: 6,
		TableID:     0x6c,
		Schema:      []byte("db"),
		Table:       []byte("tbl"),
		ColumnCount: 1,
		ColumnType:  []byte{MYSQL_TYPE_LONG},
		ColumnMeta:  []uint16{0},
		NullBitmap:  []byte{0x00},
	}

	// 3 行: value=1, value=50, value=100
	rowData := []byte{
		0x00, 0x01, 0x00, 0x00, 0x00, // row1: 1
		0x00, 0x32, 0x00, 0x00, 0x00, // row2: 50
		0x00, 0x64, 0x00, 0x00, 0x00, // row3: 100
	}

	rawData := buildCompressedRowsEventRawData(0x6c, TENDB_WRITE_ROWS_COMPRESSED_EVENT_V2, 1, rowData)

	// 过滤条件: col[0] >= 50
	rf, err := NewRowsFilter("col[0] >= 50")
	require.NoError(t, err)

	e := &RowsEvent{
		Version:     2,
		tableIDSize: 6,
		tables:      map[uint64]*TableMapEvent{0x6c: table},
		eventType:   TENDB_WRITE_ROWS_COMPRESSED_EVENT_V2,
		compressed:  true,
		flashback:   true,
		rowsFilter:  rf,
		rawBytesNew: make([]byte, len(rawData)),
	}
	copy(e.rawBytesNew, rawData)

	bodyData := rawData[EventHeaderSize:]
	pos, err := e.DecodeHeader(bodyData)
	require.NoError(t, err)

	err = e.FlashbackData2(pos, bodyData)
	require.NoError(t, err)

	// 闪回: WRITE → DELETE，且只保留 col[0] >= 50 的行
	require.Equal(t, DELETE_ROWS_EVENTv2, e.eventType)
	require.Equal(t, 2, e.RowsMatched)
	require.Len(t, e.Rows, 2)
	require.Equal(t, int32(50), e.Rows[0][0])
	require.Equal(t, int32(100), e.Rows[1][0])
}

// TestCompressedUpdateRowsWithRowsFilter 验证压缩 UPDATE 事件 + 行过滤
func TestCompressedUpdateRowsWithRowsFilter(t *testing.T) {
	table := &TableMapEvent{
		tableIDSize: 6,
		TableID:     0x6c,
		Schema:      []byte("db"),
		Table:       []byte("tbl"),
		ColumnCount: 1,
		ColumnType:  []byte{MYSQL_TYPE_LONG},
		ColumnMeta:  []uint16{0},
		NullBitmap:  []byte{0x00},
	}

	// 2 行 UPDATE: row1 BI=1 AI=11, row2 BI=50 AI=55
	rowData := []byte{
		0x00, 0x01, 0x00, 0x00, 0x00, // row1 BI: 1
		0x00, 0x0b, 0x00, 0x00, 0x00, // row1 AI: 11
		0x00, 0x32, 0x00, 0x00, 0x00, // row2 BI: 50
		0x00, 0x37, 0x00, 0x00, 0x00, // row2 AI: 55
	}

	rawData := buildCompressedRowsEventRawData(0x6c, TENDB_UPDATE_ROWS_COMPRESSED_EVENT_V2, 1, rowData)

	// 过滤条件: col[0] >= 50 (基于 BI 过滤)
	rf, err := NewRowsFilter("col[0] >= 50")
	require.NoError(t, err)

	e := &RowsEvent{
		Version:     2,
		tableIDSize: 6,
		tables:      map[uint64]*TableMapEvent{0x6c: table},
		eventType:   TENDB_UPDATE_ROWS_COMPRESSED_EVENT_V2,
		compressed:  true,
		needBitmap2: true,
		flashback:   true,
		rowsFilter:  rf,
		rawBytesNew: make([]byte, len(rawData)),
	}
	copy(e.rawBytesNew, rawData)

	bodyData := rawData[EventHeaderSize:]
	pos, err := e.DecodeHeader(bodyData)
	require.NoError(t, err)

	err = e.FlashbackData2(pos, bodyData)
	require.NoError(t, err)

	// 只有 row2 (BI=50) 满足过滤条件
	require.Equal(t, UPDATE_ROWS_EVENTv2, e.eventType)
	require.Equal(t, 1, e.RowsMatched)
	// 闪回后前后镜像互换: BI=55(原AI), AI=50(原BI)
	require.Len(t, e.Rows, 2)
	require.Equal(t, int32(55), e.Rows[0][0]) // 新 BI（原 AI）
	require.Equal(t, int32(50), e.Rows[1][0]) // 新 AI（原 BI）
}

// TestCompressedRowsWithDbTableFilter 验证压缩事件 + 库表过滤
func TestCompressedRowsWithDbTableFilter(t *testing.T) {
	table := &TableMapEvent{
		tableIDSize: 6,
		TableID:     0x6c,
		Schema:      []byte("db"),
		Table:       []byte("tbl"),
		ColumnCount: 1,
		ColumnType:  []byte{MYSQL_TYPE_LONG},
		ColumnMeta:  []uint16{0},
		NullBitmap:  []byte{0x00},
	}

	// value=42
	rowData := []byte{0x00, 0x2a, 0x00, 0x00, 0x00}

	rawData := buildCompressedRowsEventRawData(0x6c, TENDB_WRITE_ROWS_COMPRESSED_EVENT_V2, 1, rowData)

	// 设置库表过滤: include db.tbl（匹配）
	filter, err := db_table_filter.NewFilter(
		[]string{"db"}, []string{"tbl"},
		[]string{}, []string{},
	)
	require.NoError(t, err)
	err = filter.DbTableFilterCompile()
	require.NoError(t, err)

	e := &RowsEvent{
		Version:       2,
		tableIDSize:   6,
		tables:        map[uint64]*TableMapEvent{0x6c: table},
		eventType:     TENDB_WRITE_ROWS_COMPRESSED_EVENT_V2,
		compressed:    true,
		flashback:     true,
		dbTableFilter: filter,
		rawBytesNew:   make([]byte, len(rawData)),
	}
	copy(e.rawBytesNew, rawData)

	bodyData := rawData[EventHeaderSize:]
	// 使用 flashbackRowsEventFunc 完整流程（包含 dbTableFilter 检查）
	err = flashbackRowsEventFunc(e, bodyData)
	require.NoError(t, err)

	// 库表匹配: WRITE → DELETE
	require.Equal(t, DELETE_ROWS_EVENTv2, e.eventType)
	require.Len(t, e.Rows, 1)
	require.Equal(t, int32(42), e.Rows[0][0])
}

// TestCompressedRowsWithDbTableFilterNotMatch 验证压缩事件 + 库表过滤不匹配
func TestCompressedRowsWithDbTableFilterNotMatch(t *testing.T) {
	table := &TableMapEvent{
		tableIDSize: 6,
		TableID:     0x6c,
		Schema:      []byte("db"),
		Table:       []byte("tbl"),
		ColumnCount: 1,
		ColumnType:  []byte{MYSQL_TYPE_LONG},
		ColumnMeta:  []uint16{0},
		NullBitmap:  []byte{0x00},
	}

	// value=42
	rowData := []byte{0x00, 0x2a, 0x00, 0x00, 0x00}

	rawData := buildCompressedRowsEventRawData(0x6c, TENDB_WRITE_ROWS_COMPRESSED_EVENT_V2, 1, rowData)

	// 设置库表过滤: include otherdb.other（不匹配）
	filter, err := db_table_filter.NewFilter(
		[]string{"otherdb"}, []string{"other"},
		[]string{}, []string{},
	)
	require.NoError(t, err)
	err = filter.DbTableFilterCompile()
	require.NoError(t, err)

	e := &RowsEvent{
		Version:       2,
		tableIDSize:   6,
		tables:        map[uint64]*TableMapEvent{0x6c: table},
		eventType:     TENDB_WRITE_ROWS_COMPRESSED_EVENT_V2,
		compressed:    true,
		flashback:     true,
		dbTableFilter: filter,
		rawBytesNew:   make([]byte, len(rawData)),
	}
	copy(e.rawBytesNew, rawData)

	bodyData := rawData[EventHeaderSize:]
	// 使用 flashbackRowsEventFunc 完整流程（包含 dbTableFilter 检查）
	err = flashbackRowsEventFunc(e, bodyData)
	require.NoError(t, err)

	// 库表不匹配: rawBytesNew 被截断
	require.Equal(t, EventHeaderSize, len(e.rawBytesNew))
}

// ============================================================
// TransactionPayloadEvent 过滤测试
// ============================================================

// TestTransactionPayloadEventWithDbTableFilter 验证 TransactionPayloadEvent 内部事件的库表过滤
func TestTransactionPayloadEventWithDbTableFilter(t *testing.T) {
	// 使用 TestTransactionPayloadEventDecode 中的真实 payload 数据
	// 该 payload 解压后包含: QUERY + TABLE_MAP + WRITE_ROWS + TABLE_MAP + UPDATE_ROWS + TABLE_MAP + DELETE_ROWS + XID
	fdeFormat := FormatDescriptionEvent{
		Version:                0x4,
		ServerVersion:          "8.0.27",
		CreateTimestamp:        0x0,
		EventHeaderLength:      0x13,
		EventTypeHeaderLengths: []uint8{0x38, 0xd, 0x0, 0x8, 0x0, 0x12, 0x0, 0x4, 0x4, 0x4, 0x4, 0x12, 0x0, 0x0, 0x5c, 0x0, 0x4, 0x1a, 0x8, 0x0, 0x0, 0x0, 0x8, 0x8, 0x8, 0x2, 0x0, 0x0, 0x0, 0xa, 0xa, 0xa, 0x19, 0x19, 0x0, 0x12, 0x34, 0x0, 0xa, 0x28, 0x0},
		ChecksumAlgorithm:      0x1,
	}

	payload := []byte{
		0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x58, 0xbc, 0x0a,
		0x00, 0xf6, 0x13, 0x44, 0x35, 0x60, 0x45, 0xd3,
		0x1c, 0x00, 0x92, 0x80, 0xa2, 0x43, 0x5d, 0x92,
		0xe0, 0x70, 0x43, 0xc5, 0x30, 0x0c, 0x12, 0x9c,
		0xdf, 0xcd, 0xfa, 0x0d, 0x1a, 0x06, 0x11, 0x11,
		0x91, 0x05, 0x79, 0x83, 0x3e, 0x4a, 0x41, 0xda,
		0xbb, 0xb5, 0x6e, 0xd4, 0xb7, 0xb0, 0x36, 0x45,
		0x98, 0x23, 0x74, 0x0d, 0x6b, 0x3c, 0xfa, 0xde,
		0x24, 0x05, 0x37, 0x00, 0x34, 0x00, 0x36, 0x00,
		0xec, 0x97, 0x4b, 0xfe, 0x33, 0x04, 0x9c, 0x27,
		0xeb, 0x41, 0xe2, 0x10, 0x69, 0x78, 0x45, 0xd9,
		0x6a, 0x52, 0x41, 0x91, 0xa0, 0x28, 0x6b, 0xa9,
		0x50, 0x50, 0xe8, 0xaf, 0x83, 0x5f, 0x87, 0x1c,
		0xc2, 0xa8, 0x15, 0xd5, 0x34, 0xfe, 0x3f, 0x72,
		0xf1, 0x07, 0xbb, 0xc2, 0xef, 0x78, 0xc1, 0x07,
		0x0e, 0xf1, 0x9f, 0x35, 0x9c, 0x27, 0x2b, 0x52,
		0x8c, 0xf4, 0x49, 0x67, 0xfb, 0x3e, 0x7a, 0x2d,
		0xec, 0x5a, 0xa5, 0x8d, 0xd6, 0x81, 0x53, 0xfe,
		0xcd, 0xe2, 0x7f, 0xfb, 0xd5, 0xbc, 0x35, 0x00,
		0xff, 0xd9, 0x02, 0xce, 0x93, 0xf5, 0x2b, 0xd3,
		0xb4, 0x33, 0xd6, 0x38, 0xa3, 0x19, 0x64, 0x9a,
		0xe6, 0x5d, 0x75, 0x9d, 0x58, 0x6c, 0xe9, 0x09,
		0x02, 0xc7, 0x03, 0xf3, 0x3a, 0xcf, 0x85, 0x11,
		0x52, 0x2a, 0x25, 0x7c, 0xd1, 0x75, 0x8d, 0xae,
		0x75, 0x9c, 0x87, 0xc6, 0x87, 0xfd, 0x0a, 0x98,
		0x0c, 0x38, 0x4f, 0x18, 0x8c, 0x35, 0x65, 0x6c,
		0x9b, 0xde, 0xe7, 0x1e, 0x28, 0x59, 0xe9, 0x38,
		0x77, 0xba, 0x48, 0x8a, 0xb1, 0x3e, 0x91, 0x8d,
		0xe4, 0x8c, 0xee, 0xeb, 0x5a, 0x6f, 0xf7, 0x75,
		0x01, 0xff, 0xff, 0x3f, 0x22, 0xe1, 0x3f, 0x73,
		0xe0, 0x6c, 0x61, 0x87, 0x0f, 0x9c, 0xa7, 0xeb,
		0xb2, 0xaa, 0xad, 0xba, 0x56, 0x2e, 0xff, 0x05,
		0x02, 0x4c, 0x19, 0x53, 0xe8, 0x76, 0xcf, 0xea,
		0xae, 0xe4, 0x40, 0x55, 0x82, 0x8f, 0x46, 0x11,
		0x84, 0xde, 0x9c, 0x33, 0x42, 0xea, 0x60, 0x43,
		0xd2, 0x94, 0x8f, 0x0c, 0x18, 0x00, 0x56, 0x15,
		0x08, 0x52, 0xd0, 0x00, 0x10, 0x06, 0xac, 0x2e,
		0xd7, 0x32, 0x8a, 0xa4, 0x84, 0x0c, 0x48, 0x42,
		0x80, 0x44, 0x9a, 0xe5, 0x02, 0x37, 0x9b, 0x87,
		0x24, 0x02, 0x01, 0x3d, 0x86, 0x50, 0x4d, 0x3e,
		0x91, 0x39, 0x40, 0x0d, 0xb0, 0xaf, 0xf0, 0x09,
		0xfa, 0x9a, 0x1e, 0x43, 0xd4, 0x60, 0x82, 0x84,
		0x37, 0x81, 0x78, 0x55, 0xcd, 0x00, 0x0c, 0x27,
		0x78, 0x10, 0x08, 0x0d, 0x06, 0x19, 0x80, 0x17,
		0x01, 0x00, 0x00,
	}

	// 先解码 TransactionPayloadEvent 获取内部事件
	tpe := &TransactionPayloadEvent{
		format:           fdeFormat,
		Size:             91132,
		UncompressedSize: 625404,
		CompressionType:  ZSTD,
		Payload:          payload,
	}
	err := tpe.decodePayload()
	require.NoError(t, err)
	require.Len(t, tpe.Events, 8)

	// 验证内部事件类型
	require.Equal(t, TABLE_MAP_EVENT, tpe.Events[1].Header.EventType)
	require.Equal(t, WRITE_ROWS_EVENTv2, tpe.Events[2].Header.EventType)

	// 获取 TABLE_MAP_EVENT 中的库表信息
	tme, ok := tpe.Events[1].Event.(*TableMapEvent)
	require.True(t, ok)
	t.Logf("TransactionPayload 内部表: schema=%s, table=%s", tme.Schema, tme.Table)

	// 验证 RowsEvent 能正确解析
	re, ok := tpe.Events[2].Event.(*RowsEvent)
	require.True(t, ok)
	require.True(t, len(re.Rows) > 0, "WRITE_ROWS 事件应该有行数据")

	// 使用 ParseReader 完整流程测试 TransactionPayloadEvent + DbTableFilter
	// 构建包含 FDE + TRANSACTION_PAYLOAD_EVENT 的 binlog 流
	// FDE header
	fdeHeader := make([]byte, EventHeaderSize)
	binary.LittleEndian.PutUint32(fdeHeader[TimestampPos:], 0)
	fdeHeader[EventTypePos] = byte(FORMAT_DESCRIPTION_EVENT)
	binary.LittleEndian.PutUint32(fdeHeader[5:9], 1)

	// FDE body
	fdeBody := make([]byte, 0, 200)
	// binlog version (2 bytes)
	fdeBody = append(fdeBody, 0x04, 0x00)
	// server version (50 bytes)
	sv := make([]byte, 50)
	copy(sv, "8.0.27")
	fdeBody = append(fdeBody, sv...)
	// create timestamp (4 bytes)
	fdeBody = append(fdeBody, 0x00, 0x00, 0x00, 0x00)
	// event header length (1 byte)
	fdeBody = append(fdeBody, 0x13)
	// event type header lengths
	fdeBody = append(fdeBody, fdeFormat.EventTypeHeaderLengths...)
	// checksum algorithm
	fdeBody = append(fdeBody, 0x01)
	// checksum (4 bytes)
	fdeBody = append(fdeBody, 0x00, 0x00, 0x00, 0x00)

	fdeTotalSize := uint32(EventHeaderSize + len(fdeBody))
	binary.LittleEndian.PutUint32(fdeHeader[EventSizPos:EventSizPos+4], fdeTotalSize)
	binary.LittleEndian.PutUint32(fdeHeader[13:17], fdeTotalSize) // next pos
	binary.LittleEndian.PutUint16(fdeHeader[17:19], 0)

	// TRANSACTION_PAYLOAD_EVENT header
	tpeHeader := make([]byte, EventHeaderSize)
	binary.LittleEndian.PutUint32(tpeHeader[TimestampPos:], 1000000)
	tpeHeader[EventTypePos] = byte(TRANSACTION_PAYLOAD_EVENT)
	binary.LittleEndian.PutUint32(tpeHeader[5:9], 1)

	// TRANSACTION_PAYLOAD_EVENT body
	// field: OTW_PAYLOAD_COMPRESSION_TYPE_FIELD (type=2, len=1, value=0=ZSTD)
	// field: OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD (type=3, len=4, value=625404)
	// field: OTW_PAYLOAD_SIZE_FIELD (type=1, len=4, value=91132)
	// field: END_MARK (type=0)
	// payload data
	tpeBody := make([]byte, 0, len(payload)+20)
	// compression type field
	tpeBody = append(tpeBody, OTW_PAYLOAD_COMPRESSION_TYPE_FIELD, 0x01, 0x00)
	// uncompressed size field
	tpeBody = append(tpeBody, OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD, 0x04)
	uncompSizeBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(uncompSizeBuf, 625404)
	tpeBody = append(tpeBody, uncompSizeBuf...)
	// size field
	tpeBody = append(tpeBody, OTW_PAYLOAD_SIZE_FIELD, 0x04)
	sizeBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(sizeBuf, 91132)
	tpeBody = append(tpeBody, sizeBuf...)
	// end mark
	tpeBody = append(tpeBody, OTW_PAYLOAD_HEADER_END_MARK)
	// payload
	tpeBody = append(tpeBody, payload...)
	// checksum (4 bytes)
	tpeBody = append(tpeBody, 0x00, 0x00, 0x00, 0x00)

	tpeTotalSize := uint32(EventHeaderSize + len(tpeBody))
	binary.LittleEndian.PutUint32(tpeHeader[EventSizPos:EventSizPos+4], tpeTotalSize)
	binary.LittleEndian.PutUint32(tpeHeader[13:17], fdeTotalSize+tpeTotalSize)
	binary.LittleEndian.PutUint16(tpeHeader[17:19], 0)

	var binlogStream bytes.Buffer
	binlogStream.Write(fdeHeader)
	binlogStream.Write(fdeBody)
	binlogStream.Write(tpeHeader)
	binlogStream.Write(tpeBody)

	t.Run("匹配库表过滤", func(t *testing.T) {
		parser := NewBinlogParser()
		parser.Flashback = true
		parser.PrintEventInfo = &PrintEventInfo{}

		// 使用 TransactionPayload 内部的真实库表名
		filter, err := db_table_filter.NewFilter(
			[]string{string(tme.Schema)}, []string{string(tme.Table)},
			[]string{}, []string{},
		)
		require.NoError(t, err)
		err = filter.DbTableFilterCompile()
		require.NoError(t, err)
		parser.TableFilter = filter

		var events []*BinlogEvent
		reader := bytes.NewReader(binlogStream.Bytes())
		err = parser.ParseReader(reader, func(e *BinlogEvent) error {
			events = append(events, e)
			return nil
		})
		require.NoError(t, err)

		// 应该有 RowsEvent 被处理
		var rowsEvents []*BinlogEvent
		for _, ev := range events {
			if isRowsEvent(ev.Header.EventType) {
				rowsEvents = append(rowsEvents, ev)
			}
		}
		require.True(t, len(rowsEvents) > 0, "匹配的库表应该有 RowsEvent 输出")

		// 验证闪回: WRITE → DELETE
		for _, ev := range rowsEvents {
			re, ok := ev.Event.(*RowsEvent)
			require.True(t, ok)
			require.True(t, len(re.Rows) > 0 || len(re.rawBytesNew) == EventHeaderSize)
		}
	})

	t.Run("不匹配库表过滤", func(t *testing.T) {
		parser := NewBinlogParser()
		parser.Flashback = true
		parser.PrintEventInfo = &PrintEventInfo{}

		// 使用不匹配的库表名
		filter, err := db_table_filter.NewFilter(
			[]string{"nonexistent_db"}, []string{"nonexistent_table"},
			[]string{}, []string{},
		)
		require.NoError(t, err)
		err = filter.DbTableFilterCompile()
		require.NoError(t, err)
		parser.TableFilter = filter

		var events []*BinlogEvent
		reader := bytes.NewReader(binlogStream.Bytes())
		err = parser.ParseReader(reader, func(e *BinlogEvent) error {
			events = append(events, e)
			return nil
		})
		require.NoError(t, err)

		// 所有 RowsEvent 的 rawBytesNew 应该被截断
		for _, ev := range events {
			if isRowsEvent(ev.Header.EventType) {
				re, ok := ev.Event.(*RowsEvent)
				require.True(t, ok)
				require.Equal(t, EventHeaderSize, len(re.rawBytesNew),
					"不匹配的库表: rawBytesNew 应该被截断")
			}
		}
	})

	t.Run("行过滤", func(t *testing.T) {
		parser := NewBinlogParser()
		parser.Flashback = true
		parser.PrintEventInfo = &PrintEventInfo{}

		// 匹配所有库表
		filter, err := db_table_filter.NewFilter(
			[]string{"*"}, []string{"*"},
			[]string{}, []string{},
		)
		require.NoError(t, err)
		err = filter.DbTableFilterCompile()
		require.NoError(t, err)
		parser.TableFilter = filter

		// 设置行过滤: col[0] > 99999999 (大概率不匹配)
		rf, err := NewRowsFilter("col[0] > 99999999")
		require.NoError(t, err)
		parser.RowsFilter = rf

		var events []*BinlogEvent
		reader := bytes.NewReader(binlogStream.Bytes())
		err = parser.ParseReader(reader, func(e *BinlogEvent) error {
			events = append(events, e)
			return nil
		})
		require.NoError(t, err)

		// 验证行过滤生效
		for _, ev := range events {
			if isRowsEvent(ev.Header.EventType) {
				re, ok := ev.Event.(*RowsEvent)
				require.True(t, ok)
				// 行过滤后，要么有匹配行，要么 rawBytesNew 被截断
				if re.RowsMatched == 0 {
					require.Equal(t, EventHeaderSize, len(re.rawBytesNew))
				}
			}
		}
	})
}
