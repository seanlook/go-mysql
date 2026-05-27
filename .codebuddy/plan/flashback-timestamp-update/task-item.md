# 实施计划

- [ ] 1. 新增 TimestampPos 常量定义
   - 在 `replication/event.go` 中，紧跟 `EventTypePos` 和 `EventSizPos` 常量之后，新增 `const TimestampPos = 0`
   - _需求：3.1_

- [ ] 2. 在 BinlogParser 中新增闪回时间戳字段
   - 在 `replication/parser.go` 的 `BinlogParser` 结构体中新增 `flashbackTimestamp uint32` 字段，用于存储闪回开始时刻的时间戳
   - 在 `parseSingleEvent` 方法开头（进入闪回处理逻辑时），如果 `p.Flashback == true` 且 `p.flashbackTimestamp == 0`，则设置 `p.flashbackTimestamp = uint32(time.Now().Unix())`
   - _需求：2.1_

- [ ] 3. 在 RowsEvent 闪回处理中写入新时间戳
   - 在 `replication/parser_ext.go` 的 `ParseEvent2` 方法中，处理 RowsEvent 的分支里（约第 604 行），在 `len(re.rawBytesNew) > EventHeaderSize` 判断内、`computeCrc32Checksum` 调用之前，使用 `binary.LittleEndian.PutUint32(re.rawBytesNew[TimestampPos:TimestampPos+4], p.flashbackTimestamp)` 写入新时间戳
   - 仅在 `p.Flashback == true` 时执行此修改
   - _需求：1.1、1.3、1.5、2.2、2.3_

- [ ] 4. 在 TableMapEvent 处理中写入新时间戳
   - 在 `replication/parser_ext.go` 的 `ParseEvent2` 方法中，处理 TableMapEvent 的分支里（约第 658 行），在 `computeCrc32Checksum` 调用之前，使用 `binary.LittleEndian.PutUint32(te.rawBytesNew[TimestampPos:TimestampPos+4], p.flashbackTimestamp)` 写入新时间戳
   - 仅在 `p.Flashback == true` 时执行此修改
   - _需求：1.2、2.2、2.3_

- [ ] 5. 同步更新 BinlogEvent.Header.Timestamp
   - 在 `replication/parser.go` 的 `parseSingleEvent` 方法中，调用 `ParseEvent2` 返回后、构造 `BinlogEvent` 之前，如果 `p.Flashback == true`，将 `h.Timestamp = p.flashbackTimestamp`
   - _需求：2.4_

- [ ] 6. 编写单元测试验证时间戳更新
   - 在 `replication/parser_test.go` 中新增测试用例 `TestFlashbackTimestampUpdate`
   - 测试内容：解析一个包含 rows event 的 binlog 文件进行闪回，验证输出的 rawBytesNew 前 4 字节为当前时间（允许几秒误差），而非原始事件时间
   - 验证 checksum 正确性：对输出的 rawBytesNew 去掉最后 4 字节后计算 CRC32，与最后 4 字节比较
   - _需求：1.1、2.2、2.3_
