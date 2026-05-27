# 需求文档

## 引言

在 go-mysql 库的 binlog 闪回（Flashback）功能中，当前产生的闪回 binlog 事件保留了原始事件的时间戳。这导致在**连续二次闪回**场景下出现问题：用户对第一次闪回产生的 binlog 文件再次执行闪回时，如果使用时间范围过滤条件（`--start-datetime` / `--stop-datetime`），由于事件时间戳仍然是原始旧时间，会导致本应参与闪回的事件被时间过滤条件错误地跳过或排除。

**解决方案**：在闪回处理过程中，将产生的新事件的 Event Header 中的 Timestamp 字段更新为当前时间（闪回执行时刻的时间），使得闪回产生的 binlog 文件具有新的时间线，从而支持基于时间范围的连续闪回操作。

### 技术背景

- Event Header 结构（共 19 字节）：
  - `Timestamp`（offset 0-3）：4 字节 little-endian uint32，Unix 时间戳
  - `EventType`（offset 4）：1 字节
  - `ServerID`（offset 5-8）：4 字节
  - `EventSize`（offset 9-12）：4 字节
  - `LogPos`（offset 13-16）：4 字节
  - `Flags`（offset 17-18）：2 字节

- 闪回涉及的关键代码路径：
  - `parser.go` 的 `parseSingleEvent` 方法：解析单个事件并调用 `ParseEvent2`
  - `parser_ext.go` 的 `ParseEvent2` 方法：处理 rows event 的闪回逻辑，修改 `rawBytesNew`
  - `row_event_parser.go` 的 `FlashbackData2` / `DecodeData2`：实际的行数据反转逻辑

- 已有常量：`EventTypePos = 4`，`EventSizPos = 9`，需要新增 `TimestampPos = 0`

## 需求

### 需求 1

**用户故事：** 作为一名 DBA，我希望闪回产生的 binlog 事件使用当前时间作为时间戳，以便在连续二次闪回时能够正确使用时间范围过滤条件。

#### 验收标准

1. WHEN 执行闪回操作产生新的 rows event（INSERT/UPDATE/DELETE 反转）时 THEN 系统 SHALL 将新事件的 Event Header 中 Timestamp 字段更新为闪回执行时刻的 Unix 时间戳（uint32）
2. WHEN 执行闪回操作产生新的 TABLE_MAP_EVENT 时 THEN 系统 SHALL 将该事件的 Event Header 中 Timestamp 字段同样更新为当前时间戳
3. WHEN 闪回产生的事件 rawBytesNew 长度大于 EventHeaderSize（即事件有效）时 THEN 系统 SHALL 在 rawBytesNew 的 offset 0-3 位置写入当前时间的 little-endian uint32 值
4. WHEN 闪回产生的事件 rawBytesNew 长度等于 EventHeaderSize（即事件被过滤为无效）时 THEN 系统 SHALL 不修改时间戳（因为该事件不会被输出）
5. IF 非闪回模式（仅行过滤 RowsFilter 或表过滤 TableFilter）THEN 系统 SHALL 不修改事件时间戳，保持原始时间

### 需求 2

**用户故事：** 作为一名 DBA，我希望闪回产生的 binlog 文件中所有相关事件的时间戳保持一致性，并且 checksum 正确，以便时间范围过滤能正确工作且 binlog 文件格式合法。

#### 验收标准

1. WHEN 同一次闪回操作中处理多个事件时 THEN 系统 SHALL 使用同一个时间戳值（闪回开始时刻），确保所有闪回事件的时间戳一致
2. WHEN binlog 开启了 CRC32 checksum（`ChecksumAlgorithm == BINLOG_CHECKSUM_ALG_CRC32`）时 THEN 系统 SHALL 确保时间戳修改在 `computeCrc32Checksum` 调用之前完成，因为 checksum 是对整个 rawBytesNew（包含 Event Header）计算的，header 中 timestamp 的变更会影响 checksum 值
3. WHEN binlog 开启了 CRC32 checksum 且 header timestamp 被修改后 THEN 系统 SHALL 重新计算并追加正确的 4 字节 CRC32 校验和到 rawBytesNew 末尾（现有代码已通过 `p.computeCrc32Checksum(re.rawBytesNew)` 实现，只需确保时间戳修改在此调用之前）
4. WHEN 修改时间戳后 THEN 系统 SHALL 同步更新 `BinlogEvent.Header.Timestamp` 字段，确保内存中的 Header 对象与 rawData 一致

### 需求 3

**用户故事：** 作为一名开发者，我希望时间戳更新逻辑有清晰的常量定义和代码组织，以便后续维护。

#### 验收标准

1. WHEN 定义时间戳在 Event Header 中的偏移位置时 THEN 系统 SHALL 新增常量 `TimestampPos = 0`（或类似命名），与现有的 `EventTypePos`、`EventSizPos` 保持一致的命名风格
2. WHEN 实现时间戳更新逻辑时 THEN 系统 SHALL 使用 `binary.LittleEndian.PutUint32` 写入时间戳，与现有的 EventSize 修改方式保持一致
