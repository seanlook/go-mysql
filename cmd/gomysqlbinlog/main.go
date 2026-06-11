package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/pkg"
	"github.com/go-mysql-org/go-mysql/pkg/db_table_filter"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/jinzhu/copier"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type ParallelType string

const (
	TypeDefault      = "" // default
	TypeDatabase     = "database"
	TypeTable        = "table"
	TypeTableHash    = "table_hash"
	TypePrimaryHash  = "primary_hash"
	TypeLogicalClock = "logical_clock"
)

func parseBinlogFile() error {
	printEventInfo := &replication.PrintEventInfo{}
	printEventInfo.Init()

	p := replication.NewBinlogParser()
	p.PrintEventInfo = printEventInfo

	p.Flashback = viper.GetBool("flashback")
	p.ConvUpdateToWrite = viper.GetBool("conv-rows-update-to-write")
	p.SkipRowsEventTimeUpdate = viper.GetBool("skip-rows-event-time-update")
	eventTypeFilter := viper.GetStringSlice("rows-event-type")
	if len(eventTypeFilter) > 0 {
		for _, evt := range eventTypeFilter {
			if evt == "delete" {
				p.EventTypeFilter = append(p.EventTypeFilter, replication.DeleteEventType...)
			} else if evt == "update" {
				p.EventTypeFilter = append(p.EventTypeFilter, replication.UpdateEventType...)
			} else if evt == "insert" {
				p.EventTypeFilter = append(p.EventTypeFilter, replication.InsertEventType...)
			} else {
				return errors.Errorf("unknown eventy type %s", evt)
			}
		}
	}

	renameRules := viper.GetStringSlice("rewrite-db")
	if len(renameRules) > 0 {
		if rules, err := pkg.NewRenameRule(renameRules); err != nil {
			return err
		} else {
			p.RenameRule = rules
		}
	}

	timeFilter := &replication.TimeFilter{
		StartPos: viper.GetUint32("start-position"),
		StopPos:  viper.GetUint32("stop-position"),
	}
	if start := viper.GetString("start-datetime"); start != "" {
		startDatetime, err := time.ParseInLocation(time.DateTime, viper.GetString("start-datetime"), time.Local)
		if err != nil {
			return errors.WithMessage(err, "parse start-datetime")
		}
		timeFilter.StartTime = uint32(startDatetime.Local().Unix())
	}
	if stop := viper.GetString("stop-datetime"); stop != "" {
		stopDatetime, err := time.ParseInLocation(time.DateTime, viper.GetString("stop-datetime"), time.Local)
		if err != nil {
			return errors.WithMessage(err, "parse stop-datetime")
		}
		timeFilter.StopTime = uint32(stopDatetime.Local().Unix())
		p.TimeFilterQuickStop = viper.GetBool("time-filter-quick-stop")
	}
	p.TimeFilter = &replication.TimeFilter{}
	_ = copier.Copy(p.TimeFilter, timeFilter) // 这里因为 startPos, stopPos是针对不同的 file 生效

	var tableFilter *db_table_filter.DbTableFilter
	var err error
	databases := viper.GetStringSlice("databases")
	tables := viper.GetStringSlice("tables")
	excludeDatabases := viper.GetStringSlice("exclude-databases")
	excludeTables := viper.GetStringSlice("exclude-tables")
	if len(databases)+len(tables)+len(excludeDatabases)+len(excludeTables) > 0 {
		if p.Flashback {
			excludeDatabases = append(excludeDatabases, "infodba_schema")
		}
		tableFilter, err = db_table_filter.NewFilter(databases, tables, excludeDatabases, excludeTables)
		if err != nil {
			return err
		}
		p.TableFilter = tableFilter
		if err = p.TableFilter.DbTableFilterCompile(); err != nil {
			return err
		}
	} else if p.Flashback {
		tableFilter, err = db_table_filter.NewFilter([]string{"*"}, []string{"*"}, []string{"infodba_schema"}, []string{})
		p.TableFilter = tableFilter
		if err = p.TableFilter.DbTableFilterCompile(); err != nil {
			return err
		}
	}

	rfBuilder := rowsFilterBuilder{}
	if rowsFilterFromFile := viper.GetString("rows-filter-from-csv"); rowsFilterFromFile != "" {
		csvContent, err := os.ReadFile(rowsFilterFromFile)
		if err != nil {
			return errors.WithMessagef(err, "read file --rows-filter-from-csv=%s", rowsFilterFromFile)
		}
		buf := bytes.NewBuffer(csvContent)
		rowsFilterExpr, err := rfBuilder.buildRowsFilterExprFromCsv(buf)
		if err != nil {
			return err
		}
		// compile
		rowsFilter, err := replication.NewRowsFilter(rowsFilterExpr) // "col[0] == 2"
		if err != nil {
			return err
		}
		rowsFilter.AllNumberToString = rfBuilder.allNumberToString
		p.RowsFilter = rowsFilter
	}
	if rowsFilterExpr := viper.GetString("rows-filter"); rowsFilterExpr != "" {
		if guessRowsFilterType(rowsFilterExpr) < 1 { // csv format, will change to go-expr format
			buf := bytes.NewBufferString(rowsFilterExpr)
			rowsFilterExpr, err = rfBuilder.buildRowsFilterExprFromCsv(buf)
			if err != nil {
				return err
			}
		}
		// go-expr format
		rowsFilter, err := replication.NewRowsFilter(rowsFilterExpr) // "col[0] == 2"
		if err != nil {
			return err
		}
		rowsFilter.AllNumberToString = rfBuilder.allNumberToString
		p.RowsFilter = rowsFilter
	}

	// file is alias for start-file
	fileNames := viper.GetStringSlice("file")
	startFile := viper.GetString("start-file")
	stopFile := viper.GetString("stop-file")
	binlogDir := viper.GetString("binlog-dir")

	startFile = filepath.Base(startFile)
	stopFile = filepath.Base(stopFile)
	resultFileName := viper.GetString("result-file") // resultFileName is empty will output to stdout

	// 注意这里的输出逻辑
	// 单个文件解析 --files one-file
	//   正向解析/反向解析：都可以指定输出到 stdout 或者 result-file （result-file=""时输出到 stdout）
	// 多个文件解析
	//   如果是反向解析：不允许指定 result-file，不支持直接输出到 stdout，只能输出到默认的 xxx.sql 另存文件
	//   如果是正向解析
	//     --files file1,file2,file3: 精确模式，同单文件解析的输出，按顺序输出到一个 result-file
	//     --start-file file1 --stop-file file3：范围模式，每个文件的结果输出到自己的 xxx.sql 另存文件
	// output-per-file 自动根据 binlog 文件名，生产对应的解析后的 .sql 文件名。相反，输出到 result-file 或者 stdout
	if len(fileNames) == 1 {
		p.TimeFilter = timeFilter
		binlogDir = filepath.Join(binlogDir, filepath.Dir(fileNames[0]))
		fileName := filepath.Base(fileNames[0])
		binlogFile := filepath.Join(binlogDir, fileName)
		err = p.ParseFileAndPrint(binlogFile, resultFileName)
		if err != nil {
			fmt.Println(err.Error())
			return err
		}
	} else { // 多文件解析
		if p.Flashback && resultFileName != "" {
			// 多文件解析，正向解析，如果指定 result-file，则都写入同一个文件，如果不指定，则都写入 stdout
			// 多文件解析，反向解析，不允许指定 result-file
			return errors.New("--flashback cannot have --result-file when parsing multiple files")
		}
		binlogDir = filepath.Join(binlogDir, filepath.Dir(startFile))
		startSeq := pkg.GetSequenceFromFilename(startFile)
		stopSeq := pkg.GetSequenceFromFilename(stopFile)
		for seq := startSeq; seq <= stopSeq; seq++ {
			if seq == startSeq { // 只有第一个 binlog才需要 startPos
				p.TimeFilter.StartPos = timeFilter.StartPos
			} else {
				p.TimeFilter.StartPos = 0
			}
			if seq == stopSeq { // 只有最后一个 binlog才需要 stopPos
				p.TimeFilter.StopPos = timeFilter.StopPos
			} else {
				p.TimeFilter.StopPos = 0
			}
			fileName := pkg.ConstructBinlogFilename(filepath.Base(startFile), seq)
			binlogFile := filepath.Join(binlogDir, fileName)
			if stopFile != "" { // range mode
				resultFileName = fileName + ".go.sql"
			}
			if p.Flashback { // flashback mode
				resultFileName = fileName + ".back.sql"
			}
			err = p.ParseFileAndPrint(binlogFile, resultFileName)
			if err != nil {
				fmt.Println(err.Error())
				return err
			}
		}
	}
	return nil
}

// guessRowsFilterType 1:expr, -1:csv
func guessRowsFilterType(rowsFilterExpr string) int {
	if !strings.Contains(rowsFilterExpr, "\n") {
		return 1
	}
	// https://expr-lang.org/docs/language-definition
	exprKeyword := []string{"==", "!=", "<", ">", "<=", ">=", "&&", "||", "!",
		" and ", " AND ", " or ", " OR ", " not ", " NOT ", " in ", " IN "}
	for _, keyword := range exprKeyword {
		if strings.Contains(rowsFilterExpr, keyword) {
			return 1
		}
	}
	return -1
}
