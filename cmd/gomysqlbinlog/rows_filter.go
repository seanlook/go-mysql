package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/cast"
)

type rowsFilterBuilder struct {
	hasNumber         bool
	hasString         bool
	allNumberToString bool
	buildCount        int
}

type ColumnDef struct {
	ColumnName string
	// Position in information_schema.columns, start from 1
	Position int
	// DataType original data type from schema definition
	DataType string
	// TypeAlias int, str, hex
	TypeAlias string
}
type TableColumnInfo map[string]*ColumnDef

func (b *rowsFilterBuilder) parseHeaderToColumnDef(columnName string) (*ColumnDef, error) {
	parts := strings.Split(columnName, ":")
	if len(parts) == 1 {
		return &ColumnDef{
			ColumnName: columnName,
		}, nil
	} else if len(parts) == 2 {
		return &ColumnDef{
			ColumnName: parts[0],
			TypeAlias:  parts[1],
		}, nil
	} else if len(parts) == 3 {
		if pos, err := cast.ToIntE(parts[3]); err == nil {
			return &ColumnDef{
				ColumnName: parts[0],
				TypeAlias:  parts[1],
				Position:   pos,
			}, nil
		} else {
			return nil, errors.WithMessagef(err, "parse column position failed from %s", columnName)
		}
	} else {
		return nil, errors.Errorf("wrong column name format %s", columnName)
	}
}

const (
	valueTypeAliasInteger = "integer"
	valueTypeAliasString  = "string"
	valueTypeAliasHex     = "hex"
)

func (b *rowsFilterBuilder) wrapValueWithDatatype(value string, typeAlias string) string {
	if b.allNumberToString {
		typeAlias = valueTypeAliasString
		b.hasString = true
		return quoteString(value)
	}

	typeAlias = strings.ToLower(typeAlias)
	numeric := []string{"tinyint", "smallint", "mediumint", "int", "bigint", "integer", "decimal", "dec", "float",
		"double", "bool", "boolean", "bit", "hex"}
	numberAsString := []string{"bigint_unsigned", "float"}

	if typeAlias == "" {
		if val, err := cast.ToFloat32E(value); err == nil {
			if val > float32(math.MaxInt64) {
				typeAlias = valueTypeAliasString
			} else {
				typeAlias = valueTypeAliasInteger
			}
		} else if strings.HasPrefix(value, "0x") { // hex
			typeAlias = valueTypeAliasHex
		} else {
			typeAlias = valueTypeAliasString
		}
	} else if slices.Contains(numberAsString, typeAlias) {
		typeAlias = valueTypeAliasString
	} else if slices.Contains(numeric, typeAlias) {
		typeAlias = valueTypeAliasInteger
	} else {
		typeAlias = valueTypeAliasString
	}

	if typeAlias == valueTypeAliasString {
		b.hasString = true
		return quoteString(value)
	} else {
		b.hasNumber = true
		return strings.Trim(strings.Trim(value, "'"), "\"")
	}
}

func quoteString(s string) string {
	if len(s) == 0 {
		return "''"
	}
	if s[0] == '\'' && s[len(s)-1] == '\'' {
		return s
	}
	if s[0] == '"' && s[len(s)-1] == '"' {
		return s
	}
	return strconv.Quote(s)
}

func (b *rowsFilterBuilder) buildRowsFilterExprFromCsv(csvInput *bytes.Buffer) (exprOutput string, err error) {
	b.hasString = false
	b.hasNumber = false
	b.buildCount += 1

	csvReader := csv.NewReader(csvInput)
	records, err := csvReader.ReadAll()
	if err != nil {
		return "", errors.WithMessagef(err, "Unable to parse file as CSV for")
	}
	if len(records) <= 1 {
		return "", errors.Errorf("error csv format")
	}

	rowsFilterExpr := ""
	var rowsFilterExprs []string
	/*
		var records [][]string
		lines := strings.Split(csvInput, "\n")
		for _, line := range lines {
			cols := strings.Split(line, ",")
			records = append(records, cols)
		}
	*/

	if len(records) <= 1 {
		return "", errors.Errorf("error csv format")
	}
	exprHeader := records[0]
	if len(exprHeader) == 1 {
		colDef, err := b.parseHeaderToColumnDef(exprHeader[0])
		if err != nil {
			return "", err
		}
		rowsFilterExpr = fmt.Sprintf("%s in ", colDef.ColumnName)
		rowsFilterExpr += "["
		var valueList []string
		for _, line := range records[1:] {
			valueList = append(valueList, b.wrapValueWithDatatype(line[0], colDef.TypeAlias))
		}
		rowsFilterExpr += strings.Join(valueList, ",")
		rowsFilterExpr += "]"
	} else {
		for _, line := range records[1:] {
			var lineExpr []string
			for i, col := range line {
				//rowsFilterExpr = fmt.Sprintf("%s == %s", exprHeader[i], col)
				colDef, err := b.parseHeaderToColumnDef(exprHeader[i])
				if err != nil {
					return "", err
				}
				lineExpr = append(lineExpr, fmt.Sprintf("%s == %s",
					colDef.ColumnName, b.wrapValueWithDatatype(col, colDef.TypeAlias)))
			}
			rowsFilterExprs = append(rowsFilterExprs, "("+strings.Join(lineExpr, " and ")+")")
		}
		rowsFilterExpr = strings.Join(rowsFilterExprs, " or ")
	}
	return rowsFilterExpr, nil
}
