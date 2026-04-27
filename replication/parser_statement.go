package replication

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/samber/lo"
)

type SchemaNode struct {
	Schema string
	Table  string
}

// alter table add column xxx  -> alter table drop column xxx
// alter table add index, drop index -> ok
// drop table, truncate table, alter table modify column xxx, drop column -> no

var allowedStmtForTableFilter = []ast.AlterTableType{
	ast.AlterTableDropIndex,
	ast.AlterTableAddConstraint,
}

func ParseStmt(stmt ast.StmtNode) (allowed bool, ns []*SchemaNode) {
	switch t := stmt.(type) {
	case *ast.RenameTableStmt:
		ns = make([]*SchemaNode, len(t.TableToTables))
		for i, tableInfo := range t.TableToTables {
			ns[i] = &SchemaNode{
				Schema: tableInfo.OldTable.Schema.String(),
				Table:  tableInfo.OldTable.Name.String(),
			}
		}
	case *ast.AlterTableStmt:
		n := &SchemaNode{
			Schema: t.Table.Schema.String(),
			Table:  t.Table.Name.String(),
		}
		ns = []*SchemaNode{n}
		var allSpecs []ast.AlterTableType
		if len(t.Specs) > 0 {
			for _, spec := range t.Specs {
				allSpecs = append(allSpecs, spec.Tp)
				if spec.Tp == ast.AlterTableAddColumns {
					// 如果是增加在最后一列，是可以闪回的
					allowed = true
					//fmt.Println("AlterTableAddColumns", spec.Name, spec.Text())
				} else if spec.Tp == ast.AlterTableDropIndex {
					//fmt.Println("AlterTableDropIndex", spec.IndexName, spec.Text())
				} else if spec.Tp == ast.AlterTableAddConstraint {
					//fmt.Println("AlterTableAddConstraint-Index", spec.IndexName, spec.OriginalText())
				} else {
					//fmt.Println("AlterTableStmt-XX", spec.Tp, spec.OriginalText())
				}
			}
		}
		if lo.Every(allowedStmtForTableFilter, allSpecs) {
			allowed = true
		}
	case *ast.DropTableStmt:
		allowed = false // set false explicitly
		ns = make([]*SchemaNode, len(t.Tables))
		for i, table := range t.Tables {
			ns[i] = &SchemaNode{
				Schema: table.Schema.String(),
				Table:  table.Name.String(),
			}
		}
	case *ast.CreateTableStmt:
		allowed = true
		n := &SchemaNode{
			Schema: t.Table.Schema.String(),
			Table:  t.Table.Name.String(),
		}
		ns = []*SchemaNode{n}
	case *ast.TruncateTableStmt:
		allowed = false
		n := &SchemaNode{
			Schema: t.Table.Schema.String(),
			Table:  t.Table.Name.String(),
		}
		ns = []*SchemaNode{n}
	case *ast.CreateIndexStmt:
		allowed = true
		n := &SchemaNode{
			Schema: t.Table.Schema.String(),
			Table:  t.Table.Name.String(),
		}
		ns = []*SchemaNode{n}
		// 索引操作是可以闪回的
	case *ast.DropIndexStmt:
		allowed = true
		n := &SchemaNode{
			Schema: t.Table.Schema.String(),
			Table:  t.Table.Name.String(),
		}
		ns = []*SchemaNode{n}
	case *ast.InsertStmt:
		tableSource := t.Table.TableRefs.Left.(*ast.TableSource)
		table, ok := tableSource.Source.(*ast.TableName)
		if !ok || table == nil {
			return false, nil
		}
		n := &SchemaNode{
			Schema: table.Schema.String(),
			Table:  table.Name.String(),
		}
		ns = []*SchemaNode{n}
	case *ast.DeleteStmt:
		tableSource := t.TableRefs.TableRefs.Left.(*ast.TableSource)
		table, ok := tableSource.Source.(*ast.TableName)
		if !ok || table == nil {
			return false, nil
		}
		//GetTables()
		n := &SchemaNode{
			Schema: table.Schema.String(),
			Table:  table.Name.String(),
		}
		ns = []*SchemaNode{n}
	case *ast.UpdateStmt:
		tableSource := t.TableRefs.TableRefs.Left.(*ast.TableSource)
		table, ok := tableSource.Source.(*ast.TableName)
		if !ok || table == nil {
			return false, nil
		}
		n := &SchemaNode{
			Schema: table.Schema.String(),
			Table:  table.Name.String(),
		}
		ns = []*SchemaNode{n}
	}
	return allowed, ns
}
