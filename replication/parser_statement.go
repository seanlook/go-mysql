package replication

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
)

type SchemaNode struct {
	Schema string
	Table  string
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
		notAllowedAlterExists := false // 只要有一个不符合 flashback 条件，就永久false
		if len(t.Specs) > 0 {
			for _, spec := range t.Specs {
				if spec.Tp == ast.AlterTableAddColumns {
					if spec.Position == nil || spec.Position.Tp == ast.ColumnPositionNone {
						// 如果是增加在最后一列，是可以闪回的
						// 判断 add column 没有带 after/first，则认为是最后一列
						continue
					}
					notAllowedAlterExists = true
					//fmt.Println("AlterTableAddColumns", spec.Name, spec.Text())
				} else if spec.Tp == ast.AlterTableDropIndex || spec.Tp == ast.AlterTableAddConstraint {
					// 索引操作是可以闪回的
				} else {
					//fmt.Println("AlterTableStmt-XX", spec.Tp, spec.OriginalText())
					notAllowedAlterExists = true
				}
			}
		}
		if notAllowedAlterExists {
			allowed = false
		}
	case *ast.DropTableStmt:
		if t.IsView {
			// drop view ...
			allowed = true
		} else {
			allowed = false // set false explicitly
		}
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
	case *ast.OptimizeTableStmt:
		allowed = true
		n := &SchemaNode{
			Schema: t.Tables[0].Schema.String(),
			Table:  t.Tables[0].Name.String(),
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
	case *ast.DropProcedureStmt:
		// tiparser does not support FUNCTION
		allowed = true
		n := &SchemaNode{
			Schema: t.ProcedureName.Schema.String(),
			Table:  t.ProcedureName.Name.String(),
		}
		ns = []*SchemaNode{n}
	case *ast.ProcedureInfo:
		// can not find the create_procedure stmt
		allowed = true
		n := &SchemaNode{
			Schema: t.ProcedureName.Schema.String(),
			Table:  t.ProcedureName.Name.String(),
		}
		ns = []*SchemaNode{n}
	case *ast.CreateViewStmt:
		allowed = true
		n := &SchemaNode{
			Schema: t.ViewName.Schema.String(),
			Table:  t.ViewName.Name.String(),
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
