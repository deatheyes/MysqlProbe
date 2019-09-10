package probe

import (
	"fmt"

	"github.com/deatheyes/sqlparser"
)

// templateFormatter replace all the const values to '?'
func templateFormatter(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
	if f, ok := node.(*sqlparser.FuncExpr); ok {
		var distinct string
		if f.Distinct {
			distinct = "distinct "
		}

		val := fmt.Sprintf("%s%s", distinct, sqlparser.String(f.Exprs))
		arg := buf.FuncArg()
		buf.Vars[arg] = val
		buf.Myprintf("%s(%s)", f.Name.String(), arg)
		return
	}

	if value, ok := node.(*sqlparser.ComparisonExpr); ok {
		if value.Operator == sqlparser.InStr || value.Operator == sqlparser.NotInStr {
			val := sqlparser.String(value.Right)
			arg := buf.VarArg()
			buf.Vars[arg] = val
			buf.Myprintf("%v %s %s", value.Left, value.Operator, arg)
			return
		}
	}

	if value, ok := node.(*sqlparser.SQLVal); ok {
		switch value.Type {
		case sqlparser.ValArg:
			buf.WriteArg(string(value.Val))
		default:
			val := sqlparser.String(value)
			arg := buf.VarArg()
			buf.Vars[arg] = val
			buf.Myprintf("%s", arg)
		}
		return
	}

	if _, ok := node.(*sqlparser.NullVal); ok {
		arg := buf.VarArg()
		buf.Vars[arg] = "null"
		buf.Myprintf("%s", arg)
		return
	}

	node.Format(buf)
}

func generateQuery(node sqlparser.SQLNode, template bool) (string, map[string]string) {
	var buff *sqlparser.TrackedBuffer
	if template {
		buff = sqlparser.NewTrackedBuffer(templateFormatter)
	} else {
		buff = sqlparser.NewTrackedBuffer(nil)
	}
	node.Format(buff)
	return buff.String(), buff.Vars
}

// GenerateSourceQuery rebuild the query by AST
func GenerateSourceQuery(node sqlparser.SQLNode) (string, map[string]string) {
	return generateQuery(node, false)
}

// GenerateTemplateQuery generate a template according to the AST
func GenerateTemplateQuery(node sqlparser.SQLNode) (string, map[string]string) {
	return generateQuery(node, true)
}
