package probe

import (
	"github.com/deatheyes/sqlparser"
)

// templateFormatter replace all the const values to '?'
func templateFormatter(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
	if f, ok := node.(*sqlparser.FuncExpr); ok {
		buf.Myprintf("%s(...)", f.Name.String())
		return
	}

	if value, ok := node.(*sqlparser.ComparisonExpr); ok {
		if value.Operator == sqlparser.InStr || value.Operator == sqlparser.NotInStr {
			buf.Myprintf("%v %s ?", value.Left, value.Operator)
			return
		}
	}

	if value, ok := node.(*sqlparser.SQLVal); ok {
		switch value.Type {
		case sqlparser.ValArg:
			buf.WriteArg(string(value.Val))
		default:
			buf.Myprintf("?")
		}
		return
	}

	if _, ok := node.(*sqlparser.NullVal); ok {
		buf.Myprintf("?")
		return
	}

	node.Format(buf)
}

func generateQuery(node sqlparser.SQLNode, template bool) string {
	var buff *sqlparser.TrackedBuffer
	if template {
		buff = sqlparser.NewTrackedBuffer(templateFormatter)
	} else {
		buff = sqlparser.NewTrackedBuffer(nil)
	}
	node.Format(buff)
	return buff.String()
}

// GenerateSourceQuery rebuild the query by AST
func GenerateSourceQuery(node sqlparser.SQLNode) string {
	return generateQuery(node, false)
}

// GenerateTemplateQuery generate a template according to the AST
func GenerateTemplateQuery(node sqlparser.SQLNode) string {
	return generateQuery(node, true)
}
