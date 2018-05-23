package probe

import (
	"github.com/xwb1989/sqlparser"
)

func TemplateFormatter(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
	if value, ok := node.(*sqlparser.SQLVal); ok {
		switch value.Type {
		case sqlparser.ValArg:
			buf.WriteArg(string(value.Val))
		default:
			buf.Myprintf("?")
		}
	} else {
		node.Format(buf)
	}
}

func generateQuery(stmt sqlparser.Statement, template bool) string {
	var buff *sqlparser.TrackedBuffer
	if template {
		buff = sqlparser.NewTrackedBuffer(TemplateFormatter)
	} else {
		buff = sqlparser.NewTrackedBuffer(nil)
	}
	stmt.Format(buff)
	return buff.String()
}

func GenerateSourceQuery(stmt sqlparser.Statement) string {
	return generateQuery(stmt, false)
}

func GenerateTemplateQuery(stmt sqlparser.Statement) string {
	return generateQuery(stmt, true)
}
