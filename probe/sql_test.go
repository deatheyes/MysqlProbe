package probe

import (
	"testing"

	"github.com/deatheyes/sqlparser"
)

func TestTransfrom(t *testing.T) {
	query := "select * from t where a = 1 and b in (5, 6, 7)"
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		t.Error(err)
	}

	template := generateQuery(stmt, true)
	want := "select * from t where a = ? and b in (?, ?, ?)"
	if template != want {
		t.Errorf("unexpected result, template: %v , want: %v", template, want)
	}

	want = query
	result := generateQuery(stmt, false)
	if result != want {
		t.Errorf("unexpected result, query: %v , want: %v", result, want)
	}
}
