package gh

import (
	"strings"
)

// QueryBuilder wraps the logic for building dynamic queries for GORM
// that need to be execute by the db.Raw() method.
type QueryBuilder struct {
	query string        // Initial query
	args  []interface{} // Arguments
}

// NewQueryBuilder creates a new instance of the QueryBuilder.
/*
Example Usage:

	baseQuery := "SELECT DATE_TRUNC('year', date) as date, billable_type, doctor, SUM(total_amount) AS total_amount FROM income_per_billable"

	qb := NewQueryBuilder(baseQuery)

	doctor := "Dr. Smith"
	category := "Consultation"
	period := "2023"

	qb.Where("doctor=?", doctor).
		Where("billable_type=?", category).
		Where("DATE_PART('year', date)=?", period).
		GroupBy("DATE_TRUNC('year', date)", "billable_type", "doctor").
		OrderBy("total_amount DESC", "DATE_TRUNC('year', date)", "billable_type")

	query, args := qb.Build()

	db.Raw(query, args...)
*/
func NewQueryBuilder(baseQuery string) *QueryBuilder {
	return &QueryBuilder{
		query: baseQuery,
		args:  []interface{}{},
	}
}

// Where adds a where condition. Takes care of appending AND if more that one call
// has been made.
// Note that if value == "", the where condition is ignored.
func (qb *QueryBuilder) Where(condition string, value interface{}) *QueryBuilder {
	if value != "" {
		if len(qb.args) > 0 {
			qb.query += " AND " + condition
		} else {
			qb.query += " WHERE " + condition
		}
		qb.args = append(qb.args, value)
	}
	return qb
}

// GroupBy adds a GROUP BY clause. Must be called after where to generate a proper query.
func (qb *QueryBuilder) GroupBy(columns ...string) *QueryBuilder {
	if len(columns) > 0 {
		qb.query += " GROUP BY " + strings.Join(columns, ", ")
	}
	return qb
}

// OrderBy adds an ORDER BY clause. Must be called after Where and/or GroupBy
func (qb *QueryBuilder) OrderBy(columns ...string) *QueryBuilder {
	if len(columns) > 0 {
		qb.query += " ORDER BY " + strings.Join(columns, ", ")
	}
	return qb
}

// Build returns the final query and its arguments.
func (qb *QueryBuilder) Build() (string, []interface{}) {
	return qb.query, qb.args
}
