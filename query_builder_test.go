package gh_test

import (
	"reflect"
	"testing"

	"github.com/abiiranathan/gh"
)

func TestQueryBuilder(t *testing.T) {
	tests := []struct {
		name           string
		baseQuery      string
		doctor         string
		category       string
		period         string
		groupByColumns []string
		orderByColumns []string
		expectedQuery  string
		expectedArgs   []interface{}
	}{
		{
			name:           "No filters",
			baseQuery:      "SELECT DATE_TRUNC('year', date) as date, billable_type, doctor, SUM(total_amount) AS total_amount FROM income_per_billable",
			doctor:         "",
			category:       "",
			period:         "",
			groupByColumns: []string{"DATE_TRUNC('year', date)", "billable_type", "doctor"},
			orderByColumns: []string{"total_amount DESC", "DATE_TRUNC('year', date)", "billable_type"},
			expectedQuery:  "SELECT DATE_TRUNC('year', date) as date, billable_type, doctor, SUM(total_amount) AS total_amount FROM income_per_billable GROUP BY DATE_TRUNC('year', date), billable_type, doctor ORDER BY total_amount DESC, DATE_TRUNC('year', date), billable_type",
			expectedArgs:   []interface{}{},
		},
		{
			name:           "With doctor filter",
			baseQuery:      "SELECT DATE_TRUNC('year', date) as date, billable_type, doctor, SUM(total_amount) AS total_amount FROM income_per_billable",
			doctor:         "Dr. Smith",
			category:       "",
			period:         "",
			groupByColumns: []string{"DATE_TRUNC('year', date)", "billable_type", "doctor"},
			orderByColumns: []string{"total_amount DESC", "DATE_TRUNC('year', date)", "billable_type"},
			expectedQuery:  "SELECT DATE_TRUNC('year', date) as date, billable_type, doctor, SUM(total_amount) AS total_amount FROM income_per_billable WHERE doctor=? GROUP BY DATE_TRUNC('year', date), billable_type, doctor ORDER BY total_amount DESC, DATE_TRUNC('year', date), billable_type",
			expectedArgs:   []interface{}{"Dr. Smith"},
		},
		{
			name:           "With doctor and category filters",
			baseQuery:      "SELECT DATE_TRUNC('year', date) as date, billable_type, doctor, SUM(total_amount) AS total_amount FROM income_per_billable",
			doctor:         "Dr. Smith",
			category:       "Consultation",
			period:         "",
			groupByColumns: []string{"DATE_TRUNC('year', date)", "billable_type", "doctor"},
			orderByColumns: []string{"total_amount DESC", "DATE_TRUNC('year', date)", "billable_type"},
			expectedQuery:  "SELECT DATE_TRUNC('year', date) as date, billable_type, doctor, SUM(total_amount) AS total_amount FROM income_per_billable WHERE doctor=? AND billable_type=? GROUP BY DATE_TRUNC('year', date), billable_type, doctor ORDER BY total_amount DESC, DATE_TRUNC('year', date), billable_type",
			expectedArgs:   []interface{}{"Dr. Smith", "Consultation"},
		},
		{
			name:           "With all filters",
			baseQuery:      "SELECT DATE_TRUNC('year', date) as date, billable_type, doctor, SUM(total_amount) AS total_amount FROM income_per_billable",
			doctor:         "Dr. Smith",
			category:       "Consultation",
			period:         "2023",
			groupByColumns: []string{"DATE_TRUNC('year', date)", "billable_type", "doctor"},
			orderByColumns: []string{"total_amount DESC", "DATE_TRUNC('year', date)", "billable_type"},
			expectedQuery:  "SELECT DATE_TRUNC('year', date) as date, billable_type, doctor, SUM(total_amount) AS total_amount FROM income_per_billable WHERE doctor=? AND billable_type=? AND DATE_PART('year', date)=? GROUP BY DATE_TRUNC('year', date), billable_type, doctor ORDER BY total_amount DESC, DATE_TRUNC('year', date), billable_type",
			expectedArgs:   []interface{}{"Dr. Smith", "Consultation", "2023"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qb := gh.NewQueryBuilder(tt.baseQuery)

			qb.Where("doctor=?", tt.doctor)
			qb.Where("billable_type=?", tt.category)
			qb.Where("DATE_PART('year', date)=?", tt.period)
			qb.GroupBy(tt.groupByColumns...)
			qb.OrderBy(tt.orderByColumns...)

			query, args := qb.Build()

			if query != tt.expectedQuery {
				t.Errorf("Query mismatch:\nExpected: %s\nGot: %s", tt.expectedQuery, query)
			}

			if !reflect.DeepEqual(args, tt.expectedArgs) {
				t.Errorf("Args mismatch:\nExpected: %v\nGot: %v", tt.expectedArgs, args)
			}
		})
	}
}
