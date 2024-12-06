// gh is a package that provides helper functions for working with GORM and postgres databases.
package gh

import (
	"math"

	"gorm.io/gorm"
)

// gormDB is a wrapper around the *gorm.DB object that provides helper functions.
// Methods on this struct can be chained to apply filters and options.
type gormDB struct {
	db *gorm.DB
}

// WrapDB creates a new gormDB instance that wraps the *gorm.DB object.
// This allows you to chain methods to apply filters and options.
func WrapDB(db *gorm.DB) *gormDB {
	return &gormDB{db: db}
}

// DB returns the underlying *gorm.DB object.
func (gdb *gormDB) DB() *gorm.DB {
	return gdb.db
}

// DateRange applies date range filter on a date column
// e.g DateRange(db, "DATE(created_at)", "2021-01-01", "2021-12-31")
// It does nothing if start and end are empty.
func (gdb *gormDB) DateRange(column string, start, end string) *gormDB {
	if start != "" && end != "" {
		gdb.db = gdb.db.Where(column+" BETWEEN ? AND ?", start, end)
	} else if start != "" {
		gdb.db = gdb.db.Where(column+" >= ?", start)
	} else if end != "" {
		gdb.db = gdb.db.Where(column+" <= ?", end)
	}
	return gdb
}

// InRange applies a range filter for numerical or date columns.
// It filters the column using the format: column BETWEEN ? AND ? depending on input.
func (gdb *gormDB) InRange(column string, start, end interface{}) *gormDB {
	if start != nil && end != nil {
		gdb.db = gdb.db.Where(column+" BETWEEN ? AND ?", start, end)
	} else if start != nil {
		gdb.db = gdb.db.Where(column+" >= ?", start)
	} else if end != nil {
		gdb.db = gdb.db.Where(column+" <= ?", end)
	}
	return gdb
}

// MothRange is the same as date range but truncates the date to month
// e.g MonthRange("DATE(created_at)", "2021-01-01", "2021-12-31")
// This will filter to only the records between 2021-01-01 and 2021-12-31
// It does nothing if start and end are empty.
func (gdb *gormDB) MonthRange(column string, start, end string) *gormDB {
	if start != "" && end != "" {
		gdb.db = gdb.db.Where(column+" BETWEEN DATE_TRUNC('month', ?::DATE) AND DATE_TRUNC('month', ?::DATE)", start, end)
	} else if start != "" {
		gdb.db = gdb.db.Where(column+" >= DATE_TRUNC('month', ?::DATE)", start)
	} else if end != "" {
		gdb.db = gdb.db.Where(column+" <= DATE_TRUNC('month', ?::DATE)", end)
	}
	return gdb
}

// YearRange is the same as date range but truncates the date to year
// e.g YearRange("DATE(created_at)", "2021-01-01", "2024-12-31")
// It does nothing if start and end are empty.
func (gdb *gormDB) YearRange(column string, start, end string) *gormDB {
	if start != "" && end != "" {
		gdb.db = gdb.db.Where(column+" BETWEEN DATE_TRUNC('year', ?::DATE) AND DATE_TRUNC('year', ?::DATE)", start, end)
	} else if start != "" {
		gdb.db = gdb.db.Where(column+" >= DATE_TRUNC('year', ?::DATE)", start)
	} else if end != "" {
		gdb.db = gdb.db.Where(column+" <= DATE_TRUNC('year', ?::DATE)", end)
	}
	return gdb
}

// ILIKE applies case-insensitive search on a column.
// If a value is empty, it does nothing.
func (gdb *gormDB) ILIKE(column, value string) *gormDB {
	if value != "" {
		gdb.db = gdb.db.Where(column+" ILIKE ?", "%"+value+"%")
	}
	return gdb
}

// Eq applies equal filter on a column.
// If a value is empty, it does nothing.
func (gdb *gormDB) Eq(column string, value interface{}) *gormDB {
	if value != "" {
		gdb.db = gdb.db.Where(column+" = ?", value)
	}
	return gdb
}

// NotEq applies not equal filter on a column.
// If a value is empty, it does nothing.
func (gdb *gormDB) NotEq(column string, value any) *gormDB {
	if value != "" {
		gdb.db = gdb.db.Where(column+" != ?", value)
	}
	return gdb
}

// In applies IN filter on a column.
// If a value is empty, it does nothing.
func (gdb *gormDB) In(column string, values []any) *gormDB {
	if len(values) > 0 {
		gdb.db = gdb.db.Where(column+" IN ?", values)
	}
	return gdb
}

// NotIn applies NOT IN filter on a column.
// If a value is empty, it does nothing.
func (gdb *gormDB) NotIn(column string, values []any) *gormDB {
	if len(values) > 0 {
		gdb.db = gdb.db.Where(column+" NOT IN ?", values)
	}
	return gdb
}

// IsNull applies a filter for columns that should be NULL.
// If value is false, it will check if the column is NOT NULL.
func (gdb *gormDB) IsNull(column string, isNull bool) *gormDB {
	if isNull {
		gdb.db = gdb.db.Where(column + " IS NULL")
	} else {
		gdb.db = gdb.db.Where(column + " IS NOT NULL")
	}
	return gdb
}

// Distinct applies DISTINCT to the query for unique results based on the column.
func (gdb *gormDB) Distinct(column string) *gormDB {
	if column != "" {
		gdb.db = gdb.db.Distinct(column)
	}
	return gdb
}

// PreloadWithConditions preloads associations with additional conditions applied.
// The query is the association to preload and conditions is a map of key-value pairs.
func (gdb *gormDB) PreloadWithConditions(query string, conditions map[string]interface{}) *gormDB {
	if len(conditions) > 0 {
		gdb.db = gdb.db.Preload(query, func(db *gorm.DB) *gorm.DB {
			for key, value := range conditions {
				db = db.Where(key+" = ?", value)
			}
			return db
		})
	}
	return gdb
}

// Sum calculates the sum of a column. It returns the sum as an integer.
func (gdb *gormDB) Sum(column string) (int64, error) {
	var sum int64
	err := gdb.db.Model(&struct{}{}).Select("SUM(" + column + ")").Scan(&sum).Error
	return sum, err
}

// Avg calculates the average of a column. It returns the average as a float64.
func (gdb *gormDB) Avg(column string) (float64, error) {
	var avg float64
	err := gdb.db.Model(&struct{}{}).Select("AVG(" + column + ")").Scan(&avg).Error
	return avg, err
}

// ComplexFilter allows you to add multiple conditions dynamically.
func (gdb *gormDB) ComplexFilter(conditions map[string]interface{}) *gormDB {
	for column, value := range conditions {
		gdb.db = gdb.db.Where(column+" = ?", value)
	}
	return gdb
}

// CreateInBatches inserts multiple records in a single query.
// values is a slice of structs or maps.
func (gdb *gormDB) CreateInBatches(values []any, batchSize int) error {
	return gdb.db.CreateInBatches(values, batchSize).Error
}

// Order orders the results by a column.
// e.g Order("created_at DESC")
func (gdb *gormDB) Order(value string) *gormDB {
	gdb.db = gdb.db.Order(value)
	return gdb
}

// Limit limits the number of results.
// If limit is 0, it does nothing.
func (gdb *gormDB) Limit(limit int) *gormDB {
	if limit > 0 {
		gdb.db = gdb.db.Limit(limit)
	}
	return gdb
}

// Offset sets the offset for the results.
// If offset is 0, it does nothing.
func (gdb *gormDB) Offset(offset int) *gormDB {
	if offset > 0 {
		gdb.db = gdb.db.Offset(offset)
	}
	return gdb
}

// Select selects the columns to be returned.
// If columns is empty, it does nothing.
func (gdb *gormDB) Select(columns ...string) *gormDB {
	if len(columns) > 0 {
		gdb.db = gdb.db.Select(columns)
	}
	return gdb
}

// Omit omits the columns to be returned.
// If columns is empty, it does nothing.
func (gdb *gormDB) Omit(columns ...string) *gormDB {
	gdb.db = gdb.db.Omit(columns...)
	return gdb
}

// Or applies OR filter on a column.
// If a value is empty, it does nothing.
func (gdb *gormDB) Or(column string, values ...interface{}) *gormDB {
	if len(values) > 0 {
		gdb.db = gdb.db.Or(column, values...)
	}
	return gdb
}

type PreloadOptions struct {
	Query string
	Args  []any
}

// Preload preloads the associations.
func (gdb *gormDB) Preload(options ...PreloadOptions) *gormDB {
	for _, option := range options {
		gdb.db = gdb.db.Preload(option.Query, option.Args...)
	}
	return gdb
}

// Joins joins the associations.
func (gdb *gormDB) Joins(query string, args ...any) *gormDB {
	gdb.db = gdb.db.Joins(query, args...)
	return gdb
}

// First retrieves the first record.
func (gdb *gormDB) First(dest any, conds ...any) error {
	return gdb.db.First(dest, conds...).Error
}

// Find finds all records matching given conditions conds
func (gdb *gormDB) Find(dest any, conds ...any) error {
	return gdb.db.Find(dest, conds...).Error
}

// Create inserts value, returning the inserted data's primary key in value's id.
func (gdb *gormDB) Create(value any) error {
	return gdb.db.Create(value).Error
}

// Update updates the record.
// Save updates value in database. If value doesn't contain a matching primary key, value is inserted.
func (gdb *gormDB) Update(value any) error {
	return gdb.db.Save(value).Error
}

// Updates updates attributes using callbacks.
// values must be a struct or map.
// Reference: https://gorm.io/docs/update.html#Update-Changed-Fields
func (gdb *gormDB) Updates(value any) error {
	return gdb.db.Updates(value).Error
}

// Delete deletes the record (permanently). If you want to soft delete, call .DB.Delete() on the *gorm.DB object.
// It returns an error if any.
func (gdb *gormDB) Delete(value any, conds ...any) error {
	return gdb.db.Unscoped().Delete(value, conds...).Error
}

// Count returns the number of records.
func (gdb *gormDB) Count(count *int64) error {
	return gdb.db.Count(count).Error
}

// PagedResponse defines options for paginated queries.
type PagedResponse[T any] struct {
	Page       int   `json:"page"`
	PageSize   int   `json:"page_size"`
	TotalPages int64 `json:"total_pages"`
	Count      int64 `json:"count"`
	HasNext    bool  `json:"has_next"`
	HasPrev    bool  `json:"has_prev"`
	Results    []T   `json:"results"`
}

// GetPaginated retrieves a paginated list of results.
// The page and pageSize are used to calculate the offset and limit.
// If the page is less than 1, it defaults to 1.
// db is the *gorm.DB object with the model and query options already applied.
// It returns the PaginatedResults and an error if any.
func GetPaginated[T any](db *gorm.DB, model *T, page int, pageSize int) (*PagedResponse[T], error) {
	results := []T{}

	// Page must be >= 1
	if page < 1 {
		page = 1
	}

	// Calculate offset and limit
	offset := (page - 1) * pageSize

	// Retrieve total count of records after applying options
	var totalCount int64
	if err := db.Model(model).Count(&totalCount).Error; err != nil {
		return nil, err
	}

	if err := db.Model(model).Offset(offset).Limit(pageSize).Find(&results).Error; err != nil {
		return nil, err
	}

	paginatedResponse := &PagedResponse[T]{
		Page:       page,
		PageSize:   pageSize,
		HasNext:    int64(page*pageSize) < totalCount,
		HasPrev:    page > 1,
		Results:    results,
		Count:      totalCount,
		TotalPages: int64(math.Ceil(float64(totalCount) / float64(pageSize))),
	}
	return paginatedResponse, nil
}
