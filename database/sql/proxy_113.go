// +build go1.13

package sql

import (
	core "database/sql"
	"time"

	"github.com/pkg/errors"
	"github.com/aokabi/octillery/database/sql/driver"
)

// NullTime the compatible structure of NullTime in 'database/sql' package.
type NullTime struct {
	core  core.NullTime
	Time  time.Time
	Valid bool
}

// Scan the compatible method of Scan in 'database/sql' package.
func (n *NullTime) Scan(value interface{}) error {
	n.core.Time = n.Time
	n.core.Valid = n.Valid
	if err := n.core.Scan(value); err != nil {
		return errors.WithStack(err)
	}
	n.Time = n.core.Time
	n.Valid = n.core.Valid
	return nil
}

// Value the compatible method of Value in 'database/sql' package.
func (n NullTime) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Time, nil
}

// NullInt32 the compatible structure of NullInt32 in 'database/sql' package.
type NullInt32 struct {
	core  core.NullInt32
	Int32 int32
	Valid bool
}

// Scan the compatible method of Scan in 'database/sql' package.
func (n *NullInt32) Scan(value interface{}) error {
	n.core.Int32 = n.Int32
	n.core.Valid = n.Valid
	if err := n.core.Scan(value); err != nil {
		return errors.WithStack(err)
	}
	n.Int32 = n.core.Int32
	n.Valid = n.core.Valid
	return nil
}

// Value the compatible method of Value in 'database/sql' package.
func (n NullInt32) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return int64(n.Int32), nil
}
