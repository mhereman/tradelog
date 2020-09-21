package tradelog

import (
	"database/sql"
	"time"

	"github.com/shopspring/decimal"
)

// Side represents a trade side operation
type Side string

const (
	// BuySide represents a buy order (Long position)
	BuySide Side = "Buy"

	// CloseBuySide represents a close of a buy order (Close of long position)
	CloseBuySide Side = "Close Buy"

	// SellSide represents a sell order (Short position)
	SellSide Side = "Sell"

	// CloseSellSide represents a close of a sell order (Close of short position)
	CloseSellSide Side = "Close Sell"
)

// Trade represents an executed trade
type Trade struct {
	Timestamp time.Time
	Base      string
	Quote     string
	Side      Side
	Volume    decimal.Decimal
	Price     decimal.Decimal
	Fee       decimal.Decimal
}

// EnsureLogTable ensures that the log table is present
func EnsureLogTable(conn *DBConnection) (err error) {
	SQL := `CREATE TABLE IF NOT EXISTS TradeLog (
			id SERIAL PRIMARY KEY NOT NULL,
			timestamp TIMESTAMP NOT NULL,
			base STRING NOT NULL,
			quote STRING NOT NULL,
			side STRING NOT NULL,
			volume NUMERIC(18, 8) NOT NULL,
			price NUMERIC(18, 8) NOT NULL,
			fee NUMERIC(18, 8) NOT NULL
		)`

	if _, err = conn.DB().Exec(SQL); err != nil {
		return
	}

	if _, err = conn.DB().Exec("CREATE INDEX IF NOT EXISTS idx_tradelog_timestamp ON TradeLog (timestamp)"); err != nil {
		return
	}

	if _, err = conn.DB().Exec("CREATE INDEX IF NOT EXISTS idx_tradelog_asset ON TradeLog (base, quote)"); err != nil {
		return
	}

	if _, err = conn.DB().Exec("CREATE INDEX if NOT EXISTS idx_tradelog_side ON TradeLog (side)"); err != nil {
		return
	}
	return
}

// LogTrade logs a trade
func LogTrade(conn *DBConnection, trade Trade) (err error) {
	SQL := `INSERT INTO TradeLog (timestamp, base, quote, side, volume, price, fee)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`

	_, err = conn.DB().Exec(SQL, trade.Timestamp, trade.Base, trade.Quote, trade.Side, trade.Volume, trade.Price, trade.Fee)
	return
}

// LastTrade retrieves the last trade
func LastTrade(conn *DBConnection) (trade *Trade, err error) {
	SQL := `SELECT timestamp, base, quote, side, volume, price, fee
			FROM TradeLog
			ORDER BY timestamp DESC
			LIMIT 1`

	var timestamp time.Time
	var base, quote, side string
	var volumeStr, priceStr, feeStr string
	var volume, price, fee decimal.Decimal

	row := conn.DB().QueryRow(SQL)
	if err = row.Scan(&timestamp, &base, &quote, &side, &volumeStr, &priceStr, &feeStr); err != nil {
		if err == sql.ErrNoRows {
			err = nil
			return
		}
		return
	}

	if volume, err = decimal.NewFromString(volumeStr); err != nil {
		return
	}
	if price, err = decimal.NewFromString(priceStr); err != nil {
		return
	}
	if fee, err = decimal.NewFromString(feeStr); err != nil {
		return
	}

	trade = &Trade{
		Timestamp: timestamp,
		Base:      base,
		Quote:     quote,
		Volume:    volume,
		Price:     price,
		Fee:       fee,
	}
	return
}
