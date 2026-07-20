// SMA crossover paper trader in Go, backed by the Rust candlestore library.
package main

import (
	"fmt"
	"math"

	cs "github.com/tructxn/candlestore-go/candlestore"
)

const (
	symbol = "BTCUSDT:1m"
	fast   = 10
	slow   = 20
	qty    = 0.01
)

func main() {
	fmt.Printf("L3 cache detected: %d MB\n\n", cs.L3CacheBytes()/1024/1024)

	store := cs.NewHardware(10)
	defer store.Close()

	candles := syntheticCandles(200)
	for _, c := range candles {
		store.Append(symbol, c)
	}
	fmt.Printf("Loaded %d candles. Hot symbols: %d\n\n", len(candles), store.SymbolCount())

	// ── SMA crossover strategy ────────────────────────────────────────────────
	cash     := 100_000.0
	position := 0.0
	avgCost  := 0.0 // average entry price of the open position
	realized := 0.0
	trades   := 0
	prevSignal := 0 // 0=none, 1=long, -1=short

	for i := slow; i < len(candles); i++ {
		c := candles[i]

		history := store.Range(symbol, candles[i-slow].Ts, c.Ts)
		if len(history) < slow { continue }

		fastSMA := sma(history[len(history)-fast:])
		slowSMA := sma(history)

		signal := 1
		if fastSMA < slowSMA { signal = -1 }

		if signal != prevSignal {
			switch {
			case signal == 1 && position == 0:
				position = qty
				avgCost = c.Open
				cash -= qty * c.Open
				fmt.Printf("[BUY ] ts=%-19d qty=%.4f @ $%.2f\n", c.Ts, qty, c.Open)
				trades++
			case signal == -1 && position > 0:
				pnl := (c.Open - avgCost) * position
				realized += pnl
				cash += position * c.Open
				position = 0
				fmt.Printf("[SELL] ts=%-19d qty=%.4f @ $%.2f  pnl=$%.2f\n", c.Ts, qty, c.Open, pnl)
				trades++
			}
			prevSignal = signal
		}
	}

	// close open position
	if position > 0 {
		last := candles[len(candles)-1]
		realized += (last.Close - avgCost) * position
		cash += position * last.Close
		fmt.Printf("[CLOS] ts=%-19d qty=%.4f @ $%.2f\n", last.Ts, position, last.Close)
		trades++
		position = 0
	}

	fmt.Printf("\n─── Results ─────────────────────────────────────────\n")
	fmt.Printf("Trades:   %d\n", trades)
	fmt.Printf("Cash:     $%.2f\n", cash)
	fmt.Printf("Realized: $%.2f\n", realized)
	fmt.Printf("P&L:      $%.2f\n", cash-100_000.0)
}

func sma(candles []cs.Candle) float64 {
	var sum float64
	for _, c := range candles { sum += c.Close }
	return sum / float64(len(candles))
}

func syntheticCandles(n int) []cs.Candle {
	candles := make([]cs.Candle, n)
	price   := 50_000.0
	ts      := int64(1_700_000_000_000_000_000) // unix ns
	for i := range candles {
		trend := math.Sin(float64(i)*0.08) * 500.0
		noise := math.Sin(float64(i)*1.7)*100.0 + math.Cos(float64(i)*3.1)*50.0
		price += trend*0.1 + noise*0.05
		if price < 30_000 { price = 30_000 }
		candles[i] = cs.Candle{
			Ts:     ts,
			Open:   price,
			High:   price * 1.002,
			Low:    price * 0.998,
			Close:  price + math.Sin(float64(i)*2.3)*price*0.001,
			Volume: 10.0 + math.Abs(math.Sin(float64(i)*0.5))*5.0,
		}
		ts += 60_000_000_000 // 1 minute in ns
	}
	return candles
}
