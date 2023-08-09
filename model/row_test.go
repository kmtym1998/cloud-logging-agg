package model

import (
	"fmt"
	"testing"
	"time"
)

func TestPercentileNReqLatencyMs(t *testing.T) {
	target := []int{1, 50, 90, 99}

	t.Run("when sample size is 100", func(t *testing.T) {
		rows := make([]Row, 100)
		for i := range rows {
			rows[i] = Row{
				RequestLatency: time.Duration(i) * time.Second,
			}
		}

		for _, n := range target {
			t.Run("n="+fmt.Sprint(n), func(t *testing.T) {
				got := Rows(rows).PercentileNReqLatency(n)
				want := time.Duration(n) * time.Second
				if got != want {
					t.Errorf("got %v, want %v", got, want)
				}
			})
		}
	})

	t.Run("when sample size is 1000", func(t *testing.T) {
		rows := make([]Row, 1000)
		for i := range rows {
			rows[i] = Row{
				RequestLatency: time.Duration(i) * time.Second,
			}
		}

		for _, n := range target {
			t.Run("n="+fmt.Sprint(n), func(t *testing.T) {
				got := Rows(rows).PercentileNReqLatency(n)
				want := time.Duration(n*10) * time.Second
				if got != want {
					t.Errorf("got %v, want %v", got, want)
				}
			})
		}
	})
}
