package model

import (
	"math"
	"sort"
	"time"

	"github.com/samber/lo"
)

type Row struct {
	InsertID          string
	ReceivedTimestamp time.Time
	RequestLatency    time.Duration
}

type Rows []Row

// InsertID で重複を削除した Rows を返す
// なんかもうちょいいい書き方ありそう
func (r Rows) UniqByInsertID() Rows {
	insertIDs := lo.Map(r, func(row Row, _ int) string {
		return row.InsertID
	})

	uniqInsertIDs := lo.Uniq(insertIDs)

	return lo.Map(uniqInsertIDs, func(insertID string, _ int) Row {
		row, found := lo.Find(r, func(row Row) bool {
			return row.InsertID == insertID
		})
		if !found {
			panic("not found")
		}

		return row
	})
}

func (r Rows) AvgReqLatencyMs() int {
	sum := 0
	for _, row := range r {
		sum += int(row.RequestLatency.Milliseconds())
	}

	if len(r) > 0 {
		return sum / len(r)
	}

	return 0
}

func (r Rows) PercentileNReqLatencyMs(n int) time.Duration {
	if len(r) == 0 {
		return 0
	}

	sort.Slice(r, func(i, j int) bool {
		return r[i].RequestLatency < r[j].RequestLatency
	})

	percentileIndexF := float64(len(r)) * float64(n) / 100
	percentileIndexGT := int(math.Ceil(percentileIndexF))
	percentileIndexLT := int(math.Floor(percentileIndexF))

	if percentileIndexGT == percentileIndexLT {
		return r[percentileIndexGT].RequestLatency
	}

	percentileGTVal := r[percentileIndexGT].RequestLatency
	percentileLTVal := r[percentileIndexLT].RequestLatency

	remainder := (percentileGTVal - percentileLTVal) * percentileLTVal * time.Duration(n) / 100

	return percentileLTVal + remainder
}
